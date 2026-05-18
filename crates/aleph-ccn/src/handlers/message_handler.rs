//! Pending-message orchestrator. Mirrors `aleph/handlers/message_handler.py`.
//!
//! Two top-level structs:
//! * [`MessagePublisher`] — receives a wire message, persists it as
//!   `pending_messages`, and publishes a fetch/process job to RabbitMQ.
//! * [`MessageHandler`] — picks a pending message up, verifies its signature,
//!   fetches content, runs the per-type content handler and marks the
//!   message processed (or rejected, or retryable).

use std::collections::HashMap;
use std::sync::Arc;

use aleph_types::message::MessageType;
use aleph_types::message::item_type::ItemType;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use lapin::{BasicProperties, Channel, options::BasicPublishOptions};

use crate::AlephResult;
use crate::chains::signature_verifier::SignatureVerifier;
use crate::db::accessors::cost::upsert_costs;
use crate::db::accessors::files::{insert_content_file_pin, upsert_file};
use crate::db::accessors::messages::{
    get_forgotten_message, get_message_by_item_hash, upsert_confirmation, upsert_message,
    upsert_message_status,
};
use crate::db::accessors::pending_messages::delete_pending_message;
use crate::db::models::account_costs::AccountCostsDb;
use crate::db::models::messages::{ForgottenMessageDb, MessageDb};
use crate::db::models::pending_messages::PendingMessageDb;
use crate::handlers::content::aggregate::AggregateMessageHandler;
use crate::handlers::content::content_handler::ContentHandler;
use crate::handlers::content::forget::{ContentHandlerTable, ForgetMessageHandler};
use crate::handlers::content::post::PostMessageHandler;
use crate::handlers::content::store::StoreMessageHandler;
use crate::handlers::content::vm::VmMessageHandler;
use crate::permissions::AuthorityLookup;
use crate::services::ipfs::IpfsService;
use crate::services::storage::engine::StorageEngine;
use crate::toolkit::timestamp::{timestamp_to_datetime, utc_now};
use crate::types::files::FileType;
use crate::types::message_processing_result::{FailedMessage, ProcessedMessage};
use crate::types::message_status::{
    ErrorCode, MessageOrigin, MessageProcessingException, MessageStatus,
};

/// Configuration knobs needed to build the content-handler table.
///
/// The Python implementation pulls these out of `configmanager.Config`; in
/// Rust we accept them explicitly so the wire-up is checked by the type
/// system.
#[derive(Debug, Clone, Default)]
pub struct HandlersConfig {
    pub balances_addresses: Vec<String>,
    pub balances_post_type: String,
    pub credit_balances_addresses: Vec<String>,
    pub credit_balances_post_types: Vec<String>,
    pub credit_balances_channels: Vec<String>,
    pub storage_grace_period_hours: i64,
    pub max_unauthenticated_upload_file_size: i64,
    pub ipfs_enabled: bool,
    pub store_files: bool,
    /// IPFS `/files/stat` timeout, in seconds. Mirrors Python's
    /// `config.ipfs.stat_timeout` setting.
    pub ipfs_stat_timeout: u64,
    /// HTTP API servers used as a fallback when a `storage`-type file is not
    /// present locally. Mirrors Python's `api-servers` configuration.
    pub api_servers: Vec<String>,
}

/// Pipeline outcome carried back from [`MessageHandler::process`].
#[derive(Debug, Clone)]
pub enum ProcessOutcome {
    Processed(ProcessedMessage),
    Rejected {
        item_hash: String,
        error_code: ErrorCode,
    },
}

/// Build the content-handler table used by both [`MessagePublisher`] and
/// [`MessageHandler`]. Mirrors `BaseMessageHandler.content_handlers`.
pub fn build_content_handlers(
    cfg: &HandlersConfig,
    storage_engine: Arc<dyn StorageEngine>,
    ipfs: Option<Arc<IpfsService>>,
) -> HashMap<MessageType, Arc<dyn ContentHandler>> {
    let vm_handler: Arc<dyn ContentHandler> = Arc::new(VmMessageHandler::new());
    let post_handler: Arc<dyn ContentHandler> = Arc::new(PostMessageHandler::new(
        cfg.balances_addresses.clone(),
        cfg.balances_post_type.clone(),
        cfg.credit_balances_addresses.clone(),
        cfg.credit_balances_post_types.clone(),
        cfg.credit_balances_channels.clone(),
    ));
    let store_handler: Arc<dyn ContentHandler> = Arc::new(StoreMessageHandler::new(
        storage_engine,
        ipfs,
        cfg.storage_grace_period_hours,
        cfg.max_unauthenticated_upload_file_size,
        cfg.ipfs_enabled,
        cfg.store_files,
        cfg.ipfs_stat_timeout,
        cfg.api_servers.clone(),
    ));
    let aggregate_handler: Arc<dyn ContentHandler> = Arc::new(AggregateMessageHandler::new());

    let mut handlers: HashMap<MessageType, Arc<dyn ContentHandler>> = HashMap::new();
    handlers.insert(MessageType::Aggregate, aggregate_handler);
    handlers.insert(MessageType::Instance, vm_handler.clone());
    handlers.insert(MessageType::Post, post_handler);
    handlers.insert(MessageType::Program, vm_handler);
    handlers.insert(MessageType::Store, store_handler);

    let table: ContentHandlerTable = handlers.iter().map(|(k, v)| (*k, v.clone())).collect();
    let forget_handler: Arc<dyn ContentHandler> = Arc::new(ForgetMessageHandler::new(table));
    handlers.insert(MessageType::Forget, forget_handler);
    handlers
}

/// Glue object used by chain verifiers — adapts a `PendingMessageDb` into
/// the [`crate::chains::abc::PendingMessageView`] expected by
/// `SignatureVerifier`.
pub struct PendingMessageVerifierView<'a> {
    chain: aleph_types::chain::Chain,
    sender: &'a str,
    message_type: MessageType,
    item_hash: &'a str,
    signature: Option<&'a str>,
    time_seconds: f64,
}

impl<'a> PendingMessageVerifierView<'a> {
    pub fn from(pm: &'a PendingMessageDb) -> Self {
        let time_seconds = pm.time.timestamp() as f64
            + (pm.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
        Self {
            chain: pm.chain.clone(),
            sender: &pm.sender,
            message_type: pm.r#type,
            item_hash: &pm.item_hash,
            signature: pm.signature.as_deref(),
            time_seconds,
        }
    }
}

impl<'a> crate::chains::abc::PendingMessageView for PendingMessageVerifierView<'a> {
    fn chain(&self) -> aleph_types::chain::Chain {
        self.chain.clone()
    }
    fn sender(&self) -> &str {
        self.sender
    }
    fn message_type(&self) -> MessageType {
        self.message_type
    }
    fn item_hash(&self) -> &str {
        self.item_hash
    }
    fn signature(&self) -> Option<&str> {
        self.signature
    }
    fn time_seconds(&self) -> f64 {
        self.time_seconds
    }
}

/// Publishes pending messages to RabbitMQ. Mirrors Python `MessagePublisher`.
pub struct MessagePublisher {
    pub pending_exchange: String,
    pub channel: Option<Channel>,
}

impl MessagePublisher {
    pub fn new(channel: Channel, pending_exchange: String) -> Self {
        Self {
            channel: Some(channel),
            pending_exchange,
        }
    }

    /// Construct a publisher with no RabbitMQ channel attached. Used by tests
    /// (and by the boot-time wiring before MQ is available); calls to
    /// `publish_pending_message` become no-ops.
    pub fn without_channel(pending_exchange: String) -> Self {
        Self {
            channel: None,
            pending_exchange,
        }
    }

    /// Publish a process/fetch job for an already-persisted pending message.
    /// Mirrors Python `_publish_pending_message`.
    pub async fn publish_pending_message(&self, pending: &PendingMessageDb) -> AlephResult<()> {
        if pending.origin.as_deref() == Some("onchain") {
            return Ok(());
        }
        let Some(channel) = self.channel.as_ref() else {
            return Ok(());
        };
        let process_or_fetch = if pending.fetched { "process" } else { "fetch" };
        let routing_key = format!("{process_or_fetch}.{}", pending.item_hash);
        let body = pending.id.to_string();
        channel
            .basic_publish(
                &self.pending_exchange,
                &routing_key,
                BasicPublishOptions::default(),
                body.as_bytes(),
                BasicProperties::default(),
            )
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish failed: {e}")))?
            .await
            .map_err(|e| crate::AlephError::P2p(format!("publish confirm failed: {e}")))?;
        Ok(())
    }

    /// Persist a wire message as a pending row and announce a fetch/process
    /// job on RabbitMQ. Mirrors Python `MessagePublisher.add_pending_message`.
    ///
    /// Behaviour parity with pyaleph:
    /// 1. Parse the wire payload; on failure record a rejection and return
    ///    `None`.
    /// 2. Build a `PendingMessageDb` via `from_message_dict`.
    /// 3. Inline content is marked `fetched=true`; storage/IPFS content
    ///    remains `fetched=false` so the fetch worker picks it up.
    /// 4. If the message has an existing status:
    ///    - `PROCESSED`/`REMOVING` + `tx_hash`: record the confirmation, no
    ///      pending row inserted.
    ///    - Any non-`REJECTED` status: skip (return `None`).
    ///    - `REJECTED`: transition the status back to `PENDING` so the message
    ///      is retried.
    /// 5. Insert the pending row with `ON CONFLICT (sender, item_hash,
    ///    signature) DO NOTHING`. Unique-constraint violations resolve to
    ///    `None` (already in queue).
    /// 6. On any other DB error, record a rejection and return `None`.
    /// 7. On success, publish the fetch/process notification.
    pub async fn add_pending_message(
        &self,
        client: &(impl tokio_postgres::GenericClient + Sync),
        message_dict: &serde_json::Value,
        reception_time: DateTime<Utc>,
        tx_hash: Option<String>,
        check_message: bool,
        origin: Option<MessageOrigin>,
    ) -> AlephResult<Option<PendingMessageDb>> {
        use crate::db::accessors::messages::{
            get_message_status, reject_new_pending_message, upsert_confirmation,
            upsert_message_status,
        };
        use crate::types::message_status::MessageStatus;

        // 1. Parse the wire message; on failure record a rejection.
        let parsed_result = crate::schemas::pending_messages::parse_message(message_dict.clone());
        let parsed = match parsed_result {
            Ok(p) => p,
            Err(e) => {
                let exception = MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("{e}")],
                };
                let _ = reject_new_pending_message(
                    client,
                    message_dict,
                    &exception,
                    tx_hash.as_deref(),
                )
                .await;
                return Ok(None);
            }
        };
        let _mt = parsed.message_type();

        // 2. Build the pending message row from the raw dict.
        let mut pending = PendingMessageDb::from_message_dict(
            message_dict,
            reception_time,
            false,
            tx_hash.clone(),
            check_message,
            origin,
        );

        // 3. Load fetched content: inline payloads are immediately fetched;
        //    storage/IPFS payloads require a separate fetch pass. Mirrors
        //    `BaseMessageHandler.load_fetched_content`.
        pending.fetched = matches!(pending.item_type, ItemType::Inline);

        // 4. Inspect the existing message status.
        let existing_status = get_message_status(client, &pending.item_hash).await?;
        if let Some(s) = existing_status {
            if s.status != MessageStatus::Rejected {
                // PROCESSED (+ tx_hash) → record confirmation.
                if matches!(s.status, MessageStatus::Processed | MessageStatus::Removing) {
                    if let Some(tx) = tx_hash.as_deref() {
                        upsert_confirmation(client, &pending.item_hash, tx).await?;
                    }
                }
                // Any non-REJECTED status: skip the insertion entirely.
                return Ok(None);
            }
        }

        // 5. Mirror pyaleph: every code path that inserts a pending row also
        //    writes the corresponding `message_status` PENDING row. The
        //    `WHERE message_status.status = 'rejected'` clause flips a
        //    previously-rejected status back to PENDING for retry, and the
        //    INSERT branch creates the row for brand-new messages. Pyaleph
        //    sends both statements unconditionally in `add_pending_message`;
        //    skipping the upsert for new rows would silently desynchronise
        //    the two tables.
        upsert_message_status(
            client,
            &pending.item_hash,
            MessageStatus::Pending,
            reception_time,
            Some("message_status.status = 'rejected'"),
        )
        .await?;

        // 6. Insert with ON CONFLICT DO NOTHING (uq_pending_message:
        //    sender, item_hash, signature).
        match insert_pending_message_on_conflict_nothing(client, &pending).await {
            Ok(Some(id)) => {
                pending.id = id;
            }
            Ok(None) => {
                // Duplicate row — silently drop.
                return Ok(None);
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to add new pending message {} - DB error: {e}",
                    pending.item_hash
                );
                let exception = MessageProcessingException::InternalError {
                    errors: vec![format!("{e}")],
                };
                let _ = reject_new_pending_message(
                    client,
                    message_dict,
                    &exception,
                    tx_hash.as_deref(),
                )
                .await;
                return Ok(None);
            }
        }

        // 6. Notify the fetch/process worker.
        self.publish_pending_message(&pending).await?;
        Ok(Some(pending))
    }
}

/// Insert a pending message row with `ON CONFLICT (sender, item_hash,
/// signature) DO NOTHING`. Returns the row id on success, or `None` if a
/// duplicate row was already present.
async fn insert_pending_message_on_conflict_nothing(
    client: &(impl tokio_postgres::GenericClient + Sync),
    pending: &PendingMessageDb,
) -> AlephResult<Option<i64>> {
    let r#type = serde_json::to_value(&pending.r#type)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let item_type = serde_json::to_value(&pending.item_type)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let chain = serde_json::to_value(&pending.chain)
        .ok()
        .and_then(|v| v.as_str().map(|s| s.to_string()))
        .unwrap_or_default();
    let channel: Option<String> = pending
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()));
    let row_opt = client
        .query_opt(
            "INSERT INTO pending_messages (\
                item_hash, type, chain, sender, signature, item_type, item_content, \
                content, time, channel, reception_time, check_message, next_attempt, \
                retries, tx_hash, fetched, origin) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) \
             ON CONFLICT ON CONSTRAINT uq_pending_message DO NOTHING \
             RETURNING id",
            &[
                &pending.item_hash,
                &r#type,
                &chain,
                &pending.sender,
                &pending.signature,
                &item_type,
                &pending.item_content,
                &pending.content,
                &pending.time,
                &channel,
                &pending.reception_time,
                &pending.check_message,
                &pending.next_attempt,
                &pending.retries,
                &pending.tx_hash,
                &pending.fetched,
                &pending.origin,
            ],
        )
        .await?;
    Ok(row_opt.map(|r| r.get::<_, i64>(0)))
}

/// Pulls non-inline content from the storage engine / IPFS gateway, parses
/// it as JSON. Mirrors Python's
/// `StorageService.get_message_content(pending_message)`.
async fn fetch_message_content(
    pending: &PendingMessageDb,
    storage: &Arc<dyn StorageEngine>,
    ipfs: Option<&Arc<IpfsService>>,
) -> Result<(serde_json::Value, usize), MessageProcessingException> {
    match pending.item_type {
        ItemType::Inline => {
            let body = pending.item_content.as_deref().ok_or_else(|| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec!["Inline message missing item_content".into()],
                }
            })?;
            let v: serde_json::Value = serde_json::from_str(body).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid inline JSON: {e}")],
                }
            })?;
            Ok((v, body.len()))
        }
        ItemType::Storage => {
            let bytes = storage.read(&pending.item_hash).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("Storage read error: {e}")],
                }
            })?;
            let bytes = bytes.ok_or_else(|| {
                MessageProcessingException::message_content_unavailable(pending.item_hash.clone())
            })?;
            let len = bytes.len();
            let v: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid stored JSON: {e}")],
                }
            })?;
            Ok((v, len))
        }
        ItemType::Ipfs => {
            let ipfs = ipfs.ok_or_else(|| {
                MessageProcessingException::message_content_unavailable(pending.item_hash.clone())
            })?;
            let bytes: Option<Bytes> = ipfs
                .get_ipfs_content(&pending.item_hash, std::time::Duration::from_secs(30), 2)
                .await
                .map_err(|_| {
                    MessageProcessingException::message_content_unavailable(
                        pending.item_hash.clone(),
                    )
                })?;
            let bytes = bytes.ok_or_else(|| {
                MessageProcessingException::message_content_unavailable(pending.item_hash.clone())
            })?;
            let len = bytes.len();
            let v: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid IPFS JSON: {e}")],
                }
            })?;
            Ok((v, len))
        }
    }
}

/// Coordinates verification, fetch, dependency/permission/balance checks and
/// dispatch to the per-type content handler. Mirrors `MessageHandler`.
pub struct MessageHandler {
    pub signature_verifier: Arc<SignatureVerifier>,
    pub storage_engine: Arc<dyn StorageEngine>,
    pub ipfs: Option<Arc<IpfsService>>,
    pub authority_lookup: Arc<dyn AuthorityLookup>,
    pub handlers: HashMap<MessageType, Arc<dyn ContentHandler>>,
}

impl MessageHandler {
    pub fn new(
        signature_verifier: Arc<SignatureVerifier>,
        storage_engine: Arc<dyn StorageEngine>,
        ipfs: Option<Arc<IpfsService>>,
        authority_lookup: Arc<dyn AuthorityLookup>,
        cfg: &HandlersConfig,
    ) -> Self {
        let handlers = build_content_handlers(cfg, storage_engine.clone(), ipfs.clone());
        Self {
            signature_verifier,
            storage_engine,
            ipfs,
            authority_lookup,
            handlers,
        }
    }

    fn content_handler(
        &self,
        mt: MessageType,
    ) -> Result<Arc<dyn ContentHandler>, MessageProcessingException> {
        self.handlers
            .get(&mt)
            .cloned()
            .ok_or_else(|| MessageProcessingException::InternalError {
                errors: vec![format!("No content handler for type {mt:?}")],
            })
    }

    /// Verify the signature, fetch the content payload and build a
    /// `MessageDb`. Mirrors Python `fetch_pending_message`.
    pub async fn fetch_pending_message(
        &self,
        pending: &PendingMessageDb,
    ) -> Result<MessageDb, MessageProcessingException> {
        let (content, size) =
            fetch_message_content(pending, &self.storage_engine, self.ipfs.as_ref()).await?;
        let reception_time = Some(pending.reception_time);
        Ok(MessageDb::from_pending_message(
            pending,
            &content,
            size as i32,
            reception_time,
        ))
    }

    /// Verify the cryptographic signature. Mirrors Python `verify_signature`.
    pub async fn verify_signature(
        &self,
        pending: &PendingMessageDb,
    ) -> Result<(), MessageProcessingException> {
        if !pending.check_message {
            return Ok(());
        }
        let view = PendingMessageVerifierView::from(pending);
        self.signature_verifier
            .verify_signature(&view)
            .await
            .map_err(|e| match e {
                crate::AlephError::InvalidSignature => {
                    MessageProcessingException::InvalidSignature {
                        errors: vec![format!("Invalid signature for {}", pending.item_hash)],
                    }
                }
                crate::AlephError::InvalidMessage(msg) => {
                    MessageProcessingException::InvalidSignature { errors: vec![msg] }
                }
                other => MessageProcessingException::InternalError {
                    errors: vec![format!("Signature verification error: {other}")],
                },
            })
    }

    /// Combine signature verification, content fetching and pre-balance
    /// checks. Mirrors Python `verify_and_fetch_message`.
    pub async fn verify_and_fetch_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<MessageDb, MessageProcessingException> {
        self.verify_signature(pending).await?;
        let message = self.fetch_pending_message(pending).await?;
        let handler = self.content_handler(message.r#type)?;
        handler
            .check_permissions(client, &message, &*self.authority_lookup)
            .await?;
        handler.pre_check_balance(client, &message).await?;
        handler.fetch_related_content(client, &message).await?;
        Ok(message)
    }

    async fn confirm_existing_message(
        client: &tokio_postgres::Transaction<'_>,
        existing: &MessageDb,
        pending: &PendingMessageDb,
    ) -> Result<(), MessageProcessingException> {
        if pending.signature != existing.signature {
            return Err(MessageProcessingException::InvalidSignature {
                errors: vec![format!("Invalid signature for {}", pending.item_hash)],
            });
        }
        delete_pending_message(client, pending.id)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting pending message: {e}")],
            })?;
        if let Some(tx_hash) = pending.tx_hash.as_deref() {
            upsert_confirmation(client, &pending.item_hash, tx_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error upserting confirmation: {e}")],
                })?;
        }
        Ok(())
    }

    async fn confirm_existing_forgotten_message(
        client: &tokio_postgres::Transaction<'_>,
        forgotten: &ForgottenMessageDb,
        pending: &PendingMessageDb,
    ) -> Result<(), MessageProcessingException> {
        if pending.signature != forgotten.signature {
            return Err(MessageProcessingException::InvalidSignature {
                errors: vec![format!("Invalid signature for {}", pending.item_hash)],
            });
        }
        delete_pending_message(client, pending.id)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting pending message: {e}")],
            })?;
        Ok(())
    }

    async fn insert_message(
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        upsert_message(client, message).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error upserting message: {e}")],
            }
        })?;

        if message.item_type != ItemType::Inline {
            let time = message
                .content
                .get("time")
                .and_then(|v| v.as_f64())
                .map(timestamp_to_datetime)
                .unwrap_or(message.time);
            upsert_file(
                client,
                &message.item_hash,
                message.size as i64,
                FileType::File,
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error upserting file: {e}")],
            })?;
            insert_content_file_pin(
                client,
                &message.item_hash,
                &message.sender,
                &message.item_hash,
                time,
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting content file pin: {e}")],
            })?;
        }

        delete_pending_message(client, pending.id)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting pending message: {e}")],
            })?;
        upsert_message_status(
            client,
            &message.item_hash,
            MessageStatus::Processed,
            pending.reception_time,
            Some("message_status.status = 'pending'"),
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error upserting message status: {e}")],
        })?;

        if let Some(tx_hash) = pending.tx_hash.as_deref() {
            upsert_confirmation(client, &message.item_hash, tx_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error upserting confirmation: {e}")],
                })?;
        }
        Ok(())
    }

    async fn insert_costs(
        client: &tokio_postgres::Transaction<'_>,
        costs: &[AccountCostsDb],
    ) -> Result<(), MessageProcessingException> {
        if costs.is_empty() {
            return Ok(());
        }
        upsert_costs(client, costs).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting costs: {e}")],
            }
        })?;
        Ok(())
    }

    /// Process a pending message end-to-end. Mirrors Python `process`.
    pub async fn process_pending_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        pending: &PendingMessageDb,
    ) -> Result<ProcessOutcome, MessageProcessingException> {
        // 1. Confirm-existing branch
        let existing = get_message_by_item_hash(client, &pending.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error get_message_by_item_hash: {e}")],
            })?;
        if let Some(existing) = existing {
            Self::confirm_existing_message(client, &existing, pending).await?;
            let item_hash = existing.item_hash.clone();
            let message_value = serde_json::to_value(&existing.content).unwrap_or_default();
            // Mirror Python: `confirm_existing_message` returns a
            // `ProcessedMessage(message=existing, is_confirmation=True)`
            // without an explicit `origin` — the confirmation outcome is
            // independent of where the duplicate came from.
            let pm = ProcessedMessage::new(item_hash, message_value, true);
            return Ok(ProcessOutcome::Processed(pm));
        }

        // 2. Forgotten-duplicate branch
        let forgotten = get_forgotten_message(client, &pending.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error get_forgotten_message: {e}")],
            })?;
        if let Some(f) = forgotten {
            Self::confirm_existing_forgotten_message(client, &f, pending).await?;
            return Ok(ProcessOutcome::Rejected {
                item_hash: pending.item_hash.clone(),
                error_code: ErrorCode::ForgottenDuplicate,
            });
        }

        // 3. Verify + fetch + check dependencies/permissions/balance
        let message = self.verify_and_fetch_message(client, pending).await?;
        let handler = self.content_handler(message.r#type)?;
        handler.check_dependencies(client, &message).await?;
        handler
            .check_permissions(client, &message, &*self.authority_lookup)
            .await?;
        let costs = handler.check_balance(client, &message).await?;

        Self::insert_message(client, pending, &message).await?;
        if let Some(costs) = costs.as_ref() {
            Self::insert_costs(client, costs).await?;
        }

        handler
            .process(client, std::slice::from_ref(&message))
            .await?;

        let message_value = serde_json::to_value(&message.content).unwrap_or_default();
        let pm = ProcessedMessage::new(message.item_hash.clone(), message_value, false)
            .with_origin(parse_origin(pending.origin.as_deref()));
        Ok(ProcessOutcome::Processed(pm))
    }

    /// Convenience wrapper that converts an exception into a `FailedMessage`
    /// so callers can record the retry/rejected status uniformly.
    pub fn failure_from(
        pending: &PendingMessageDb,
        err: &MessageProcessingException,
    ) -> FailedMessage {
        if err.is_retry() {
            FailedMessage::will_retry(pending.item_hash.clone(), err.error_code())
        } else {
            FailedMessage::rejected(pending.item_hash.clone(), err.error_code())
        }
        .with_origin(parse_origin(pending.origin.as_deref()))
    }

    /// Best-effort retry scheduling helper that updates `next_attempt`.
    pub async fn schedule_retry(
        client: &(impl tokio_postgres::GenericClient + Sync),
        pending: &PendingMessageDb,
        next_attempt: DateTime<Utc>,
    ) -> AlephResult<()> {
        crate::db::accessors::pending_messages::set_next_retry(client, pending.id, next_attempt)
            .await
    }
}

fn parse_origin(s: Option<&str>) -> Option<MessageOrigin> {
    match s? {
        "onchain" => Some(MessageOrigin::Onchain),
        "p2p" => Some(MessageOrigin::P2p),
        "ipfs" => Some(MessageOrigin::Ipfs),
        _ => None,
    }
}

/// Helper exposed for callers that only need the current time stamping
/// semantics used by the orchestrator. Mirrors Python's `utc_now()` usage.
pub fn now() -> DateTime<Utc> {
    utc_now()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::storage::in_memory::InMemoryStorageEngine;
    use aleph_types::chain::Chain;
    use async_trait::async_trait;

    struct AlwaysAllow;
    #[async_trait]
    impl AuthorityLookup for AlwaysAllow {
        async fn get_security_aggregate(&self, _owner: &str) -> Option<serde_json::Value> {
            None
        }
        async fn get_message_by_item_hash(
            &self,
            _item_hash: &str,
        ) -> Option<Box<dyn crate::permissions::MessageForAuth + Send + Sync>> {
            None
        }
    }

    fn make_cfg() -> HandlersConfig {
        HandlersConfig {
            balances_addresses: vec!["0xbalances".into()],
            balances_post_type: "balances".into(),
            credit_balances_addresses: vec!["0xcredit".into()],
            credit_balances_post_types: vec!["aleph_credit_distribution".into()],
            credit_balances_channels: Vec::new(),
            storage_grace_period_hours: 24,
            max_unauthenticated_upload_file_size: 0,
            ipfs_enabled: false,
            store_files: false,
            ipfs_stat_timeout: 30,
            api_servers: Vec::new(),
        }
    }

    #[test]
    fn build_content_handlers_covers_all_types() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
        let table = build_content_handlers(&make_cfg(), engine, None);
        for mt in [
            MessageType::Aggregate,
            MessageType::Forget,
            MessageType::Instance,
            MessageType::Post,
            MessageType::Program,
            MessageType::Store,
        ] {
            assert!(table.contains_key(&mt), "missing handler for {:?}", mt);
        }
    }

    #[test]
    fn parse_origin_handles_variants() {
        assert_eq!(parse_origin(None), None);
        assert_eq!(parse_origin(Some("onchain")), Some(MessageOrigin::Onchain));
        assert_eq!(parse_origin(Some("p2p")), Some(MessageOrigin::P2p));
        assert_eq!(parse_origin(Some("ipfs")), Some(MessageOrigin::Ipfs));
        assert_eq!(parse_origin(Some("???")), None);
    }

    #[tokio::test]
    async fn fetch_message_content_handles_inline() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
        let pending = PendingMessageDb {
            id: 1,
            item_hash: "h".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: Some(r#"{"address":"0xabc","time":1.0}"#.into()),
            content: None,
            time: Utc::now(),
            channel: None,
            reception_time: Utc::now(),
            check_message: false,
            next_attempt: Utc::now(),
            retries: 0,
            tx_hash: None,
            fetched: true,
            origin: Some("p2p".into()),
        };
        let (v, size) = fetch_message_content(&pending, &engine, None)
            .await
            .unwrap();
        assert_eq!(v["address"], "0xabc");
        assert!(size > 0);
    }

    #[tokio::test]
    async fn fetch_message_content_missing_inline_errors() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
        let pending = PendingMessageDb {
            id: 1,
            item_hash: "h".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: None,
            time: Utc::now(),
            channel: None,
            reception_time: Utc::now(),
            check_message: false,
            next_attempt: Utc::now(),
            retries: 0,
            tx_hash: None,
            fetched: true,
            origin: None,
        };
        let err = fetch_message_content(&pending, &engine, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            MessageProcessingException::InvalidMessageFormat { .. }
        ));
    }

    #[tokio::test]
    async fn fetch_message_content_storage_missing_is_unavailable() {
        let engine: Arc<dyn StorageEngine> = Arc::new(InMemoryStorageEngine::default());
        let pending = PendingMessageDb {
            id: 1,
            item_hash: "missing-hash".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Storage,
            item_content: None,
            content: None,
            time: Utc::now(),
            channel: None,
            reception_time: Utc::now(),
            check_message: false,
            next_attempt: Utc::now(),
            retries: 0,
            tx_hash: None,
            fetched: false,
            origin: None,
        };
        let err = fetch_message_content(&pending, &engine, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            MessageProcessingException::MessageContentUnavailable { .. }
        ));
    }

    #[test]
    fn failure_from_uses_retry_kind() {
        let pending = PendingMessageDb {
            id: 1,
            item_hash: "h".into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "x".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: None,
            time: Utc::now(),
            channel: None,
            reception_time: Utc::now(),
            check_message: false,
            next_attempt: Utc::now(),
            retries: 0,
            tx_hash: None,
            fetched: false,
            origin: Some("p2p".into()),
        };
        let retry = MessageHandler::failure_from(
            &pending,
            &MessageProcessingException::message_content_unavailable("h"),
        );
        assert_eq!(
            retry.status,
            crate::types::message_status::MessageProcessingStatus::FailedWillRetry
        );
        let reject = MessageHandler::failure_from(
            &pending,
            &MessageProcessingException::InvalidSignature { errors: Vec::new() },
        );
        assert_eq!(
            reject.status,
            crate::types::message_status::MessageProcessingStatus::FailedRejected
        );
        // Origin propagated.
        assert_eq!(retry.origin, Some(MessageOrigin::P2p));
    }

    #[test]
    fn lookup_trait_object_is_send_sync() {
        let lookup: Arc<dyn AuthorityLookup> = Arc::new(AlwaysAllow);
        fn assert_send<T: Send + Sync>(_: &T) {}
        assert_send(&lookup);
    }

    /// Compile-time assertion: `MessageHandler::process_pending_message`
    /// returns a `Send` future so it can be driven by axum 0.8's
    /// multi-threaded scheduler.
    #[allow(dead_code)]
    fn process_pending_message_future_is_send() {
        fn assert_send<F: Send>(_: F) {}
        let handler: Option<Arc<MessageHandler>> = None;
        let pending: Option<Arc<PendingMessageDb>> = None;
        let tx: Option<Arc<tokio_postgres::Transaction<'static>>> = None;
        if let (Some(h), Some(p), Some(t)) = (handler, pending, tx) {
            // The future is built but never awaited — it only needs to type-check.
            let fut = async move {
                let _ = h.process_pending_message(&t, &p).await;
            };
            assert_send(fut);
        }
    }
}
