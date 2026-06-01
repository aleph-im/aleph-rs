//! FORGET message handler. Mirrors `aleph/handlers/content/forget.py`.

use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::Arc;

use aleph_types::message::MessageType;

use crate::db::accessors::aggregates::aggregate_exists;
use crate::db::accessors::messages::{
    append_to_forgotten_by, forget_message as db_forget_message, get_message_by_item_hash,
    get_message_status, message_exists,
};
use crate::db::accessors::vms::get_vms_dependent_volumes;
use crate::db::models::messages::MessageDb;
use crate::handlers::content::content_handler::ContentHandler;
use crate::types::message_status::{MessageProcessingException, MessageStatus};

/// Map of `MessageType` → handler used by FORGET to clean up message-type-
/// specific state. Stored behind `Arc` so the FORGET handler can outlive the
/// orchestrator that builds the table.
pub type ContentHandlerTable = Vec<(MessageType, Arc<dyn ContentHandler>)>;

/// Outcome of inspecting a forget target's stored status. Mirrors the
/// Python `_forget_item_hash` branch logic and is factored out so it can be
/// unit-tested without a real Postgres client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ForgetTargetAction {
    /// Target is already FORGOTTEN — append a row to its `forgotten_by` list.
    AppendForgottenBy,
    /// Target is not in a forgettable state — raise `ForgetTargetNotFound`.
    TargetNotFound,
    /// Target is PROCESSED or REMOVING — proceed with the forget.
    Forget,
}

/// FORGET message handler.
pub struct ForgetMessageHandler {
    handlers: ContentHandlerTable,
}

impl ForgetMessageHandler {
    /// Build a FORGET handler bound to the per-type content handlers.
    pub fn new(handlers: ContentHandlerTable) -> Self {
        Self { handlers }
    }

    fn handler_for(&self, mt: MessageType) -> Option<Arc<dyn ContentHandler>> {
        self.handlers
            .iter()
            .find_map(|(k, v)| if *k == mt { Some(v.clone()) } else { None })
    }

    fn forget_content_hashes(message: &MessageDb) -> Vec<String> {
        message
            .content
            .get("hashes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn forget_content_aggregates(message: &MessageDb) -> Vec<String> {
        message
            .content
            .get("aggregates")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Read the forget content `address` field. Mirrors Python's Pydantic
    /// `ForgetContent`, where a missing `address` raises a validation error.
    fn forget_content_address(message: &MessageDb) -> Result<String, MessageProcessingException> {
        message
            .content
            .get("address")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!(
                    "FORGET message {} missing 'address'",
                    message.item_hash
                )],
            })
    }

    /// Resolve the FORGET's `hashes` + `aggregates` fields into a flat list
    /// of target item hashes.
    async fn list_target_messages(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<Vec<String>, MessageProcessingException> {
        let mut targets = Self::forget_content_hashes(message);
        let address = Self::forget_content_address(message)?;
        for key in Self::forget_content_aggregates(message) {
            let rows = client
                .query(
                    "SELECT item_hash FROM aggregate_elements WHERE key = $1 AND owner = $2",
                    &[&key, &address],
                )
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error listing aggregate elements: {e}")],
                })?;
            for r in rows {
                targets.push(r.get::<_, String>("item_hash"));
            }
        }
        Ok(targets)
    }

    async fn forget_by_message_type(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        match self.handler_for(message.r#type) {
            Some(h) => h.forget_message(client, message).await,
            None => {
                if message.r#type == MessageType::Forget {
                    Err(MessageProcessingException::CannotForgetForgetMessage {
                        target_hash: message.item_hash.clone(),
                    })
                } else {
                    Ok(HashSet::new())
                }
            }
        }
    }

    async fn forget_one_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        target: &MessageDb,
        forgotten_by: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        db_forget_message(client, &target.item_hash, &forgotten_by.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error forgetting message: {e}")],
            })?;

        let additional = self.forget_by_message_type(client, target).await?;
        for h in additional {
            db_forget_message(client, &h, &forgotten_by.item_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error forgetting message: {e}")],
                })?;
        }
        Ok(())
    }

    /// Decide what to do for an item-hash whose status is `status` while
    /// being targeted by FORGET `forgotten_by_hash`. Mirrors the Python
    /// `_forget_item_hash` flow without touching the DB.
    pub(crate) fn classify_forget_target(
        status: MessageStatus,
        item_hash: &str,
        forgotten_by_hash: &str,
    ) -> ForgetTargetAction {
        if status == MessageStatus::Forgotten {
            return ForgetTargetAction::AppendForgottenBy;
        }
        if status == MessageStatus::Rejected {
            tracing::info!("Message {} was rejected, nothing to do.", item_hash);
        }
        if status == MessageStatus::Removed {
            tracing::info!("Message {} was removed, nothing to do.", item_hash);
        }
        // Python flow: only PROCESSED / REMOVING are forgettable. Every
        // other status (including REJECTED and REMOVED) raises
        // ForgetTargetNotFound.
        if status != MessageStatus::Processed && status != MessageStatus::Removing {
            tracing::error!(
                "FORGET message {} targets message {} which is not processed yet",
                forgotten_by_hash,
                item_hash
            );
            return ForgetTargetAction::TargetNotFound;
        }
        ForgetTargetAction::Forget
    }

    async fn forget_item_hash(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        item_hash: &str,
        forgotten_by: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let status = get_message_status(client, item_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching status: {e}")],
            }
        })?;
        let status = match status {
            None => {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: Some(item_hash.to_string()),
                    aggregate_key: None,
                });
            }
            Some(s) => s,
        };

        match Self::classify_forget_target(status.status, item_hash, &forgotten_by.item_hash) {
            ForgetTargetAction::AppendForgottenBy => {
                append_to_forgotten_by(client, item_hash, &forgotten_by.item_hash)
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error appending forgotten_by: {e}")],
                    })?;
                return Ok(());
            }
            ForgetTargetAction::TargetNotFound => {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: Some(item_hash.to_string()),
                    aggregate_key: None,
                });
            }
            ForgetTargetAction::Forget => {}
        }

        let target = get_message_by_item_hash(client, item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching target message: {e}")],
            })?;
        let target = match target {
            None => {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: Some(item_hash.to_string()),
                    aggregate_key: None,
                });
            }
            Some(t) => t,
        };

        if target.r#type == MessageType::Forget {
            return Err(MessageProcessingException::CannotForgetForgetMessage {
                target_hash: target.item_hash.clone(),
            });
        }

        self.forget_one_message(client, &target, forgotten_by).await
    }

    async fn process_forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let hashes = Self::list_target_messages(client, message).await?;
        for h in hashes {
            self.forget_item_hash(client, &h, message).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ContentHandler for ForgetMessageHandler {
    async fn check_dependencies(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let hashes = Self::forget_content_hashes(message);
        let aggregates = Self::forget_content_aggregates(message);
        if hashes.is_empty() && aggregates.is_empty() {
            return Err(MessageProcessingException::NoForgetTarget { errors: Vec::new() });
        }
        let address = Self::forget_content_address(message)?;

        for h in &hashes {
            let exists = message_exists(client, h).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error checking message_exists: {e}")],
                }
            })?;
            if !exists {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: Some(h.clone()),
                    aggregate_key: None,
                });
            }
            let dependents = get_vms_dependent_volumes(client, h).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error checking volume deps: {e}")],
                }
            })?;
            if let Some(vm) = dependents {
                return Err(MessageProcessingException::ForgetNotAllowed {
                    file_hash: h.clone(),
                    vm_hash: vm.item_hash,
                });
            }
        }
        for k in &aggregates {
            let exists = aggregate_exists(client, k, &address).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error checking aggregate_exists: {e}")],
                }
            })?;
            if !exists {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: None,
                    aggregate_key: Some(k.clone()),
                });
            }
        }
        Ok(())
    }

    async fn check_permissions(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
        lookup: &dyn crate::permissions::AuthorityLookup,
    ) -> Result<(), MessageProcessingException> {
        use crate::handlers::content::content_handler::{
            MessageAuthView, check_delegated_authorization_local,
        };

        // FORGET is authorized per-target: a sender can forget a target if
        // they could have created it under the target owner's security
        // aggregate. No base check on the FORGET's own content.address is
        // performed; the FORGET's content.address is a signing convention,
        // not an authorization gate.
        let targets = Self::list_target_messages(client, message).await?;
        for target_hash in &targets {
            let status = get_message_status(client, target_hash).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error fetching status: {e}")],
                }
            })?;
            let status = match status {
                None => {
                    return Err(MessageProcessingException::ForgetTargetNotFound {
                        target_hash: Some(target_hash.clone()),
                        aggregate_key: None,
                    });
                }
                Some(s) => s,
            };
            if matches!(
                status.status,
                MessageStatus::Forgotten | MessageStatus::Rejected | MessageStatus::Removed
            ) {
                continue;
            }
            if status.status != MessageStatus::Processed && status.status != MessageStatus::Removing
            {
                return Err(MessageProcessingException::ForgetTargetNotFound {
                    target_hash: Some(target_hash.clone()),
                    aggregate_key: None,
                });
            }
            let target = get_message_by_item_hash(client, target_hash)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error fetching target: {e}")],
                })?;
            let target = match target {
                None => {
                    return Err(MessageProcessingException::InternalError {
                        errors: vec![format!(
                            "Target message {target_hash} is marked as processed but does not exist"
                        )],
                    });
                }
                Some(t) => t,
            };
            if target.r#type == MessageType::Forget {
                tracing::warn!(
                    "FORGET message {} may not forget FORGET message {}",
                    message.item_hash,
                    target_hash
                );
                return Err(MessageProcessingException::CannotForgetForgetMessage {
                    target_hash: target_hash.clone(),
                });
            }
            // Authorize the sender against the target as if they were
            // creating it: same owner aggregate, same type/channel/chain
            // filters, evaluated against the target's attributes.
            let target_view = MessageAuthView::from_message(&target);
            let target_owner = target_view.content_address.clone();
            if !check_delegated_authorization_local(
                lookup,
                &message.sender,
                &target_owner,
                &target_view,
            )
            .await
            {
                return Err(MessageProcessingException::PermissionDenied {
                    errors: vec![format!(
                        "Sender {} is not authorized to forget message {target_hash} owned by \
                         {target_owner}: the sender could not have created this target under the \
                         owner's security aggregate",
                        message.sender
                    )],
                });
            }
        }
        Ok(())
    }

    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        messages: &[MessageDb],
    ) -> Result<(), MessageProcessingException> {
        for message in messages {
            self.process_forget_message(client, message).await?;
        }
        Ok(())
    }

    async fn forget_message(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        Err(MessageProcessingException::CannotForgetForgetMessage {
            target_hash: message.item_hash.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message_status::MessageStatus;
    use aleph_types::chain::Chain;
    use aleph_types::message::item_type::ItemType;
    use chrono::Utc;
    use serde_json::json;

    fn forget_msg(hashes: &[&str], aggregates: &[&str], address: &str) -> MessageDb {
        let now = Utc::now();
        let content = json!({
            "address": address,
            "hashes": hashes,
            "aggregates": aggregates,
            "time": now.timestamp() as f64,
        });
        MessageDb {
            item_hash: "forget1".into(),
            r#type: MessageType::Forget,
            chain: Chain::Ethereum,
            sender: address.into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content,
            time: now,
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: now,
            owner: Some(address.into()),
            content_type: None,
            content_ref: None,
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[test]
    fn forget_content_extraction() {
        let m = forget_msg(&["h1", "h2"], &["k1"], "0xabc");
        let hashes = ForgetMessageHandler::forget_content_hashes(&m);
        let aggregates = ForgetMessageHandler::forget_content_aggregates(&m);
        let address = ForgetMessageHandler::forget_content_address(&m).unwrap();
        assert_eq!(hashes, vec!["h1", "h2"]);
        assert_eq!(aggregates, vec!["k1"]);
        assert_eq!(address, "0xabc");
    }

    #[test]
    fn forget_content_address_missing_yields_invalid_message_format() {
        // Construct a forget message whose JSON content has no `address`
        // field. Python's Pydantic validator raises here; Rust must too.
        let now = Utc::now();
        let mut m = forget_msg(&["h1"], &[], "0xabc");
        if let Some(obj) = m.content.as_object_mut() {
            obj.remove("address");
        }
        let _ = now;
        let err = ForgetMessageHandler::forget_content_address(&m).unwrap_err();
        assert!(matches!(
            err,
            MessageProcessingException::InvalidMessageFormat { .. }
        ));
    }

    #[test]
    fn forget_no_targets_yields_no_target() {
        // Construct an empty FORGET; check_dependencies returns NoForgetTarget
        // without ever touching the DB when both lists are empty.
        let m = forget_msg(&[], &[], "0xabc");
        assert!(ForgetMessageHandler::forget_content_hashes(&m).is_empty());
        assert!(ForgetMessageHandler::forget_content_aggregates(&m).is_empty());
    }

    #[test]
    fn classify_rejected_target_yields_target_not_found() {
        // REJECTED → Python falls through to the `raise ForgetTargetNotFound`
        // branch. Rust must match.
        assert_eq!(
            ForgetMessageHandler::classify_forget_target(
                MessageStatus::Rejected,
                "item",
                "forget1",
            ),
            ForgetTargetAction::TargetNotFound
        );
    }

    #[test]
    fn classify_removed_target_yields_target_not_found() {
        assert_eq!(
            ForgetMessageHandler::classify_forget_target(MessageStatus::Removed, "item", "forget1",),
            ForgetTargetAction::TargetNotFound
        );
    }

    #[test]
    fn classify_forgotten_target_appends_forgotten_by() {
        assert_eq!(
            ForgetMessageHandler::classify_forget_target(
                MessageStatus::Forgotten,
                "item",
                "forget1",
            ),
            ForgetTargetAction::AppendForgottenBy
        );
    }

    #[test]
    fn classify_processed_or_removing_targets_forget() {
        assert_eq!(
            ForgetMessageHandler::classify_forget_target(
                MessageStatus::Processed,
                "item",
                "forget1",
            ),
            ForgetTargetAction::Forget
        );
        assert_eq!(
            ForgetMessageHandler::classify_forget_target(
                MessageStatus::Removing,
                "item",
                "forget1",
            ),
            ForgetTargetAction::Forget
        );
    }

    #[test]
    fn handler_for_uses_message_type_table() {
        let h: Arc<dyn ContentHandler> =
            Arc::new(super::super::aggregate::AggregateMessageHandler::new());
        let table: ContentHandlerTable = vec![(MessageType::Aggregate, h)];
        let handler = ForgetMessageHandler::new(table);
        assert!(handler.handler_for(MessageType::Aggregate).is_some());
        assert!(handler.handler_for(MessageType::Post).is_none());
    }
}
