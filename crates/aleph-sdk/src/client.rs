use crate::aggregate_models::corechannel::CoreChannelAggregate;
use crate::aggregate_models::domains::{DOMAINS_AGGREGATE_KEY, DomainsAggregate};
use crate::aggregate_models::port_forwarding::{
    PORT_FORWARDING_AGGREGATE_KEY, PortForwardingAggregate,
};
use crate::aggregate_models::pricing::{PRICING_ADDRESS, PricingAggregate};
use crate::aggregate_models::settings::{SETTINGS_ADDRESS, SETTINGS_KEY, SettingsAggregate};
use crate::aggregate_models::vm_images::{VM_IMAGES_KEY, VmImagesAggregate};
use crate::aggregate_models::websites::{WEBSITES_AGGREGATE_KEY, WebsitesAggregate};
use crate::authorization::{AlephAuthorizationClient, ReceivedAuthorization};
use crate::messages::StoreBuilder;
use crate::verify::Hasher;
use aleph_types::account::Account;
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::StorageEngine;
use aleph_types::message::item_type::ItemType;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    ContentSource, FileRef, Message, MessageConfirmation, MessageContent, MessageContentEnum,
    MessageHeader, MessageStatus, MessageType, RawFileRef, SignatureVerificationError,
};
use aleph_types::timestamp::Timestamp;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt, TryStreamExt};
use http::Extensions;
use memsizes::Bytes;
use reqwest::{Request, Response, StatusCode};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware, Middleware, Next};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{StringWithSeparator, formats::CommaSeparator, serde_as, skip_serializing_none};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Semaphore;
use url::Url;

/// Middleware that limits the number of concurrent HTTP requests.
///
/// Placed inside the retry middleware so that permits are held only during actual network I/O,
/// not during backoff sleeps between retries.
struct ConcurrencyLimit {
    semaphore: Arc<Semaphore>,
}

#[async_trait::async_trait]
impl Middleware for ConcurrencyLimit {
    async fn handle(
        &self,
        req: Request,
        extensions: &mut Extensions,
        next: Next<'_>,
    ) -> reqwest_middleware::Result<Response> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .expect("semaphore is never closed");
        next.run(req, extensions).await
    }
}

/// Read a file in chunks and compute its hash using the given Hasher.
pub async fn hash_file(
    path: &std::path::Path,
    mut hasher: Hasher,
) -> Result<ItemHash, StorageError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut buf = vec![0u8; 64 * 1024]; // 64 KiB chunks
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize())
}

#[derive(Clone)]
pub struct AlephClient {
    http_client: ClientWithMiddleware,
    /// Plain client without retry middleware — used for uploads where the
    /// request body (multipart) is not cloneable and therefore cannot be retried.
    upload_client: reqwest::Client,
    ccn_url: Url,
    ipfs_gateway: Url,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("File not found: {0}")]
    NotFound(ItemHash),
    #[error("File reference not found: {0}")]
    RefNotFound(FileRef),
    #[error("failed to parse file size '{value}': {source}")]
    InvalidSize {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error("Integrity verification failed: {0}")]
    IntegrityError(#[from] crate::verify::VerifyError),
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
    #[error("Insufficient balance")]
    InsufficientBalance,
    #[error("IPFS is disabled on this node")]
    IpfsDisabled,
    #[error("Invalid signature on STORE message")]
    InvalidSignature,
    #[error("Invalid upload metadata: {0}")]
    InvalidMetadata(String),
    #[error("File too large")]
    FileTooLarge,
    #[error("Upload failed: {0}")]
    UploadFailed(reqwest_middleware::Error),
    /// The node responded but the body could not be deserialized as JSON.
    #[error("invalid response body from node")]
    InvalidResponseBody(#[source] reqwest::Error),
    /// The node responded with JSON but the embedded `hash` was not a valid item hash.
    #[error("invalid item hash '{value}' in node response: {source}")]
    InvalidResponseHash {
        value: String,
        #[source]
        source: aleph_types::item_hash::ItemHashError,
    },
    /// IPFS gateway returned a body the SDK could not parse as a kubo add-response.
    #[error("invalid IPFS gateway response")]
    InvalidIpfsResponse(#[from] crate::ipfs::ParseRootError),
    #[error("cannot upload empty folder: {0}")]
    EmptyFolder(std::path::PathBuf),
    #[error("non-UTF-8 path cannot be sent to IPFS gateway: {0}")]
    NonUtf8Path(std::path::PathBuf),
    #[error("upload integrity mismatch: locally computed {expected}, server returned {actual}")]
    UploadIntegrityMismatch {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("CID mismatch: gateway returned {remote} but local computation produced {local}")]
    CidMismatch { local: ItemHash, remote: ItemHash },
    /// 422: the CAR header's declared root does not match the metadata's
    /// STORE item_hash. No IPFS side effects on the server. The two CID
    /// strings come from the server's body and are not re-validated here.
    #[error("CAR header root does not match metadata: car={car_root}, metadata={metadata_root}")]
    CarHeaderRootMismatch {
        car_root: String,
        metadata_root: String,
    },
    /// 422: pyaleph imported the CAR successfully but the imported root
    /// differs from the declared root (CAR header lied). The root is now
    /// pinned on the server under the 24h grace period.
    #[error(
        "imported root does not match expected: kubo={kubo_root}, expected={expected_root}; root is pinned on the node under grace period"
    )]
    ImportedRootMismatch {
        kubo_root: String,
        expected_root: String,
    },
    /// 502/504: kubo or the pinning backend is unavailable or timed out.
    /// Transient; retry.
    #[error("IPFS backend unavailable: {0}")]
    IpfsBackendUnavailable(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("local folder hash failed: {0}")]
    FolderHashFailed(#[from] crate::folder_hash::FolderHashError),
}

#[derive(thiserror::Error, Debug)]
pub enum MessageError {
    #[error("Message not found: {0}")]
    NotFound(ItemHash),
    #[error("Expected message {item_hash} to be of type {expected}, got {actual}")]
    InvalidType {
        item_hash: ItemHash,
        expected: MessageType,
        actual: MessageType,
    },
    #[error("Unexpected message status for {item_hash}: {actual}, expected {expected}")]
    UnexpectedStatus {
        item_hash: ItemHash,
        expected: MessageStatus,
        actual: MessageStatus,
    },
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("Integrity verification failed: {0}")]
    Integrity(#[from] IntegrityError),
    #[error("API error (HTTP {status}): {body}")]
    ApiError { status: u16, body: String },
    #[error(transparent)]
    HttpError(#[from] reqwest_middleware::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The base URL had a scheme that has no websocket equivalent.
    #[error("cannot derive a websocket scheme from the base URL")]
    WebsocketBadScheme,
    /// The message filter could not be serialized into a query string.
    #[error("failed to serialize websocket query string")]
    WebsocketSerializeFilter(#[source] serde_qs::Error),
    /// The websocket handshake / TCP connect failed (initial or reconnect).
    #[error("websocket connect failed")]
    WebsocketConnect(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    /// An incoming websocket frame could not be deserialized as a [`Message`].
    #[error("failed to parse websocket message")]
    WebsocketParse(#[source] serde_json::Error),
    /// An error was reported by the websocket stream after connect.
    #[error("websocket stream error")]
    WebsocketStream(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    #[error("upload hash mismatch: expected {expected}, got {actual}")]
    HashMismatch {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("Message build error: {0}")]
    Build(#[from] crate::messages::MessageBuildError),
}

impl MessageError {
    /// Returns true if the error represents a 404 from the CCN, whether
    /// surfaced as the typed `NotFound` variant, an `ApiError` with status
    /// 404, or an `HttpError` wrapping a reqwest 404.
    pub fn is_not_found(&self) -> bool {
        match self {
            MessageError::NotFound(_) => true,
            MessageError::ApiError { status, .. } => *status == 404,
            MessageError::HttpError(e) => e.status() == Some(StatusCode::NOT_FOUND),
            _ => false,
        }
    }
}

/// Error during message integrity verification.
#[derive(Debug, thiserror::Error)]
pub enum IntegrityError {
    /// The computed hash doesn't match the expected item hash.
    #[error("Hash mismatch: expected {expected}, computed {actual}")]
    HashMismatch {
        expected: ItemHash,
        actual: ItemHash,
    },

    /// The raw content passed hash verification but could not be deserialized as MessageContent.
    #[error("Failed to deserialize verified content")]
    ContentDeserializationFailed(#[source] serde_json::Error),

    /// The cryptographic signature does not match the message sender.
    #[error("Signature verification failed: {0}")]
    SignatureVerificationFailed(#[from] SignatureVerificationError),
}

/// A message that passed full verification: content hash matched AND the
/// signature was checked against the sender.
#[derive(Debug)]
pub struct VerifiedMessage {
    message: Message,
}

impl From<VerifiedMessage> for Message {
    fn from(v: VerifiedMessage) -> Self {
        v.message
    }
}

impl VerifiedMessage {
    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn content(&self) -> &MessageContentEnum {
        self.message.content()
    }
}

/// A message whose content hash was verified but whose signature could not be
/// checked because it is absent (`signature: null`). Pyaleph emits such
/// messages for a small set of pre-signature-enforcement-era mainnet entries,
/// notably smart-contract-originated messages.
///
/// **Authentication caveat.** A correctly-behaving CCN only serves `signature:
/// null` for messages whose authenticity is anchored elsewhere (on-chain TX
/// data). The current `/api/v0/messages` response does not surface the
/// provenance discriminant, so a malicious CCN could in principle strip
/// signatures and serve unsigned forgeries. Callers MUST decide for themselves
/// whether they trust their CCN before treating `UnsignedMessage` as
/// authentic; out-of-band verification (e.g. fetching the on-chain TX
/// referenced in `confirmations`) is the only way to authenticate without
/// trusting the CCN.
#[derive(Debug)]
pub struct UnsignedMessage {
    message: Message,
}

impl From<UnsignedMessage> for Message {
    fn from(u: UnsignedMessage) -> Self {
        u.message
    }
}

impl UnsignedMessage {
    pub fn message(&self) -> &Message {
        &self.message
    }

    pub fn content(&self) -> &MessageContentEnum {
        self.message.content()
    }
}

#[derive(Debug)]
pub struct InvalidMessage {
    pub header: MessageHeader,
    pub error: IntegrityError,
}

impl std::fmt::Display for InvalidMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "integrity check failed for {}: {}",
            self.header.item_hash, self.error
        )
    }
}

impl std::error::Error for InvalidMessage {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Outcome of verifying a single message.
///
/// Distinguishes three cases that callers typically need to treat differently:
/// - [`Verified`](Self::Verified): content hash and signature both checked.
/// - [`Unsigned`](Self::Unsigned): content hash checked, but signature is
///   absent (legacy pre-enforcement-era data). See [`UnsignedMessage`] for
///   the authentication caveat.
/// - [`Invalid`](Self::Invalid): an integrity check (hash mismatch,
///   deserialization failure, bad signature) failed.
///
/// Network and I/O errors from fetching non-inline content are returned as
/// `Err(MessageError)` from the verifying APIs, not as an `Invalid` variant.
#[derive(Debug)]
pub enum MessageVerification {
    Verified(VerifiedMessage),
    Unsigned(UnsignedMessage),
    Invalid(InvalidMessage),
}

impl MessageVerification {
    /// Returns the underlying `Message` if verification succeeded (including
    /// the unsigned case), or `None` for `Invalid`.
    pub fn message(&self) -> Option<&Message> {
        match self {
            MessageVerification::Verified(v) => Some(v.message()),
            MessageVerification::Unsigned(u) => Some(u.message()),
            MessageVerification::Invalid(_) => None,
        }
    }

    /// Consuming counterpart to [`message`](Self::message).
    pub fn into_message(self) -> Option<Message> {
        match self {
            MessageVerification::Verified(v) => Some(v.into()),
            MessageVerification::Unsigned(u) => Some(u.into()),
            MessageVerification::Invalid(_) => None,
        }
    }
}

/// Verifies a [`MessageHeader`] and resolves it to a [`MessageVerification`].
///
/// Performs, in order:
/// 1. **Signature check.** If the signature is present and valid: continue.
///    If it is present but invalid: return [`MessageVerification::Invalid`].
///    If it is absent (`signature: null` from legacy pyaleph data): track that
///    fact and continue to content verification.
/// 2. **Content hash check.** Inline messages are hashed locally; non-inline
///    messages (storage/ipfs) are downloaded from
///    `/api/v0/storage/raw/{item_hash}` and their hash is verified against
///    `item_hash`.
/// 3. **Content deserialization.** Always performed from the verified raw
///    bytes, never from the CCN's pre-deserialized `content` field.
///
/// Returns [`MessageVerification::Verified`] for messages that passed every
/// step, [`MessageVerification::Unsigned`] for messages that passed the hash
/// check but had no signature, and [`MessageVerification::Invalid`] for any
/// integrity failure. Network and I/O errors propagate via `Err(MessageError)`.
async fn verify_message_header<C: AlephStorageClient + Sync + ?Sized>(
    client: &C,
    header: MessageHeader,
) -> Result<MessageVerification, MessageError> {
    // Signature check — cheap, no I/O. Missing is not an error here: legacy
    // pre-enforcement-era pyaleph messages are unsigned by design, and we
    // still want to integrity-check their content.
    let signed = match header.verify_signature() {
        Ok(()) => true,
        Err(SignatureVerificationError::MissingSignature) => false,
        Err(e) => {
            return Ok(MessageVerification::Invalid(InvalidMessage {
                header,
                error: IntegrityError::SignatureVerificationFailed(e),
            }));
        }
    };

    let content = match &header.content_source {
        ContentSource::Inline { item_content } => {
            if let Some(Err((expected, actual))) =
                header.content_source.verify_inline_hash(&header.item_hash)
            {
                return Ok(MessageVerification::Invalid(InvalidMessage {
                    header,
                    error: IntegrityError::HashMismatch { expected, actual },
                }));
            }
            MessageContent::deserialize_with_type(header.message_type, item_content.as_bytes())
        }
        ContentSource::Storage | ContentSource::Ipfs => {
            let download = client.download_file_by_hash(&header.item_hash).await?;
            let raw_bytes = match download.with_verification().bytes().await {
                Ok(bytes) => bytes,
                Err(MessageError::Storage(StorageError::IntegrityError(
                    crate::verify::VerifyError::IntegrityMismatch { expected, actual },
                ))) => {
                    return Ok(MessageVerification::Invalid(InvalidMessage {
                        header,
                        error: IntegrityError::HashMismatch { expected, actual },
                    }));
                }
                Err(e) => return Err(e),
            };
            MessageContent::deserialize_with_type(header.message_type, &raw_bytes)
        }
    };

    let content = match content {
        Ok(c) => c,
        Err(e) => {
            return Ok(MessageVerification::Invalid(InvalidMessage {
                header,
                error: IntegrityError::ContentDeserializationFailed(e),
            }));
        }
    };

    let message = header.with_content(content);
    Ok(if signed {
        MessageVerification::Verified(VerifiedMessage { message })
    } else {
        MessageVerification::Unsigned(UnsignedMessage { message })
    })
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalReason {
    BalanceInsufficient,
    CreditInsufficient,
}

/// A pending message as returned by the CCN API (message status endpoint).
/// Not to be confused with `aleph_types::message::pending::PendingMessage`
/// which is the outgoing message type used for submission.
///
/// Time is typed as `DateTime<Utc>` (rather than `Timestamp`) because the CCN
/// emits ISO-8601 datetime strings for pending messages, like `ForgottenMessage`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RawPendingMessage {
    pub sender: Address,
    pub chain: Chain,
    pub signature: Option<Signature>,
    #[serde(flatten)]
    pub content_source: ContentSource,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_hash: ItemHash,
    pub time: DateTime<Utc>,
    pub channel: Option<Channel>,
    pub content: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RejectedMessage {
    pub sender: Address,
    pub chain: Chain,
    pub signature: Option<Signature>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_type: ItemType,
    #[serde(default)]
    pub item_content: Option<String>,
    pub item_hash: ItemHash,
    pub time: Timestamp,
    pub channel: Option<Channel>,
    pub content: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ForgottenMessage {
    pub sender: Address,
    pub chain: Chain,
    pub signature: Option<Signature>,
    #[serde(rename = "type")]
    pub message_type: MessageType,
    pub item_hash: ItemHash,
    pub time: DateTime<Utc>,
    pub channel: Option<Channel>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum MessageWithStatus<M> {
    // More than one message with the same item hash can be pending at the same time.
    Pending {
        messages: Vec<RawPendingMessage>,
    },
    Processed {
        message: M,
    },
    Removing {
        message: M,
        reason: RemovalReason,
    },
    Removed {
        message: M,
        reason: RemovalReason,
    },
    Forgotten {
        message: ForgottenMessage,
        forgotten_by: Vec<ItemHash>,
    },
    Rejected {
        message: RejectedMessage,
        error_code: i64,
    },
}

impl<M> MessageWithStatus<M> {
    pub fn status(&self) -> MessageStatus {
        match self {
            MessageWithStatus::Pending { .. } => MessageStatus::Pending,
            MessageWithStatus::Processed { .. } => MessageStatus::Processed,
            MessageWithStatus::Removing { .. } => MessageStatus::Removing,
            MessageWithStatus::Removed { .. } => MessageStatus::Removed,
            MessageWithStatus::Forgotten { .. } => MessageStatus::Forgotten,
            MessageWithStatus::Rejected { .. } => MessageStatus::Rejected,
        }
    }

    /// Applies a fallible transformation to the message in variants that carry one
    /// (Processed, Removing, Removed). Other variants are passed through unchanged.
    pub async fn try_map_message_async<N, E, F, Fut>(self, f: F) -> Result<MessageWithStatus<N>, E>
    where
        F: FnOnce(M) -> Fut,
        Fut: Future<Output = Result<N, E>>,
    {
        Ok(match self {
            MessageWithStatus::Processed { message } => MessageWithStatus::Processed {
                message: f(message).await?,
            },
            MessageWithStatus::Removing { message, reason } => MessageWithStatus::Removing {
                message: f(message).await?,
                reason,
            },
            MessageWithStatus::Removed { message, reason } => MessageWithStatus::Removed {
                message: f(message).await?,
                reason,
            },
            MessageWithStatus::Pending { messages } => MessageWithStatus::Pending { messages },
            MessageWithStatus::Forgotten {
                message,
                forgotten_by,
            } => MessageWithStatus::Forgotten {
                message,
                forgotten_by,
            },
            MessageWithStatus::Rejected {
                message,
                error_code,
            } => MessageWithStatus::Rejected {
                message,
                error_code,
            },
        })
    }
}

#[derive(Debug, Deserialize)]
struct GetMessageResponse {
    #[serde(flatten)]
    message: MessageWithStatus<Message>,
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SortBy {
    Time,
    TxTime,
}

#[derive(Debug, Copy, Clone)]
pub enum SortOrder {
    Asc,
    Desc,
}

impl SortOrder {
    fn as_i8(self) -> i8 {
        match self {
            SortOrder::Asc => 1,
            SortOrder::Desc => -1,
        }
    }
}

impl Serialize for SortOrder {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i8(self.as_i8())
    }
}

impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SortOrder::Asc => "asc",
            SortOrder::Desc => "desc",
        })
    }
}

/// Pagination parameters for page-mode list endpoints.
#[skip_serializing_none]
#[derive(Debug, Clone, Default, Serialize)]
pub struct PaginationParams {
    /// Maximum number of items per page.
    pub pagination: Option<u32>,
    /// Page number (starts at 1).
    pub page: Option<u32>,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Clone, Default, Serialize)]
pub struct MessageFilter {
    #[serde(rename = "msgType")]
    pub message_type: Option<MessageType>,

    #[serde(rename = "msgTypes")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, MessageType>>")]
    pub message_types: Option<Vec<MessageType>>,

    #[serde(rename = "contentTypes")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub content_types: Option<Vec<String>>,

    #[serde(rename = "contentKeys")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub content_keys: Option<Vec<String>>,

    #[serde(rename = "contentHashes")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, ItemHash>>")]
    pub content_hashes: Option<Vec<ItemHash>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub refs: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, Address>>")]
    pub addresses: Option<Vec<Address>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, Address>>")]
    pub owners: Option<Vec<Address>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub tags: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, ItemHash>>")]
    pub hashes: Option<Vec<ItemHash>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub channels: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub chains: Option<Vec<String>>,

    #[serde(rename = "startDate")]
    pub start_date: Option<Timestamp>,
    #[serde(rename = "endDate")]
    pub end_date: Option<Timestamp>,

    #[serde(rename = "sortBy")]
    pub sort_by: Option<SortBy>,
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<SortOrder>,

    #[serde(rename = "msgStatuses")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, MessageStatus>>")]
    pub message_statuses: Option<Vec<MessageStatus>>,
}

#[derive(Debug, Deserialize)]
pub struct GetMessagesResponse {
    pub messages: Vec<Message>,
    pub pagination_per_page: u32,
    pub pagination_page: u32,
    pub pagination_total: u32,
}

/// Query filter for GET /api/v0/posts.json and /api/v1/posts.json.
///
/// Posts are a higher-level view of POST messages: when a post is amended, the endpoint
/// returns the merged result with the latest content and metadata from the amendment,
/// while preserving a reference to the original post via `original_item_hash`.
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Clone, Default, Serialize)]
pub struct PostFilter {
    /// Filter by sender address.
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, Address>>")]
    pub addresses: Option<Vec<Address>>,

    /// Filter by item hash.
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, ItemHash>>")]
    pub hashes: Option<Vec<ItemHash>>,

    /// Filter by reference hash.
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub refs: Option<Vec<String>>,

    /// Filter by post type (e.g., `"corechan-operation"`).
    #[serde(rename = "types")]
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub post_types: Option<Vec<String>>,

    /// Filter by tag.
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub tags: Option<Vec<String>>,

    /// Filter by channel.
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub channels: Option<Vec<String>>,

    /// Start date filter (inclusive).
    #[serde(rename = "startDate")]
    pub start_date: Option<Timestamp>,

    /// End date filter (exclusive).
    #[serde(rename = "endDate")]
    pub end_date: Option<Timestamp>,

    /// Sort key.
    #[serde(rename = "sortBy")]
    pub sort_by: Option<SortBy>,

    /// Sort order.
    #[serde(rename = "sortOrder")]
    pub sort_order: Option<SortOrder>,
}

/// A merged post as returned by GET /api/v0/posts.json.
///
/// This is the legacy format that includes the full message envelope (chain, signature,
/// confirmations, etc.). When a post has been amended, the response shows the latest
/// version with `original_item_hash` pointing to the original post.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostV0 {
    /// The blockchain used for this post.
    pub chain: Chain,
    /// Hash of the current version (may be an amendment).
    pub item_hash: ItemHash,
    /// Address of the sender.
    pub sender: Address,
    /// Post type (e.g., `"corechan-operation"`).
    #[serde(rename = "type")]
    pub post_type: String,
    /// Channel of the message.
    #[serde(default)]
    pub channel: Option<Channel>,
    /// Whether the post has on-chain confirmations.
    #[serde(default)]
    pub confirmed: bool,
    /// The user-defined content of the post (arbitrary JSON).
    pub content: serde_json::Value,
    /// Timestamp of the post.
    pub time: Timestamp,
    /// On-chain confirmations.
    #[serde(default)]
    pub confirmations: Vec<MessageConfirmation>,
    /// Hash of the original post (before any amendments).
    pub original_item_hash: ItemHash,
    /// Type of the original post.
    #[serde(default)]
    pub original_type: Option<String>,
    /// Alias for `original_item_hash`.
    pub hash: ItemHash,
    /// Address of the post owner.
    pub address: Address,
    /// Reference hash (for amendments, points to the amended post).
    #[serde(default, rename = "ref")]
    pub reference: Option<String>,
}

impl PostV0 {
    /// Deserializes the content into a typed struct.
    pub fn content_as<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.content.clone())
    }
}

#[derive(Debug, Deserialize)]
pub struct GetPostsV0Response {
    pub posts: Vec<PostV0>,
    pub pagination_per_page: u32,
    pub pagination_page: u32,
    pub pagination_total: u32,
}

/// A merged post as returned by GET /api/v1/posts.json.
///
/// This is the leaner format that omits message-level fields (chain, signature,
/// confirmations, etc.) and uses ISO 8601 timestamps instead of unix floats.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostV1 {
    /// Hash of the current version (may be an amendment).
    pub item_hash: ItemHash,
    /// The user-defined content of the post (arbitrary JSON).
    pub content: serde_json::Value,
    /// Hash of the original post (before any amendments).
    pub original_item_hash: ItemHash,
    /// Type of the original post.
    #[serde(default)]
    pub original_type: Option<String>,
    /// Address of the post owner.
    pub address: Address,
    /// Reference hash (for amendments, points to the amended post).
    #[serde(default, rename = "ref")]
    pub reference: Option<String>,
    /// Channel of the message.
    #[serde(default)]
    pub channel: Option<Channel>,
    /// When the post was first created (ISO 8601).
    pub created: DateTime<Utc>,
    /// When the post was last updated (ISO 8601), i.e. the time of the latest amendment.
    pub last_updated: DateTime<Utc>,
}

impl PostV1 {
    /// Deserializes the content into a typed struct.
    pub fn content_as<T: DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_value(self.content.clone())
    }
}

#[derive(Debug, Deserialize)]
pub struct GetPostsV1Response {
    pub posts: Vec<PostV1>,
    pub pagination_per_page: u32,
    pub pagination_page: u32,
    pub pagination_total: u32,
}

#[derive(Debug, Deserialize)]
struct GetMessageHeadersResponse {
    messages: Vec<MessageHeader>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PublicationStatus {
    pub status: String,
    pub failed: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PostMessageResponse {
    pub publication_status: PublicationStatus,
    pub message_status: String,
}

/// Body for POSTing a message to a node via POST /api/v0/messages.
#[derive(Serialize)]
struct PostMessageBody<'a> {
    sync: bool,
    message: &'a PendingMessage,
}

/// Response from the `/api/v0/price/estimate` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceEstimate {
    pub required_tokens: f64,
    pub payment_type: String,
    pub cost: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FileMetadata {
    #[serde(rename = "ref")]
    pub reference: RawFileRef,
    pub owner: Address,
    pub file_hash: ItemHash,
    pub size: Bytes,
}

pub struct FileDownload {
    response: reqwest::Response,
    expected_hash: ItemHash,
    verify: bool,
}

impl FileDownload {
    pub(crate) fn new(response: reqwest::Response, expected_hash: ItemHash) -> Self {
        Self {
            response,
            expected_hash,
            verify: false,
        }
    }

    /// Enables integrity verification for this download.
    ///
    /// When enabled, [`bytes()`](Self::bytes) and [`to_file()`](Self::to_file) will verify the
    /// downloaded content matches the expected hash. Note that [`to_file()`](Self::to_file) writes
    /// data to disk as it streams — if verification fails, the partial file remains on disk and the
    /// caller is responsible for cleanup.
    ///
    /// Verification is **not** applied by [`into_stream()`](Self::into_stream).
    pub fn with_verification(mut self) -> Self {
        self.verify = true;
        self
    }

    pub async fn bytes(self) -> Result<bytes::Bytes, MessageError> {
        let content = self
            .response
            .bytes()
            .await
            .map_err(reqwest_middleware::Error::from)
            .map_err(MessageError::from)?;

        if self.verify {
            let mut verifier = crate::verify::HashVerifier::new(&self.expected_hash)
                .map_err(StorageError::IntegrityError)?;
            verifier.update(&content);
            verifier.finalize().map_err(StorageError::IntegrityError)?;
        }

        Ok(content)
    }

    pub async fn to_file(self, path: impl AsRef<std::path::Path>) -> Result<(), MessageError> {
        let mut file = tokio::fs::File::create(path)
            .await
            .map_err(MessageError::Io)?;
        let mut stream = self.response.bytes_stream();

        let mut verifier = if self.verify {
            Some(
                crate::verify::HashVerifier::new(&self.expected_hash)
                    .map_err(StorageError::IntegrityError)?,
            )
        } else {
            None
        };

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(reqwest_middleware::Error::from)?;
            if let Some(ref mut v) = verifier {
                v.update(&chunk);
            }
            file.write_all(&chunk).await.map_err(MessageError::Io)?;
        }
        file.flush().await.map_err(MessageError::Io)?;

        if let Some(v) = verifier {
            v.finalize().map_err(StorageError::IntegrityError)?;
        }

        Ok(())
    }

    /// Returns the raw response body as a byte stream.
    ///
    /// Verification is **not** performed; the `with_verification()` flag is ignored.
    pub fn into_stream(
        self,
    ) -> impl futures_util::Stream<Item = Result<bytes::Bytes, reqwest::Error>> {
        self.response.bytes_stream()
    }
}

pub trait AlephMessageClient {
    fn get_message(
        &self,
        item_hash: &ItemHash,
    ) -> impl Future<Output = Result<MessageWithStatus<Message>, MessageError>> + Send;
    fn get_messages(
        &self,
        filter: &MessageFilter,
        pagination: PaginationParams,
    ) -> impl Future<Output = Result<Vec<Message>, MessageError>> + Send;

    /// Returns a stream that automatically paginates through all messages matching the filter.
    ///
    /// Items are yielded one at a time. Pages are fetched lazily — the first HTTP request
    /// happens on the first `.next()` call. Uses cursor-based pagination internally.
    ///
    /// The stream terminates when all results have been consumed, or on the first error
    /// (after the retry middleware has exhausted its retries).
    /// `pagination` controls items per page (default 200, max 200).
    fn get_messages_iterator(
        &self,
        filter: MessageFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<Message, MessageError>> + Send + '_;

    fn subscribe_to_messages(
        &self,
        filter: &MessageFilter,
        history: Option<u32>,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Message, MessageError>> + Send + Unpin,
            MessageError,
        >,
    > + Send;

    fn post_message(
        &self,
        message: &PendingMessage,
        sync: bool,
    ) -> impl Future<Output = Result<PostMessageResponse, MessageError>> + Send;

    /// Submits a signed message to the network, uploading content to storage
    /// or IPFS first if the message is non-inline.
    ///
    /// Prefer this over [`post_message`](Self::post_message) for messages built
    /// with [`MessageBuilder`](crate::builder::MessageBuilder) or the typed builders,
    /// as it handles the content upload step transparently.
    fn submit_message(
        &self,
        message: &PendingMessage,
        sync: bool,
    ) -> impl Future<Output = Result<PostMessageResponse, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        async move {
            match message.item_type {
                ItemType::Inline => {}
                ItemType::Storage => {
                    let uploaded = self
                        .upload_to_storage(message.item_content.as_bytes(), None, false)
                        .await?;
                    if uploaded != message.item_hash {
                        return Err(MessageError::HashMismatch {
                            expected: message.item_hash.clone(),
                            actual: uploaded,
                        });
                    }
                }
                ItemType::Ipfs => {
                    let uploaded = self
                        .upload_to_ipfs(message.item_content.as_bytes(), None, false)
                        .await?;
                    if uploaded != message.item_hash {
                        return Err(MessageError::HashMismatch {
                            expected: message.item_hash.clone(),
                            actual: uploaded,
                        });
                    }
                }
            }
            self.post_message(message, sync).await
        }
    }

    /// Uploads a file and creates a STORE message in one call.
    ///
    /// This is a convenience that combines `upload_file_to_storage`/`upload_file_to_ipfs`,
    /// `StoreBuilder`, and `post_message`. For more control (setting reference,
    /// metadata, or channel), use those components directly.
    ///
    /// The file is hashed locally and uploaded together with the signed STORE
    /// message in a single authenticated request, regardless of storage engine.
    fn create_store(
        &self,
        account: &impl Account,
        path: impl AsRef<std::path::Path> + Send,
        storage_engine: StorageEngine,
        sync: bool,
    ) -> impl Future<Output = Result<ItemHash, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        async move {
            let path = path.as_ref();
            let hasher = match storage_engine {
                StorageEngine::Storage => Hasher::for_storage(),
                StorageEngine::Ipfs => Hasher::for_ipfs(),
            };
            let file_hash = hash_file(path, hasher).await?;
            let message = StoreBuilder::new(account, file_hash.clone(), storage_engine).build()?;
            match storage_engine {
                StorageEngine::Storage => {
                    self.upload_file_to_storage(path, Some(&message), sync)
                        .await?;
                }
                StorageEngine::Ipfs => {
                    self.upload_file_to_ipfs(path, Some(&message), sync).await?;
                }
            }
            Ok(file_hash)
        }
    }

    /// Verifies a fully-fetched [`Message`] by re-checking its raw content
    /// and signature.
    ///
    /// Returns a [`MessageVerification`] describing the outcome:
    /// [`Verified`](MessageVerification::Verified) (content hash + signature
    /// both checked), [`Unsigned`](MessageVerification::Unsigned) (content
    /// hash checked, signature absent), or
    /// [`Invalid`](MessageVerification::Invalid) (integrity failure).
    /// `Err(MessageError)` is reserved for transient failures (network, I/O).
    fn verify_message(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<MessageVerification, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        verify_message_header(self, MessageHeader::from(message))
    }

    /// Fetches a single message and verifies its integrity.
    ///
    /// Verification is only performed for statuses that carry a full
    /// [`Message`] (Processed, Removing, Removed); other statuses (Pending,
    /// Forgotten, Rejected) are passed through unchanged. See
    /// [`verify_message`](Self::verify_message) for the verification outcomes.
    fn get_message_and_verify(
        &self,
        item_hash: &ItemHash,
    ) -> impl Future<Output = Result<MessageWithStatus<MessageVerification>, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        async {
            self.get_message(item_hash)
                .await?
                .try_map_message_async(|msg| self.verify_message(msg))
                .await
        }
    }

    /// Fetches messages matching the filter and verifies each one's integrity.
    ///
    /// Messages are fetched as headers (without deserializing the CCN's `content` field),
    /// then each message's raw content is obtained and verified:
    /// - Inline messages: verified locally from `item_content`
    /// - Non-inline messages: downloaded from `/api/v0/storage/raw/{item_hash}`
    ///
    /// Each message resolves to a [`MessageVerification`] — `Verified`,
    /// `Unsigned`, or `Invalid`. The outer `Err(MessageError)` is reserved for
    /// transient failures (network, 404) that abort the entire batch.
    ///
    /// **Note:** Non-inline messages require a sequential HTTP round-trip each to
    /// `/storage/raw/{item_hash}`, so verifying a page of N non-inline messages incurs N
    /// additional requests.
    ///
    /// ```ignore
    /// let results = client.get_messages_and_verify(&filter).await?;
    ///
    /// for outcome in results {
    ///     match outcome {
    ///         MessageVerification::Verified(v) => use_signed(v),
    ///         MessageVerification::Unsigned(u) => use_legacy(u), // see UnsignedMessage caveat
    ///         MessageVerification::Invalid(i) => log::warn!("{}", i),
    ///     }
    /// }
    /// ```
    fn get_messages_and_verify(
        &self,
        filter: &MessageFilter,
    ) -> impl Future<Output = Result<Vec<MessageVerification>, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync;
}

pub trait AlephStorageClient {
    fn get_file_size(
        &self,
        file_hash: &ItemHash,
    ) -> impl Future<Output = Result<Bytes, MessageError>> + Send;

    fn get_file_metadata_by_message_hash(
        &self,
        message_hash: &ItemHash,
    ) -> impl Future<Output = Result<FileMetadata, MessageError>> + Send;

    fn get_file_metadata_by_ref(
        &self,
        file_ref: &FileRef,
    ) -> impl Future<Output = Result<FileMetadata, MessageError>> + Send;

    fn download_file_by_hash(
        &self,
        file_hash: &ItemHash,
    ) -> impl Future<Output = Result<FileDownload, MessageError>> + Send;

    fn download_file_by_ref(
        &self,
        file_ref: &FileRef,
    ) -> impl Future<Output = Result<FileDownload, MessageError>> + Send
    where
        Self: Sync,
    {
        async {
            let metadata = self.get_file_metadata_by_ref(file_ref).await?;
            self.download_file_by_hash(&metadata.file_hash).await
        }
    }

    fn download_file_by_message_hash(
        &self,
        message_hash: &ItemHash,
    ) -> impl Future<Output = Result<FileDownload, MessageError>> + Send
    where
        Self: Sync,
    {
        async {
            let metadata = self.get_file_metadata_by_message_hash(message_hash).await?;
            self.download_file_by_hash(&metadata.file_hash).await
        }
    }

    /// Uploads raw bytes to the node's storage backend.
    ///
    /// Sends a `POST /api/v0/storage/add_file` multipart request and returns
    /// the SHA-256 hash of the uploaded content.
    ///
    /// When `message` is provided, `sync` controls whether the server waits
    /// for the STORE message to be processed before responding. Ignored when
    /// no message is attached.
    fn upload_to_storage(
        &self,
        data: &[u8],
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;

    /// Uploads raw bytes to the node's IPFS backend.
    ///
    /// Sends a `POST /api/v0/ipfs/add_file` multipart request and returns
    /// the IPFS CID of the pinned content.
    ///
    /// When `message` is `Some`, the multipart form includes a signed
    /// STORE message in the `metadata` field. The message must be a STORE
    /// with `content.item_type=ipfs` and `content.item_hash` set to the
    /// CID the server will compute (callers compute it locally with
    /// `Hasher::for_ipfs()`). The server pins, recomputes the CID, rejects
    /// with 422 on mismatch, and processes the message inline. `sync`
    /// controls whether the server waits for STORE message processing
    /// before responding; ignored when `message` is `None`.
    fn upload_to_ipfs(
        &self,
        data: &[u8],
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;

    /// Uploads a file from disk to native storage, streaming without
    /// loading the full file into memory. Returns the locally-computed
    /// SHA-256 hash, verified against the server's response.
    ///
    /// When `message` is provided, `sync` controls whether the server waits
    /// for the STORE message to be processed before responding. Ignored when
    /// no message is attached.
    fn upload_file_to_storage(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;

    /// Uploads a file from disk to IPFS, streaming without loading the
    /// full file into memory. Returns the locally-computed CID, verified
    /// against the server's response.
    ///
    /// `message` and `sync` behave as for [`Self::upload_to_ipfs`].
    fn upload_file_to_ipfs(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send;
}

/// Methods used to query account properties, ex: their balance.
pub trait AlephAccountClient {
    /// Gets the balance of an Aleph account, in ALEPH tokens and credits.
    fn get_balance(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<AccountBalance, MessageError>> + Send;

    /// Gets the total size of all files stored by the user.
    fn get_total_storage_size(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<Bytes, MessageError>> + Send;

    /// Returns a cursor-walking stream over the files stored by `address`.
    ///
    /// `pagination` caps the items per round-trip (defaults to the SDK's
    /// cursor default, capped at the server's max). `sort_order` is `-1`
    /// for newest first (server default) or `1` for oldest first; `None`
    /// uses the server default.
    fn get_account_files_iterator(
        &self,
        address: &Address,
        pagination: Option<u32>,
        sort_order: Option<i8>,
    ) -> impl Stream<Item = Result<AccountFile, MessageError>> + Send + '_;

    /// Gets the price of a VM in Aleph tokens using the holder tier, i.e. the minimum amount
    /// of Aleph tokens that the user needs to hold in his account.
    fn get_vm_price(
        &self,
        item_hash: &ItemHash,
    ) -> impl Future<Output = Result<f64, MessageError>> + Send;

    /// Returns a paginated history of credit-affecting events for the address
    /// (purchases, transfers, expirations, etc.).
    ///
    /// Pages are 1-indexed to match the server. `page_size` is clamped by the
    /// server side; pass `None` to use the server default. `filters` narrows
    /// the rows by time range, direction, and billed resource type.
    fn get_credit_history(
        &self,
        address: &Address,
        page: u32,
        page_size: Option<u32>,
        filters: &CreditHistoryFilters,
    ) -> impl Future<Output = Result<CreditHistoryResponse, MessageError>> + Send;

    /// Returns aggregate spend totals for the address over the same filter
    /// set as [`get_credit_history`](Self::get_credit_history). Unlike the
    /// listing endpoint this always succeeds, returning zeroed totals for an
    /// address with no matching entries.
    fn get_credit_history_summary(
        &self,
        address: &Address,
        filters: &CreditHistoryFilters,
    ) -> impl Future<Output = Result<CreditHistorySummary, MessageError>> + Send;
}

pub trait AlephAggregateClient {
    /// Returns the most recent version of an aggregate.
    fn get_aggregate<T: DeserializeOwned>(
        &self,
        address: &Address,
        key: &str,
    ) -> impl Future<Output = Result<T, MessageError>> + Send;

    /// Returns the most recent version of the corechannel aggregate, i.e., the aggregate
    /// that lists all the nodes on the network.
    fn get_corechannel_aggregate(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<CoreChannelAggregate, MessageError>> + Send {
        self.get_aggregate(address, "corechannel")
    }

    /// Returns the pricing aggregate that describes compute pricing tiers on the network.
    fn get_pricing_aggregate(
        &self,
    ) -> impl Future<Output = Result<PricingAggregate, MessageError>> + Send {
        self.get_aggregate(&PRICING_ADDRESS, "pricing")
    }

    /// Returns the `websites` aggregate for the given address.
    ///
    /// "No aggregate stored for this address" is mapped to an empty aggregate
    /// regardless of how the CCN signals it:
    ///
    /// * `200` with `data: null`, `data: {}`, or `data: {"websites": null}` -> empty.
    /// * `404 Not Found` -> empty.
    ///
    /// Other transport errors (timeouts, 5xx, parse failures, etc.) are propagated.
    fn get_websites_aggregate(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<WebsitesAggregate, MessageError>> + Send;

    /// Returns the `domains` aggregate for the given address.
    ///
    /// "No aggregate stored for this address" is mapped to an empty aggregate
    /// regardless of how the CCN signals it:
    ///
    /// * `200` with `data: null`, `data: {}`, or `data: {"domains": null}` -> empty.
    /// * `404 Not Found` -> empty.
    ///
    /// Other transport errors (timeouts, 5xx, parse failures, etc.) are propagated.
    fn get_domains_aggregate(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<DomainsAggregate, MessageError>> + Send;

    /// Returns the `port-forwarding` aggregate for the given address.
    ///
    /// "No aggregate stored for this address" is mapped to an empty aggregate
    /// regardless of how the CCN signals it:
    ///
    /// * `200` with `data: null`, `data: {}`, or `data: {"port-forwarding": null}` -> empty.
    /// * `404 Not Found` -> empty.
    ///
    /// Other transport errors (timeouts, 5xx, parse failures, etc.) are propagated.
    fn get_port_forwarding_aggregate(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<PortForwardingAggregate, MessageError>> + Send;

    /// Returns the most recent version of the vm-images aggregate that lists
    /// rootfs presets, runtimes, and confidential UEFI firmware curated on the
    /// network.
    fn get_vm_images_aggregate(
        &self,
    ) -> impl Future<Output = Result<VmImagesAggregate, MessageError>> + Send {
        self.get_aggregate(&PRICING_ADDRESS, VM_IMAGES_KEY)
    }

    /// Returns the most recent version of the foundation `settings` aggregate,
    /// which lists (among other network settings) the GPU models compatible
    /// with the network in `compatible_gpus`.
    fn get_settings_aggregate(
        &self,
    ) -> impl Future<Output = Result<SettingsAggregate, MessageError>> + Send {
        self.get_aggregate(&SETTINGS_ADDRESS, SETTINGS_KEY)
    }

    /// Returns the most recent version of multiple aggregates, keyed by their aggregate key.
    ///
    /// The result map only contains keys that exist on the server. Keys that
    /// have no aggregate are silently omitted.
    fn get_aggregates(
        &self,
        address: &Address,
        keys: &[&str],
    ) -> impl Future<Output = Result<HashMap<String, serde_json::Value>, MessageError>> + Send;

    /// Returns every aggregate owned by `address`, keyed by their aggregate key.
    ///
    /// Use `get_aggregate` / `get_aggregates` when you already know the keys
    /// you want; this method is for enumerating an address's aggregate
    /// namespace (e.g. `aleph aggregate list`).
    fn get_all_aggregates(
        &self,
        address: &Address,
    ) -> impl Future<Output = Result<HashMap<String, serde_json::Value>, MessageError>> + Send;
}

pub trait AlephPostClient {
    /// Queries posts matching the given filter using the v0 (legacy) format.
    ///
    /// The v0 format includes the full message envelope (chain, signature,
    /// confirmations, etc.). Returns the full response including pagination metadata.
    fn get_posts_v0(
        &self,
        filter: &PostFilter,
        pagination: PaginationParams,
    ) -> impl Future<Output = Result<GetPostsV0Response, MessageError>> + Send;

    /// Queries posts matching the given filter using the v1 format.
    ///
    /// The v1 format is leaner: it omits message-level fields and uses ISO 8601
    /// timestamps (`created`, `last_updated`) instead of unix floats.
    fn get_posts_v1(
        &self,
        filter: &PostFilter,
        pagination: PaginationParams,
    ) -> impl Future<Output = Result<GetPostsV1Response, MessageError>> + Send;

    /// Returns a stream that automatically paginates through all posts matching the filter
    /// using the v0 (legacy) format.
    ///
    /// Items are yielded one at a time. Pages are fetched lazily — the first HTTP request
    /// happens on the first `.next()` call. Uses cursor-based pagination internally.
    ///
    /// The stream terminates when all results have been consumed, or on the first error
    /// (after the retry middleware has exhausted its retries).
    /// `pagination` controls items per page (default 200, max 200).
    fn get_posts_v0_iterator(
        &self,
        filter: PostFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<PostV0, MessageError>> + Send + '_;

    /// Returns a stream that automatically paginates through all posts matching the filter
    /// using the v1 format.
    ///
    /// Items are yielded one at a time. Pages are fetched lazily — the first HTTP request
    /// happens on the first `.next()` call. Uses cursor-based pagination internally.
    ///
    /// The stream terminates when all results have been consumed, or on the first error
    /// (after the retry middleware has exhausted its retries).
    ///
    /// `pagination` controls items per page (default 200, max 200).
    fn get_posts_v1_iterator(
        &self,
        filter: PostFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<PostV1, MessageError>> + Send + '_;
}

/// Configuration for HTTP retry behavior on transient errors (429, 5xx).
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. Default: 3.
    pub max_retries: u32,
    /// Minimum backoff duration between retries. Default: 500ms.
    pub min_backoff: Duration,
    /// Maximum backoff duration between retries. Default: 30s.
    pub max_backoff: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            min_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
        }
    }
}

/// Configuration for HTTP timeouts.
#[derive(Debug, Clone)]
pub struct TimeoutConfig {
    /// Timeout for establishing a TCP connection. Default: 10s.
    pub connect_timeout: Duration,
    /// Overall timeout for an individual HTTP request (including reading the
    /// response body). Default: 120s. Set to `None` via [`TimeoutConfig::no_request_timeout`]
    /// to disable.
    pub request_timeout: Option<Duration>,
}

impl TimeoutConfig {
    /// Returns a config with no per-request timeout (connect timeout still applies).
    /// Useful for long-running streaming downloads.
    pub fn no_request_timeout() -> Self {
        Self {
            request_timeout: None,
            ..Default::default()
        }
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Some(Duration::from_secs(120)),
        }
    }
}

const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 16;

/// Builder for [`AlephClient`].
///
/// ```
/// # use url::Url;
/// # use aleph_sdk::client::{AlephClient, RetryConfig, TimeoutConfig};
/// # use std::time::Duration;
/// let client = AlephClient::builder(Url::parse("https://api3.aleph.im").unwrap())
///     .max_concurrent_requests(32)
///     .retry_config(RetryConfig { max_retries: 5, ..Default::default() })
///     .timeout_config(TimeoutConfig { connect_timeout: Duration::from_secs(5), ..Default::default() })
///     .build();
/// ```
pub struct AlephClientBuilder {
    ccn_url: Url,
    retry_config: RetryConfig,
    timeout_config: TimeoutConfig,
    max_concurrent_requests: usize,
    ipfs_gateway: Url,
}

impl AlephClientBuilder {
    pub fn retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    pub fn timeout_config(mut self, config: TimeoutConfig) -> Self {
        self.timeout_config = config;
        self
    }

    /// Sets the maximum number of concurrent HTTP requests. Default: 16.
    ///
    /// # Panics
    ///
    /// Panics if `n` is 0 (would deadlock all requests).
    pub fn max_concurrent_requests(mut self, n: usize) -> Self {
        assert!(n > 0, "max_concurrent_requests must be > 0");
        self.max_concurrent_requests = n;
        self
    }

    /// Overrides the default IPFS gateway URL.
    pub fn ipfs_gateway(mut self, gateway: Url) -> Self {
        self.ipfs_gateway = gateway;
        self
    }

    pub fn build(self) -> AlephClient {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(self.retry_config.min_backoff, self.retry_config.max_backoff)
            .build_with_max_retries(self.retry_config.max_retries);

        let concurrency_limit = ConcurrencyLimit {
            semaphore: Arc::new(Semaphore::new(self.max_concurrent_requests)),
        };

        let base_client = self.build_reqwest_client();

        // Retry is the outer middleware: it decides whether to retry.
        // ConcurrencyLimit is the inner middleware: each attempt (including retries)
        // acquires a permit only for the duration of actual network I/O.
        let http_client = ClientBuilder::new(base_client.clone())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .with(concurrency_limit)
            .build();

        // Upload client shares the same timeout settings but has no retry
        // middleware (multipart bodies are not cloneable and cannot be retried).
        AlephClient {
            http_client,
            upload_client: base_client,
            ccn_url: self.ccn_url,
            ipfs_gateway: self.ipfs_gateway,
        }
    }

    fn build_reqwest_client(&self) -> reqwest::Client {
        let mut builder =
            reqwest::Client::builder().connect_timeout(self.timeout_config.connect_timeout);
        if let Some(timeout) = self.timeout_config.request_timeout {
            builder = builder.timeout(timeout);
        }
        builder.build().expect("failed to build HTTP client")
    }
}

impl AlephClient {
    pub fn new(ccn_url: Url) -> Self {
        Self::builder(ccn_url).build()
    }

    pub fn builder(ccn_url: Url) -> AlephClientBuilder {
        AlephClientBuilder {
            ccn_url,
            retry_config: RetryConfig::default(),
            timeout_config: TimeoutConfig::default(),
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
            ipfs_gateway: Url::parse(crate::ipfs::DEFAULT_IPFS_GATEWAY)
                .expect("DEFAULT_IPFS_GATEWAY is a valid URL"),
        }
    }

    /// Overrides the IPFS gateway URL on an existing client.
    pub fn with_ipfs_gateway(mut self, gateway: Url) -> Self {
        self.ipfs_gateway = gateway;
        self
    }
}

impl AlephMessageClient for AlephClient {
    /// Queries a message by item hash.
    ///
    /// Returns the message with its corresponding status.
    async fn get_message(
        &self,
        item_hash: &ItemHash,
    ) -> Result<MessageWithStatus<Message>, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/messages/{}", item_hash))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(MessageError::NotFound(item_hash.clone()));
        }
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let get_message_response: GetMessageResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(get_message_response.message)
    }

    async fn get_messages(
        &self,
        filter: &MessageFilter,
        pagination: PaginationParams,
    ) -> Result<Vec<Message>, MessageError> {
        Ok(self.get_messages_raw(filter, &pagination).await?.messages)
    }

    fn get_messages_iterator(
        &self,
        filter: MessageFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<Message, MessageError>> + Send + '_ {
        let pagination = pagination
            .unwrap_or(CURSOR_DEFAULT_PAGINATION)
            .min(CURSOR_MAX_PAGINATION);
        async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            loop {
                let response = self
                    .get_messages_cursor(&filter, cursor.as_deref(), pagination)
                    .await?;
                for message in response.messages {
                    yield message;
                }
                cursor = match response.next_cursor {
                    Some(c) => Some(c),
                    None => break,
                };
            }
        }
    }

    async fn subscribe_to_messages(
        &self,
        filter: &MessageFilter,
        history: Option<u32>,
    ) -> Result<impl Stream<Item = Result<Message, MessageError>> + Send + Unpin, MessageError>
    {
        let rx = crate::ws::subscribe(self.ccn_url.clone(), filter, history).await?;
        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    async fn post_message(
        &self,
        message: &PendingMessage,
        sync: bool,
    ) -> Result<PostMessageResponse, MessageError> {
        let body = PostMessageBody { sync, message };

        let url = self
            .ccn_url
            .join("/api/v0/messages")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.post(url).json(&body).send().await?;

        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(MessageError::ApiError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        let response: PostMessageResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(response)
    }

    async fn get_messages_and_verify(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<MessageVerification>, MessageError> {
        let headers = self.get_message_headers(filter).await?;

        let verify_futures = headers
            .into_iter()
            .map(|header| verify_message_header(self, header));

        // All verifications run concurrently. Inline messages complete instantly (no I/O);
        // non-inline downloads are gated by the ConcurrencyLimit middleware.
        futures_util::stream::iter(verify_futures)
            .buffer_unordered(usize::MAX)
            .try_collect()
            .await
    }
}

/// Maximum items per page in cursor mode (server caps at 200 too).
const CURSOR_MAX_PAGINATION: u32 = 200;
const CURSOR_DEFAULT_PAGINATION: u32 = 200;

/// Cursor-mode response for messages. Private — only used by the iterators.
#[derive(Debug, Deserialize)]
struct MessagesCursorResponse {
    messages: Vec<Message>,
    next_cursor: Option<String>,
}

/// Cursor-mode response for v0 posts. Private — only used by the iterators.
#[derive(Debug, Deserialize)]
struct PostsV0CursorResponse {
    posts: Vec<PostV0>,
    next_cursor: Option<String>,
}

/// Cursor-mode response for v1 posts. Private — only used by the iterators.
#[derive(Debug, Deserialize)]
struct PostsV1CursorResponse {
    posts: Vec<PostV1>,
    next_cursor: Option<String>,
}

impl AlephClient {
    /// Fetches messages matching the filter, returning the full response including
    /// pagination metadata.
    ///
    /// Used by [`get_messages`](AlephMessageClient::get_messages).
    async fn get_messages_raw(
        &self,
        filter: &MessageFilter,
        pagination: &PaginationParams,
    ) -> Result<GetMessagesResponse, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/messages.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&pagination)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let get_messages_response: GetMessagesResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(get_messages_response)
    }

    /// Like [`get_messages_raw`] but uses cursor-based pagination.
    ///
    /// `cursor` is the opaque cursor from a previous response's `next_cursor`,
    /// or `None` for the first request (sends `cursor=` empty string to activate
    /// cursor mode on the server).
    async fn get_messages_cursor(
        &self,
        filter: &MessageFilter,
        cursor: Option<&str>,
        pagination: u32,
    ) -> Result<MessagesCursorResponse, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/messages.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let req = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&[("cursor", cursor.unwrap_or(""))])
            .query(&[("pagination", &pagination.to_string())]);

        let response = req
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let resp: MessagesCursorResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(resp)
    }

    /// Estimate the cost of a message before submitting it.
    ///
    /// Calls `POST /api/v0/price/estimate` on the CCN.
    pub async fn estimate_price(
        &self,
        message: &PendingMessage,
    ) -> Result<PriceEstimate, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/price/estimate")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let body = serde_json::json!({ "message": message });

        let response = self.http_client.post(url).json(&body).send().await?;

        let status = response.status();
        if status.is_client_error() || status.is_server_error() {
            let body_text = response.text().await.unwrap_or_default();
            return Err(MessageError::ApiError {
                status: status.as_u16(),
                body: body_text,
            });
        }

        let estimate: PriceEstimate = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(estimate)
    }

    /// Fetches messages matching the filter, returning only the headers (without content).
    ///
    /// Used by [`get_messages_and_verify`](AlephMessageClient::get_messages_and_verify) to avoid
    /// deserializing the CCN's `content` field, which is discarded in favor of content
    /// deserialized from verified raw bytes.
    async fn get_message_headers(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<MessageHeader>, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/messages.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let response: GetMessageHeadersResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(response.messages)
    }
}

impl AlephAuthorizationClient for AlephClient {
    async fn get_received_authorizations(
        &self,
        address: &Address,
    ) -> Result<Vec<ReceivedAuthorization>, MessageError> {
        /// The CCN `authorizations` field is either an object keyed by granter
        /// address (Python CCN) or an array of `ReceivedAuthorization` (heph).
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum AuthorizationsBody {
            Grouped(HashMap<String, Vec<crate::authorization::AuthorizationRule>>),
            List(Vec<ReceivedAuthorization>),
        }

        #[derive(Deserialize)]
        struct Response {
            authorizations: AuthorizationsBody,
        }

        let url = self
            .ccn_url
            .join(&format!("/api/v0/authorizations/received/{}.json", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&[("pagination", "200")])
            .send()
            .await?;

        let parsed: Response = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(match parsed.authorizations {
            AuthorizationsBody::Grouped(map) => map
                .into_iter()
                .map(|(granter, authorizations)| ReceivedAuthorization {
                    granter: Address::from(granter),
                    authorizations,
                })
                .collect(),
            AuthorizationsBody::List(list) => list,
        })
    }
}

#[derive(Deserialize)]
struct UploadResponse {
    hash: String,
}

/// Classifies an HTTP error response from the storage / IPFS upload
/// endpoints into the right `StorageError` variant.
///
/// Returns `None` for success codes and for unmapped non-success codes
/// (which the caller maps to `UploadFailed` to preserve retry semantics).
fn classify_status_and_body(status: reqwest::StatusCode, body: &str) -> Option<StorageError> {
    use reqwest::StatusCode;
    match status {
        StatusCode::PAYMENT_REQUIRED => Some(StorageError::InsufficientBalance),
        StatusCode::FORBIDDEN if body.contains("IPFS is disabled on this node") => {
            Some(StorageError::IpfsDisabled)
        }
        StatusCode::FORBIDDEN => Some(StorageError::InvalidSignature),
        StatusCode::PAYLOAD_TOO_LARGE => Some(StorageError::FileTooLarge),
        StatusCode::UNPROCESSABLE_ENTITY => {
            if body.contains("Root CID does not match")
                && let Some((car_root, metadata_root)) = parse_cid_pair(body)
            {
                return Some(StorageError::CarHeaderRootMismatch {
                    car_root,
                    metadata_root,
                });
            }
            if body.contains("Imported root does not match expected")
                && let Some((kubo_root, expected_root)) = parse_cid_pair(body)
            {
                return Some(StorageError::ImportedRootMismatch {
                    kubo_root,
                    expected_root,
                });
            }
            Some(StorageError::InvalidMetadata(body.to_string()))
        }
        StatusCode::BAD_GATEWAY | StatusCode::GATEWAY_TIMEOUT => {
            Some(StorageError::IpfsBackendUnavailable(body.to_string()))
        }
        _ => None,
    }
}

/// Parse `"... (A != B)..."` and return (A, B). Tolerates surrounding text.
fn parse_cid_pair(body: &str) -> Option<(String, String)> {
    let open = body.find('(')?;
    let close = body[open..].find(')')?;
    let inner = &body[open + 1..open + close];
    let (a, b) = inner.split_once(" != ")?;
    Some((a.trim().to_string(), b.trim().to_string()))
}

/// Inspects an upload response: returns the success response unchanged,
/// or maps non-success status codes to a `StorageError`.
///
/// 403, 422, 502, and 504 require body inspection; other statuses do not. For
/// unmapped non-success codes the helper falls back to `UploadFailed`
/// to preserve the existing retryable-error semantics.
async fn handle_storage_response(
    response: reqwest::Response,
) -> Result<reqwest::Response, StorageError> {
    use reqwest::StatusCode;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    if matches!(
        status,
        StatusCode::FORBIDDEN
            | StatusCode::UNPROCESSABLE_ENTITY
            | StatusCode::BAD_GATEWAY
            | StatusCode::GATEWAY_TIMEOUT
    ) {
        let body = response.text().await.unwrap_or_default();
        return Err(
            classify_status_and_body(status, &body).expect("403/422/502/504 always classify")
        );
    }
    if let Some(err) = classify_status_and_body(status, "") {
        return Err(err);
    }
    Err(StorageError::UploadFailed(reqwest_middleware::Error::from(
        response
            .error_for_status()
            .expect_err("non-success status checked above"),
    )))
}

/// Serializes the storage upload metadata field as JSON.
///
/// Both the storage and IPFS authenticated upload endpoints accept the
/// same `metadata` multipart field: a JSON object with the signed STORE
/// message and a `sync` flag.
fn serialize_storage_metadata(message: &PendingMessage, sync: bool) -> String {
    serde_json::json!({"message": message, "sync": sync}).to_string()
}

fn build_storage_metadata_part(message: &PendingMessage, sync: bool) -> reqwest::multipart::Part {
    reqwest::multipart::Part::text(serialize_storage_metadata(message, sync))
        .mime_str("application/json")
        .expect("application/json is a valid mime type")
}

/// Read a CARv1 body file fully and chain it after the in-memory header
/// bytes, returning a single `reqwest::Body`.
///
/// The CARv1 producer in `upload_folder_to_ipfs_authenticated` writes the
/// block-frame section to a tempfile during the DAG walk; this helper
/// prepends the header (built in memory after the walk completes) and
/// hands the combined body to reqwest. Buffers the whole CAR into memory
/// at upload time; streaming-first is a future optimization (the 4 GiB
/// server cap bounds memory usage).
async fn build_car_upload_body(
    header_bytes: Vec<u8>,
    car_body_path: &std::path::Path,
) -> std::io::Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let file_size = tokio::fs::metadata(car_body_path).await?.len() as usize;
    let mut file = tokio::fs::File::open(car_body_path).await?;
    let mut body = Vec::with_capacity(header_bytes.len() + file_size);
    body.extend_from_slice(&header_bytes);
    file.read_to_end(&mut body).await?;
    Ok(body)
}

/// Extract the file CID from a STORE `PendingMessage`.
///
/// For STORE messages the file hash is embedded in `item_content` as the
/// `"item_hash"` JSON field, not in `PendingMessage::item_hash` (which is the
/// SHA-256 of the serialized message body). This helper deserializes that JSON
/// and returns the file's `ItemHash`.
fn extract_message_item_hash(message: &PendingMessage) -> Result<ItemHash, StorageError> {
    let content: serde_json::Value = serde_json::from_str(&message.item_content).map_err(|e| {
        StorageError::InvalidMetadata(format!("STORE message item_content is not JSON: {e}"))
    })?;
    let raw = content
        .get("item_hash")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StorageError::InvalidMetadata("STORE message item_content is missing item_hash".into())
        })?;
    raw.parse::<ItemHash>().map_err(|source| {
        StorageError::InvalidMetadata(format!(
            "STORE message item_hash '{}' is not a valid ItemHash: {}",
            raw, source,
        ))
    })
}

impl AlephStorageClient for AlephClient {
    async fn get_file_size(&self, file_hash: &ItemHash) -> Result<Bytes, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/storage/raw/{}", file_hash))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .head(url)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let headers = response.headers();
        let content_length = headers
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok());

        content_length
            .ok_or_else(|| StorageError::NotFound(file_hash.clone()))
            .and_then(|s| {
                s.parse::<u64>()
                    .map(Bytes::from)
                    .map_err(|source| StorageError::InvalidSize {
                        value: s.to_string(),
                        source,
                    })
            })
            .map_err(MessageError::Storage)
    }

    async fn get_file_metadata_by_message_hash(
        &self,
        message_hash: &ItemHash,
    ) -> Result<FileMetadata, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/storage/by-message-hash/{}", message_hash))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(MessageError::NotFound(message_hash.clone()));
        }
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let file_metadata: FileMetadata = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(file_metadata)
    }

    async fn get_file_metadata_by_ref(
        &self,
        file_ref: &FileRef,
    ) -> Result<FileMetadata, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/storage/by-ref/{}", file_ref))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(StorageError::RefNotFound(file_ref.clone()).into());
        }
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let file_metadata: FileMetadata = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(file_metadata)
    }

    async fn download_file_by_hash(
        &self,
        file_hash: &ItemHash,
    ) -> Result<FileDownload, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/storage/raw/{}", file_hash))
            .map_err(StorageError::InvalidUrl)?;

        let response = self.http_client.get(url).send().await?;

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::NotFound(file_hash.clone()).into());
        }
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        Ok(FileDownload::new(response, file_hash.clone()))
    }

    async fn upload_to_storage(
        &self,
        data: &[u8],
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> Result<ItemHash, StorageError> {
        let url = self
            .ccn_url
            .join("/api/v0/storage/add_file")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name("upload")
            .mime_str("application/octet-stream")
            .expect("valid mime type");
        let mut form = reqwest::multipart::Form::new().part("file", part);

        if let Some(msg) = message {
            form = form.part("metadata", build_storage_metadata_part(msg, sync));
        }

        // Use the plain client — multipart bodies are not cloneable, so the
        // retry middleware would fail with "Request object is not cloneable".
        let response = self
            .upload_client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        let response = handle_storage_response(response).await?;

        let upload: UploadResponse = response
            .json()
            .await
            .map_err(StorageError::InvalidResponseBody)?;

        upload
            .hash
            .parse::<ItemHash>()
            .map_err(|source| StorageError::InvalidResponseHash {
                value: upload.hash.clone(),
                source,
            })
    }

    async fn upload_to_ipfs(
        &self,
        data: &[u8],
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> Result<ItemHash, StorageError> {
        let url = self
            .ccn_url
            .join("/api/v0/ipfs/add_file")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let part = reqwest::multipart::Part::bytes(data.to_vec())
            .file_name("upload")
            .mime_str("application/octet-stream")
            .expect("valid mime type");
        let mut form = reqwest::multipart::Form::new().part("file", part);

        if let Some(msg) = message {
            form = form.part("metadata", build_storage_metadata_part(msg, sync));
        }

        // Use the plain client — multipart bodies are not cloneable, so the
        // retry middleware would fail with "Request object is not cloneable".
        let response = self
            .upload_client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        let response = handle_storage_response(response).await?;

        let upload: UploadResponse = response
            .json()
            .await
            .map_err(StorageError::InvalidResponseBody)?;

        upload
            .hash
            .parse::<ItemHash>()
            .map_err(|source| StorageError::InvalidResponseHash {
                value: upload.hash.clone(),
                source,
            })
    }

    async fn upload_file_to_storage(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> Result<ItemHash, StorageError> {
        self.upload_file_streaming(
            "/api/v0/storage/add_file",
            Hasher::for_storage(),
            path.as_ref(),
            message,
            sync,
            None,
        )
        .await
    }

    async fn upload_file_to_ipfs(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        message: Option<&PendingMessage>,
        sync: bool,
    ) -> Result<ItemHash, StorageError> {
        self.upload_file_streaming(
            "/api/v0/ipfs/add_file",
            Hasher::for_ipfs(),
            path.as_ref(),
            message,
            sync,
            None,
        )
        .await
    }
}

impl AlephClient {
    /// Shared body for [`AlephStorageClient::upload_file_to_storage`] /
    /// `upload_file_to_ipfs` and their `_with_progress` variants.
    ///
    /// With `progress == None` the file is sent via `Part::file` (no per-chunk
    /// hook). With `Some(on_tick)` the file is streamed through
    /// [`crate::progress::report_upload_progress`] so the caller observes upload
    /// progress. In both cases the locally-computed hash is verified against the
    /// server's response.
    async fn upload_file_streaming(
        &self,
        endpoint_path: &str,
        hasher: Hasher,
        path: &std::path::Path,
        message: Option<&PendingMessage>,
        sync: bool,
        progress: Option<Box<dyn FnMut(u64, u64) + Send>>,
    ) -> Result<ItemHash, StorageError> {
        // Pass 1: stream file to compute the content hash locally.
        let local_hash = hash_file(path, hasher).await?;

        // Pass 2: upload the file.
        let url = self
            .ccn_url
            .join(endpoint_path)
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let part = match progress {
            Some(on_tick) => {
                // Stream from disk so each chunk passes through the progress
                // reporter. `Part::stream_with_length` keeps Content-Length set.
                let total = tokio::fs::metadata(path).await?.len();
                let file = tokio::fs::File::open(path).await?;
                let stream = tokio_util::io::ReaderStream::new(file);
                let body = reqwest::Body::wrap_stream(crate::progress::report_upload_progress(
                    stream, total, on_tick,
                ));
                let file_name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("file")
                    .to_string();
                reqwest::multipart::Part::stream_with_length(body, total)
                    .file_name(file_name)
                    .mime_str("application/octet-stream")
                    .expect("valid mime type")
            }
            // Part::file() returns io::Result<Part>, converts via StorageError::Io
            None => reqwest::multipart::Part::file(path)
                .await?
                .mime_str("application/octet-stream")
                .expect("valid mime type"),
        };
        let mut form = reqwest::multipart::Form::new().part("file", part);

        if let Some(msg) = message {
            form = form.part("metadata", build_storage_metadata_part(msg, sync));
        }

        let response = self
            .upload_client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        let response = handle_storage_response(response).await?;

        let upload: UploadResponse = response
            .json()
            .await
            .map_err(StorageError::InvalidResponseBody)?;

        let server_hash = upload.hash.parse::<ItemHash>().map_err(|source| {
            StorageError::InvalidResponseHash {
                value: upload.hash.clone(),
                source,
            }
        })?;

        if local_hash != server_hash {
            return Err(StorageError::UploadIntegrityMismatch {
                expected: local_hash,
                actual: server_hash,
            });
        }

        Ok(local_hash)
    }

    /// Like [`AlephStorageClient::upload_file_to_storage`] but reports upload
    /// progress: `on_tick(sent, total)` is called roughly every 500 ms and once
    /// more when the upload completes.
    pub async fn upload_file_to_storage_with_progress(
        &self,
        path: &std::path::Path,
        message: Option<&PendingMessage>,
        sync: bool,
        on_tick: impl FnMut(u64, u64) + Send + 'static,
    ) -> Result<ItemHash, StorageError> {
        self.upload_file_streaming(
            "/api/v0/storage/add_file",
            Hasher::for_storage(),
            path,
            message,
            sync,
            Some(Box::new(on_tick)),
        )
        .await
    }

    /// Like [`AlephStorageClient::upload_file_to_ipfs`] but reports upload
    /// progress; see [`Self::upload_file_to_storage_with_progress`].
    pub async fn upload_file_to_ipfs_with_progress(
        &self,
        path: &std::path::Path,
        message: Option<&PendingMessage>,
        sync: bool,
        on_tick: impl FnMut(u64, u64) + Send + 'static,
    ) -> Result<ItemHash, StorageError> {
        self.upload_file_streaming(
            "/api/v0/ipfs/add_file",
            Hasher::for_ipfs(),
            path,
            message,
            sync,
            Some(Box::new(on_tick)),
        )
        .await
    }

    /// Uploads a directory tree to the configured IPFS gateway and returns
    /// the root directory CID.
    ///
    /// The caller is responsible for posting any STORE message that references
    /// the returned hash.
    ///
    /// # Symlinks
    ///
    /// Symlinks inside `path` are dereferenced (per `collect_folder_files`
    /// `follow_symlinks=true` default) and their target's bytes are uploaded as
    /// regular files. The resulting CID will NOT match `ipfs add -r` on the
    /// same source folder if it contains symlinks — kubo's recursive add would
    /// build UnixFS `Symlink` nodes whereas this function uploads the resolved
    /// file bytes. Both the local hash and the gateway response agree on the
    /// dereferenced representation, so verification still succeeds.
    pub async fn upload_folder_to_ipfs(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        opts: crate::ipfs::UploadFolderOptions,
    ) -> Result<ItemHash, StorageError> {
        use crate::ipfs::{CollectError, build_add_query, collect_folder_files, parse_ndjson_root};

        let path = path.as_ref();
        let entries = collect_folder_files(path, opts.follow_symlinks).map_err(|e| match e {
            CollectError::Empty(p) => StorageError::EmptyFolder(p),
            CollectError::NonUtf8(p) => StorageError::NonUtf8Path(p),
            CollectError::Walk { source, .. } => StorageError::Io(source.into()),
            // `CollectError` is non-exhaustive now that it lives in `aleph-cid`.
            other => StorageError::Io(std::io::Error::other(other.to_string())),
        })?;

        let local_cid = ItemHash::Ipfs(crate::folder_hash::hash_folder_root(&entries, &opts)?);

        let mut form = reqwest::multipart::Form::new();
        for entry in entries {
            let part = reqwest::multipart::Part::file(&entry.absolute_path)
                .await?
                .file_name(entry.relative_path)
                .mime_str("application/octet-stream")
                .expect("valid mime type");
            form = form.part("file", part);
        }

        let query = build_add_query(&opts);
        let url = self
            .ipfs_gateway
            .join(&format!("/api/v0/add?{query}"))
            .map_err(StorageError::InvalidUrl)?;

        let response = self
            .upload_client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        match response.status() {
            StatusCode::FORBIDDEN => return Err(StorageError::IpfsDisabled),
            status if !status.is_success() => {
                return Err(StorageError::UploadFailed(reqwest_middleware::Error::from(
                    response
                        .error_for_status()
                        .expect_err("already checked non-success"),
                )));
            }
            _ => {}
        }

        let body = response
            .text()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        let cid = parse_ndjson_root(&body)?;
        let remote_cid = ItemHash::Ipfs(cid);
        if local_cid != remote_cid {
            return Err(StorageError::CidMismatch {
                local: local_cid,
                remote: remote_cid,
            });
        }
        Ok(remote_cid)
    }

    /// Upload a directory to pyaleph's authenticated IPFS CAR endpoint.
    ///
    /// The SDK rebuilds the UnixFS DAG locally, asserts its root matches
    /// `message.item_hash` (fails with `CidMismatch` if not), writes
    /// the DAG into a CARv1 temp file, and posts CAR + signed STORE metadata
    /// to `/api/v0/ipfs/add_car`. Returns the root `ItemHash` on success.
    pub async fn upload_folder_to_ipfs_authenticated(
        &self,
        path: impl AsRef<std::path::Path> + Send,
        message: &PendingMessage,
        sync: bool,
        opts: crate::ipfs::UploadFolderOptions,
    ) -> Result<ItemHash, StorageError> {
        use crate::car::{write_block_frame, write_carv1_header};
        use crate::folder_hash::build_folder_dag;
        use crate::ipfs::{CollectError, collect_folder_files};
        use std::io::Write;

        let path = path.as_ref();
        let entries = collect_folder_files(path, opts.follow_symlinks).map_err(|e| match e {
            CollectError::Empty(p) => StorageError::EmptyFolder(p),
            CollectError::NonUtf8(p) => StorageError::NonUtf8Path(p),
            CollectError::Walk { source, .. } => StorageError::Io(source.into()),
            // `CollectError` is non-exhaustive now that it lives in `aleph-cid`.
            other => StorageError::Io(std::io::Error::other(other.to_string())),
        })?;

        // 1. Walk the DAG, stream block frames into a tempfile body.
        let mut body_tmp = tempfile::NamedTempFile::new()?;
        let mut last_cid_bytes: Option<Vec<u8>> = None;
        let local_root = build_folder_dag(&entries, &opts, &mut |cid, block| {
            write_block_frame(&mut body_tmp, cid, block)?;
            last_cid_bytes = Some(cid.to_vec());
            Ok(())
        })?;
        body_tmp.flush()?;
        let local_root = ItemHash::Ipfs(local_root);
        let root_cid_bytes =
            last_cid_bytes.expect("build_folder_dag always emits at least the root");

        // 2. Sanity check the local root against metadata.item_hash.
        let metadata_root = extract_message_item_hash(message)?;
        if local_root != metadata_root {
            return Err(StorageError::CidMismatch {
                local: local_root,
                remote: metadata_root,
            });
        }

        // 3. Build the CARv1 header bytes.
        let mut header_bytes = Vec::new();
        write_carv1_header(&mut header_bytes, &root_cid_bytes)?;

        // 4. Construct the multipart body.
        let body = build_car_upload_body(header_bytes, body_tmp.path()).await?;
        let file_part = reqwest::multipart::Part::bytes(body)
            .file_name("upload.car")
            .mime_str("application/vnd.ipld.car")
            .expect("application/vnd.ipld.car is a valid mime");
        let form = reqwest::multipart::Form::new()
            .part("file", file_part)
            .part("metadata", build_storage_metadata_part(message, sync));

        let url = self
            .ccn_url
            .join("/api/v0/ipfs/add_car")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        // 5. POST and classify the response.
        let response = self
            .upload_client
            .post(url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| StorageError::UploadFailed(reqwest_middleware::Error::from(e)))?;

        let response = handle_storage_response(response).await?;
        let upload: UploadResponse = response
            .json()
            .await
            .map_err(StorageError::InvalidResponseBody)?;
        let server_hash = upload.hash.parse::<ItemHash>().map_err(|source| {
            StorageError::InvalidResponseHash {
                value: upload.hash.clone(),
                source,
            }
        })?;
        if server_hash != local_root {
            return Err(StorageError::UploadIntegrityMismatch {
                expected: local_root,
                actual: server_hash,
            });
        }
        Ok(local_root)
    }
}

#[derive(Debug, Deserialize)]
pub struct AccountBalance {
    #[serde(rename = "balance")]
    pub aleph_tokens: f64,
    #[serde(rename = "locked_amount")]
    pub locked_aleph_tokens: f64,
    #[serde(default, rename = "credit_balance")]
    pub credits: u64,
}

/// One row of `/api/v0/addresses/{address}/credit_history`.
///
/// Mirrors `aleph.schemas.api.accounts.CreditHistoryResponseItem` in pyaleph.
/// Optional fields reflect that purchase / transfer / expiration entries
/// populate different subsets.
#[derive(Debug, Deserialize, Serialize)]
pub struct CreditHistoryItem {
    pub amount: i64,
    /// Per-credit price at the time of the entry, serialized as a decimal
    /// string (e.g. `"0.000001"`). `None` for non-purchase rows.
    #[serde(default)]
    pub price: Option<String>,
    #[serde(default)]
    pub bonus_amount: Option<i64>,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default)]
    pub chain: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub origin_ref: Option<String>,
    #[serde(default)]
    pub payment_method: Option<String>,
    pub credit_ref: String,
    pub credit_index: i64,
    #[serde(default)]
    pub expiration_date: Option<DateTime<Utc>>,
    pub message_timestamp: DateTime<Utc>,
}

/// Page-paginated response from `/api/v0/addresses/{address}/credit_history`.
#[derive(Debug, Deserialize, Serialize)]
pub struct CreditHistoryResponse {
    pub address: String,
    pub credit_history: Vec<CreditHistoryItem>,
    pub pagination_page: u32,
    pub pagination_total: u64,
    pub pagination_per_page: u32,
}

/// Sign-based classification of a credit history entry, mirroring pyaleph's
/// `CreditFlow`. Incoming entries have a positive amount (distributions,
/// received transfers); outgoing entries have a negative amount (expenses,
/// sent transfers).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreditDirection {
    Incoming,
    Outgoing,
}

impl CreditDirection {
    /// The `direction` query-param value expected by pyaleph.
    fn as_wire(self) -> &'static str {
        match self {
            CreditDirection::Incoming => "incoming",
            CreditDirection::Outgoing => "outgoing",
        }
    }
}

/// Server-side filters shared by the credit history listing and summary
/// endpoints. All fields are optional; the default value applies no filtering.
#[derive(Debug, Default, Clone)]
pub struct CreditHistoryFilters {
    /// Inclusive lower bound on `message_timestamp`, as Unix seconds.
    pub start_date: Option<i64>,
    /// Inclusive upper bound on `message_timestamp`, as Unix seconds.
    pub end_date: Option<i64>,
    /// Restrict to incoming (top-ups) or outgoing (expenses) entries.
    pub direction: Option<CreditDirection>,
    /// Restrict to entries whose billed resource is one of these message
    /// types (e.g. `STORE` for storage, `INSTANCE`/`PROGRAM` for compute).
    pub resource_types: Vec<MessageType>,
}

impl CreditHistoryFilters {
    /// Render the filters as repeated query parameters in pyaleph's camelCase
    /// wire vocabulary. Empty/`None` fields are omitted.
    fn query_params(&self) -> Vec<(&'static str, String)> {
        let mut params = Vec::new();
        if let Some(start) = self.start_date {
            params.push(("startDate", start.to_string()));
        }
        if let Some(end) = self.end_date {
            params.push(("endDate", end.to_string()));
        }
        if let Some(direction) = self.direction {
            params.push(("direction", direction.as_wire().to_string()));
        }
        if !self.resource_types.is_empty() {
            let joined = self
                .resource_types
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(",");
            params.push(("resourceTypes", joined));
        }
        params
    }
}

/// Aggregate spend totals from
/// `/api/v0/addresses/{address}/credit_history/summary`.
///
/// Mirrors `aleph.schemas.api.accounts.GetAccountCreditHistorySummaryResponse`
/// in pyaleph. `total_incoming` is the sum of positive amounts (>= 0),
/// `total_outgoing` the sum of negative amounts (<= 0), and `total_amount`
/// their net.
#[derive(Debug, Deserialize, Serialize)]
pub struct CreditHistorySummary {
    pub address: String,
    pub entry_count: i64,
    pub total_amount: i64,
    pub total_incoming: i64,
    pub total_outgoing: i64,
}

/// One row of `/api/v0/addresses/{address}/files`.
///
/// `created` is a Unix-epoch timestamp (the cursor-mode shape used by the
/// iterator). The non-cursor mode of the endpoint serializes `created` as
/// an ISO string instead, but the SDK only ever reads cursor mode.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AccountFile {
    pub file_hash: String,
    pub size: Bytes,
    /// Server-side storage backend tag (e.g. `"file"`, `"dir"`).
    #[serde(rename = "type")]
    pub storage_engine: String,
    pub created: aleph_types::timestamp::Timestamp,
    pub item_hash: ItemHash,
}

/// One cursor-mode page of `/api/v0/addresses/{address}/files`. Private:
/// callers either walk the iterator or hit `get_total_storage_size` for the
/// address-wide total.
///
/// `total_size` is read as `f64` because pyaleph emits it as a float in
/// cursor mode (e.g. `78051738.0`); the wrapper rounds to the nearest byte.
#[derive(Debug, Deserialize)]
struct AccountFilesCursorResponse {
    #[serde(default)]
    files: Vec<AccountFile>,
    #[serde(default)]
    total_size: f64,
    #[serde(default)]
    next_cursor: Option<String>,
}

impl AccountFilesCursorResponse {
    fn total_size_bytes(&self) -> Bytes {
        Bytes::from(self.total_size.round().max(0.0) as u64)
    }
}

#[derive(Debug, Deserialize)]
struct GetPriceResponse {
    required_tokens: f64,
}

impl AlephAccountClient for AlephClient {
    async fn get_balance(&self, address: &Address) -> Result<AccountBalance, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/addresses/{}/balance", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let account_balance: AccountBalance = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(account_balance)
    }

    async fn get_total_storage_size(&self, address: &Address) -> Result<Bytes, MessageError> {
        // Cursor mode with the smallest possible page still carries the
        // address-wide `total_size`. Costs us one cheap row over the wire.
        let page = self
            .get_account_files_cursor(address, None, 1, None)
            .await?;
        Ok(page.total_size_bytes())
    }

    fn get_account_files_iterator(
        &self,
        address: &Address,
        pagination: Option<u32>,
        sort_order: Option<i8>,
    ) -> impl Stream<Item = Result<AccountFile, MessageError>> + Send + '_ {
        let pagination = pagination
            .unwrap_or(CURSOR_DEFAULT_PAGINATION)
            .min(CURSOR_MAX_PAGINATION);
        let address = address.clone();
        async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            loop {
                let response = self
                    .get_account_files_cursor(&address, cursor.as_deref(), pagination, sort_order)
                    .await?;
                let next = response.next_cursor.clone();
                for file in response.files {
                    yield file;
                }
                cursor = match next {
                    Some(c) => Some(c),
                    None => break,
                };
            }
        }
    }

    async fn get_vm_price(&self, item_hash: &ItemHash) -> Result<f64, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/price/{}", item_hash))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let get_price_response: GetPriceResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(get_price_response.required_tokens)
    }

    async fn get_credit_history(
        &self,
        address: &Address,
        page: u32,
        page_size: Option<u32>,
        filters: &CreditHistoryFilters,
    ) -> Result<CreditHistoryResponse, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/addresses/{}/credit_history", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let mut request = self
            .http_client
            .get(url)
            .query(&[("page", page.to_string())])
            .query(&filters.query_params());
        if let Some(per_page) = page_size {
            request = request.query(&[("pagination", per_page.to_string())]);
        }

        let response = request.send().await?;

        // pyaleph returns 404 when the address has no credit history rather
        // than an empty page. Surface that as an empty response so callers
        // don't have to special-case the status code.
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(CreditHistoryResponse {
                address: address.to_string(),
                credit_history: Vec::new(),
                pagination_page: page,
                pagination_total: 0,
                pagination_per_page: page_size.unwrap_or(0),
            });
        }

        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let history: CreditHistoryResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(history)
    }

    async fn get_credit_history_summary(
        &self,
        address: &Address,
        filters: &CreditHistoryFilters,
    ) -> Result<CreditHistorySummary, MessageError> {
        let path = format!("/api/v0/addresses/{}/credit_history/summary", address);
        let url = self
            .ccn_url
            .join(&path)
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filters.query_params())
            .send()
            .await?;

        // Unlike the listing endpoint, the summary endpoint always returns 200
        // (with zeroed totals for an unknown address). A 404 therefore means
        // the route does not exist, i.e. the CCN predates this endpoint, so we
        // surface it as an error rather than silently reporting zero spend.
        if response.status() == StatusCode::NOT_FOUND {
            return Err(MessageError::ApiError {
                status: StatusCode::NOT_FOUND.as_u16(),
                body: format!(
                    "credit history summary endpoint not found ({path}); \
                     this CCN may be running a version without support for it"
                ),
            });
        }

        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let summary: CreditHistorySummary = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(summary)
    }
}

impl AlephClient {
    /// Fetch one cursor-mode page of `/api/v0/addresses/{address}/files`.
    ///
    /// Passing any value for `cursor` (including an empty string on the
    /// first request) activates the server's cursor mode. Returns an empty
    /// page when the address has no files (the endpoint responds with 404
    /// in that case).
    async fn get_account_files_cursor(
        &self,
        address: &Address,
        cursor: Option<&str>,
        pagination: u32,
        sort_order: Option<i8>,
    ) -> Result<AccountFilesCursorResponse, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/addresses/{}/files", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let mut req = self
            .http_client
            .get(url)
            .query(&[("cursor", cursor.unwrap_or(""))])
            .query(&[("pagination", &pagination.to_string())]);
        if let Some(order) = sort_order {
            req = req.query(&[("sort_order", &order.to_string())]);
        }

        let response = req.send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(AccountFilesCursorResponse {
                files: Vec::new(),
                total_size: 0.0,
                next_cursor: None,
            });
        }
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let page: AccountFilesCursorResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(page)
    }
}

/// Shape of `/api/v0/aggregates/{address}.json` when fetching multiple
/// keys (or every key) at once. Shared by `get_aggregates` and
/// `get_all_aggregates`.
#[derive(Deserialize)]
struct AggregatesResponse {
    data: HashMap<String, serde_json::Value>,
}

/// Maps a `404 Not Found` from `get_aggregate` to `Ok(None)` so callers that
/// semantically treat "no aggregate stored" as empty don't need to discriminate
/// between "200 with empty data" and "404 missing key".
///
/// The CCN's `/api/v0/aggregates/{address}.json?keys=...` endpoint is inconsistent:
/// most deployments return `200 {"data": null}` for an unknown address+key pair, but
/// some return `404`. Both should surface to callers as "no data". `get_aggregate`
/// surfaces a 404 as `MessageError::HttpError` (the `error_for_status()` route),
/// so we match on that variant and inspect its status code.
///
/// Only `404` is swallowed; other transport errors (timeouts, 5xx, decode failures)
/// are propagated unchanged.
fn map_aggregate_404_to_empty(
    result: Result<Option<serde_json::Value>, MessageError>,
) -> Result<Option<serde_json::Value>, MessageError> {
    match result {
        Ok(value) => Ok(value),
        Err(MessageError::HttpError(ref e)) if e.status() == Some(StatusCode::NOT_FOUND) => {
            Ok(None)
        }
        Err(e) => Err(e),
    }
}

/// Descends through the inner aggregate envelope returned by `get_aggregate::<Option<Value>>`.
///
/// The CCN responds with `{"data": <inner>}`. `get_aggregate` strips the outer `data`
/// wrapper, so `inner` is what we receive here. The full mapping from CCN response to
/// helper output is:
///
/// * `200` with `data: null` (`raw == None`) -> `T::default()` (empty).
/// * `200` with `data` an object that does not contain `key` -> `T::default()`.
/// * `200` with `data[key]` explicitly `null` -> `T::default()`.
/// * `200` with `data[key]` parseable as `T` -> that value.
/// * `200` with `data[key]` present but not deserializable as `T` ->
///   `MessageError::ApiError { status: 200, body: "invalid <key> aggregate: ..." }`.
/// * `200` with a malformed envelope, i.e. `data` is something other than an object
///   (e.g. a string or number) -> `T::default()`. This is intentional graceful
///   degradation: a transiently broken CCN should not nuke a user-facing list
///   command. Logging or alerting on this case belongs at a higher layer; this
///   helper deliberately swallows it.
fn extract_aggregate_value<T>(raw: Option<serde_json::Value>, key: &str) -> Result<T, MessageError>
where
    T: DeserializeOwned + Default,
{
    let mut map = match raw {
        Some(serde_json::Value::Object(map)) => map,
        _ => return Ok(T::default()),
    };

    let inner = match map.remove(key) {
        None | Some(serde_json::Value::Null) => return Ok(T::default()),
        Some(value) => value,
    };

    serde_json::from_value(inner).map_err(|e| MessageError::ApiError {
        status: 200,
        body: format!("invalid {key} aggregate: {e}"),
    })
}

impl AlephAggregateClient for AlephClient {
    async fn get_aggregate<T: DeserializeOwned>(
        &self,
        address: &Address,
        key: &str,
    ) -> Result<T, MessageError> {
        #[derive(Deserialize)]
        struct AggregateResponse<T> {
            data: T,
        }

        let url = self
            .ccn_url
            .join(&format!("/api/v0/aggregates/{}.json", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&[("keys", key)])
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let aggregate_response: AggregateResponse<T> = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(aggregate_response.data)
    }

    async fn get_websites_aggregate(
        &self,
        address: &Address,
    ) -> Result<WebsitesAggregate, MessageError> {
        let raw = map_aggregate_404_to_empty(
            self.get_aggregate::<Option<serde_json::Value>>(address, WEBSITES_AGGREGATE_KEY)
                .await,
        )?;
        extract_aggregate_value(raw, WEBSITES_AGGREGATE_KEY)
    }

    async fn get_domains_aggregate(
        &self,
        address: &Address,
    ) -> Result<DomainsAggregate, MessageError> {
        let raw = map_aggregate_404_to_empty(
            self.get_aggregate::<Option<serde_json::Value>>(address, DOMAINS_AGGREGATE_KEY)
                .await,
        )?;
        extract_aggregate_value(raw, DOMAINS_AGGREGATE_KEY)
    }

    async fn get_port_forwarding_aggregate(
        &self,
        address: &Address,
    ) -> Result<PortForwardingAggregate, MessageError> {
        let raw = map_aggregate_404_to_empty(
            self.get_aggregate::<Option<serde_json::Value>>(address, PORT_FORWARDING_AGGREGATE_KEY)
                .await,
        )?;
        extract_aggregate_value(raw, PORT_FORWARDING_AGGREGATE_KEY)
    }

    async fn get_aggregates(
        &self,
        address: &Address,
        keys: &[&str],
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        if keys.is_empty() {
            return Err(MessageError::ApiError {
                status: 0,
                body: "get_aggregates requires at least one key".to_string(),
            });
        }

        let url = self
            .ccn_url
            .join(&format!("/api/v0/aggregates/{}.json", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let keys_csv = keys.join(",");
        let response = self
            .http_client
            .get(url)
            .query(&[("keys", &keys_csv)])
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let aggregates_response: AggregatesResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(aggregates_response.data)
    }

    async fn get_all_aggregates(
        &self,
        address: &Address,
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/aggregates/{}.json", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?;

        // pyaleph returns 404 when the address has no aggregates rather than
        // an empty data map. Treat it as an empty result so callers don't
        // have to special-case the status code.
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(HashMap::new());
        }

        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let aggregates_response: AggregatesResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(aggregates_response.data)
    }
}

impl AlephClient {
    async fn get_posts_v0_cursor(
        &self,
        filter: &PostFilter,
        cursor: Option<&str>,
        pagination: u32,
    ) -> Result<PostsV0CursorResponse, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let req = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&[("cursor", cursor.unwrap_or(""))])
            .query(&[("pagination", &pagination.to_string())]);

        let response = req
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let resp: PostsV0CursorResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(resp)
    }

    async fn get_posts_v1_cursor(
        &self,
        filter: &PostFilter,
        cursor: Option<&str>,
        pagination: u32,
    ) -> Result<PostsV1CursorResponse, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v1/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let req = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&[("cursor", cursor.unwrap_or(""))])
            .query(&[("pagination", &pagination.to_string())]);

        let response = req
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let resp: PostsV1CursorResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(resp)
    }
}

impl AlephPostClient for AlephClient {
    async fn get_posts_v0(
        &self,
        filter: &PostFilter,
        pagination: PaginationParams,
    ) -> Result<GetPostsV0Response, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&pagination)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let posts_response: GetPostsV0Response = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(posts_response)
    }

    async fn get_posts_v1(
        &self,
        filter: &PostFilter,
        pagination: PaginationParams,
    ) -> Result<GetPostsV1Response, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v1/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
            .query(&pagination)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let posts_response: GetPostsV1Response = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(posts_response)
    }

    fn get_posts_v0_iterator(
        &self,
        filter: PostFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<PostV0, MessageError>> + Send + '_ {
        let pagination = pagination
            .unwrap_or(CURSOR_DEFAULT_PAGINATION)
            .min(CURSOR_MAX_PAGINATION);
        async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            loop {
                let response = self
                    .get_posts_v0_cursor(&filter, cursor.as_deref(), pagination)
                    .await?;
                for post in response.posts {
                    yield post;
                }
                cursor = match response.next_cursor {
                    Some(c) => Some(c),
                    None => break,
                };
            }
        }
    }

    fn get_posts_v1_iterator(
        &self,
        filter: PostFilter,
        pagination: Option<u32>,
    ) -> impl Stream<Item = Result<PostV1, MessageError>> + Send + '_ {
        let pagination = pagination
            .unwrap_or(CURSOR_DEFAULT_PAGINATION)
            .min(CURSOR_MAX_PAGINATION);
        async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            loop {
                let response = self
                    .get_posts_v1_cursor(&filter, cursor.as_deref(), pagination)
                    .await?;
                for post in response.posts {
                    yield post;
                }
                cursor = match response.next_cursor {
                    Some(c) => Some(c),
                    None => break,
                };
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_models::corechannel::CORECHANNEL_ADDRESS;
    use aleph_types::{address, channel, item_hash};

    #[test]
    fn sort_by_serializes_kebab_case() {
        assert_eq!(serde_json::to_value(SortBy::Time).unwrap(), "time");
        assert_eq!(serde_json::to_value(SortBy::TxTime).unwrap(), "tx-time");
    }

    #[test]
    fn sort_order_serializes_as_signed_int() {
        assert_eq!(serde_json::to_value(SortOrder::Asc).unwrap(), 1);
        assert_eq!(serde_json::to_value(SortOrder::Desc).unwrap(), -1);
    }

    #[test]
    fn test_storage_error_display() {
        assert_eq!(
            StorageError::InsufficientBalance.to_string(),
            "Insufficient balance"
        );
        assert_eq!(
            StorageError::IpfsDisabled.to_string(),
            "IPFS is disabled on this node"
        );
        assert_eq!(StorageError::FileTooLarge.to_string(), "File too large");
    }

    const FORGOTTEN_MESSAGE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/api-responses/forgotten-message.json"
    ));
    const PENDING_MESSAGE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/api-responses/pending-message.json"
    ));

    #[test]
    fn test_deserialize_pending_message() {
        let message: MessageWithStatus<Message> = serde_json::from_str(PENDING_MESSAGE).unwrap();

        match message {
            MessageWithStatus::Pending { messages } => {
                assert_eq!(messages.len(), 1);
                let pending = &messages[0];
                assert_eq!(pending.chain, Chain::Ethereum);
                assert_eq!(
                    pending.item_hash,
                    item_hash!("cab98cd9e1f957bd99259acff3eb35d960436121c7f567a2c9cb941c24e0c01b")
                );
                assert_eq!(
                    pending.sender,
                    address!("0x4D52380D3191274a04846c89c069E6C3F2Ed94e4")
                );
                assert_eq!(pending.message_type, MessageType::Post);
                assert_eq!(pending.content_source, ContentSource::Storage);
                assert_eq!(pending.channel, Some(channel!("aleph-scoring")));
                assert!(pending.content.is_none());
            }
            _ => panic!("Expected Pending variant"),
        }
    }

    #[test]
    fn test_deserialize_forgotten_message() {
        let message: MessageWithStatus<Message> = serde_json::from_str(FORGOTTEN_MESSAGE).unwrap();

        match message {
            MessageWithStatus::Forgotten {
                message,
                forgotten_by,
            } => {
                assert_eq!(message.chain, Chain::Ethereum);
                assert_eq!(
                    message.item_hash,
                    item_hash!("821d7b01866bdfafc8d07539d6191061ab5858dfbfcab046d7b799e5e01da51f")
                );
                assert_eq!(
                    message.sender,
                    address!("0xCBD8c644d5628DB7Fd6D600681E15bcF82d79274")
                );
                assert_eq!(message.message_type, MessageType::Store);
                assert_eq!(message.channel, Some(channel!("TEST")));

                assert_eq!(forgotten_by.len(), 1);
                assert_eq!(
                    forgotten_by[0],
                    item_hash!("3292ebfacccf1315ad21615101661b147dabfb2e1f97d7c46262a528a3e22852")
                );
            }
            _ => panic!("Expected Forgotten variant"),
        }
    }

    /// Inline POST message with `signature: null`. Content hash matches
    /// `item_hash`. Used to exercise the unsigned-but-integrity-checked path.
    const INLINE_UNSIGNED_POST: &str = r#"{
        "sender": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
        "chain": "ETH",
        "signature": null,
        "type": "POST",
        "item_content": "{\"type\":\"05567c5b-0606-4a6e-a639-25734c06e2a0\",\"address\":\"0xB68B9D4f3771c246233823ed1D3Add451055F9Ef\",\"content\":{\"body\":\"Hello World\"},\"time\":1762515431.653}",
        "item_type": "inline",
        "item_hash": "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c",
        "time": 1762515431.653,
        "channel": "TEST",
        "content": {
            "address": "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef",
            "time": 1762515431.653,
            "content": { "body": "Hello World" },
            "ref": null,
            "type": "05567c5b-0606-4a6e-a639-25734c06e2a0"
        },
        "confirmed": false,
        "confirmations": []
    }"#;

    fn inline_only_client() -> AlephClient {
        // Inline verification never touches the network; any URL works.
        AlephClient::new(Url::parse("http://test.invalid").unwrap())
    }

    #[tokio::test]
    async fn unsigned_inline_message_passes_integrity_and_resolves_to_unsigned() {
        let message: Message = serde_json::from_str(INLINE_UNSIGNED_POST).unwrap();
        assert!(message.signature.is_none());

        let header = MessageHeader::from(message);
        let outcome = verify_message_header(&inline_only_client(), header)
            .await
            .expect("inline verification should not perform I/O");

        match outcome {
            MessageVerification::Unsigned(u) => {
                assert_eq!(
                    u.message().item_hash,
                    item_hash!("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c")
                );
            }
            other => panic!("expected Unsigned, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn unsigned_inline_message_with_tampered_content_is_invalid() {
        let message: Message = serde_json::from_str(INLINE_UNSIGNED_POST).unwrap();
        let mut header = MessageHeader::from(message);
        // Tamper item_content — its hash will no longer match item_hash.
        if let ContentSource::Inline {
            ref mut item_content,
        } = header.content_source
        {
            item_content.push_str(" tampered");
        } else {
            panic!("fixture must be inline");
        }

        let outcome = verify_message_header(&inline_only_client(), header)
            .await
            .expect("inline verification should not perform I/O");

        assert!(matches!(
            outcome,
            MessageVerification::Invalid(InvalidMessage {
                error: IntegrityError::HashMismatch { .. },
                ..
            })
        ));
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN with IPFS — no heph equivalent yet"]
    async fn test_download_cidv0_with_verification() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash = item_hash!("QmQKPXPMENCLL7HfyPiTZkmyX5iHp5QrYdWWMeP6pEhiS4");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        let content = download
            .with_verification()
            .bytes()
            .await
            .expect("CIDv0 verified download should succeed");
        assert!(!content.is_empty());
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN with IPFS — no heph equivalent yet"]
    async fn test_download_large_cidv0_with_verification() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash = item_hash!("QmdFaKjHBGsU525nHD6fgH7o1YnGZgfNo1x9jspzwCaR9N");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        let content = download
            .with_verification()
            .bytes()
            .await
            .expect("large CIDv0 verified download should succeed");
        assert!(!content.is_empty());
    }

    #[test]
    fn test_ws_message_filter_serialization() {
        let filter = MessageFilter {
            message_type: Some(MessageType::Post),
            addresses: Some(vec![address!("0x1234")]),
            channels: Some(vec!["TEST".to_string()]),
            ..Default::default()
        };

        let query = serde_qs::to_string(&filter).unwrap();
        assert!(query.contains("msgType=POST"));
        assert!(query.contains("addresses=0x1234"));
        assert!(query.contains("channels=TEST"));
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN — requires corechannel data not in heph"]
    async fn test_get_corechannel_aggregate() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));

        let aggregate = client
            .get_corechannel_aggregate(&CORECHANNEL_ADDRESS)
            .await
            .unwrap_or_else(|e| panic!("failed to fetch corechannel aggregate: {:?}", e));

        assert!(
            !aggregate.corechannel.nodes.is_empty(),
            "should have at least one CCN"
        );
        assert!(
            !aggregate.corechannel.resource_nodes.is_empty(),
            "should have at least one CRN"
        );
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN websocket — no heph equivalent yet"]
    async fn test_subscribe_to_messages() {
        use futures_util::StreamExt;

        let client = AlephClient::new(Url::parse("https://api2.aleph.im").expect("valid url"));
        let filter = MessageFilter {
            message_type: Some(MessageType::Post),
            ..Default::default()
        };

        let mut stream = client
            .subscribe_to_messages(&filter, None)
            .await
            .expect("should connect");

        // Read up to 5 historical messages
        let mut count = 0;
        while let Some(result) = stream.next().await {
            match result {
                Ok(msg) => {
                    assert_eq!(msg.message_type, MessageType::Post);
                    count += 1;
                    if count >= 5 {
                        break;
                    }
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }

        assert!(count > 0, "should have received at least one message");
    }

    #[test]
    #[should_panic(expected = "max_concurrent_requests must be > 0")]
    fn test_builder_rejects_zero_concurrency() {
        AlephClient::builder(Url::parse("https://api3.aleph.im").unwrap())
            .max_concurrent_requests(0)
            .build();
    }

    /// Spin up a TCP listener that accepts connections but never sends a response,
    /// simulating a stalled server. Returns the URL to connect to.
    async fn start_stalling_server() -> Url {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                // Accept the connection and hold it open without responding.
                let Ok((_stream, _)) = listener.accept().await else {
                    break;
                };
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
        });
        Url::parse(&format!("http://{addr}")).unwrap()
    }

    #[tokio::test]
    async fn test_request_timeout_fires_on_stalled_server() {
        let url = start_stalling_server().await;
        let client = AlephClient::builder(url)
            .timeout_config(TimeoutConfig {
                connect_timeout: Duration::from_secs(5),
                request_timeout: Some(Duration::from_millis(200)),
            })
            .retry_config(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build();

        let start = std::time::Instant::now();
        let hash = aleph_types::item_hash!(
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
        let result = client.get_message(&hash).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "should fail with timeout");
        // The server sleeps for an hour, so any return well under that proves the
        // 200ms request timeout fired. The margin is deliberately generous: loaded
        // CI runners (especially Windows) can starve the single-threaded test
        // runtime for several seconds before the timer is serviced.
        assert!(
            elapsed < Duration::from_secs(60),
            "should bail out via the request timeout (took {elapsed:?}), not hang indefinitely"
        );
    }

    #[tokio::test]
    async fn test_connect_timeout_fires_on_unreachable_host() {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737) — routable nowhere, will time out.
        let client = AlephClient::builder(Url::parse("http://192.0.2.1:1").unwrap())
            .timeout_config(TimeoutConfig {
                connect_timeout: Duration::from_millis(200),
                request_timeout: Some(Duration::from_secs(120)),
            })
            .retry_config(RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build();

        let start = std::time::Instant::now();
        let hash = aleph_types::item_hash!(
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
        let result = client.get_message(&hash).await;
        let elapsed = start.elapsed();

        assert!(result.is_err(), "should fail with connect timeout");
        // Returning well under the 120s request budget proves the 200ms connect
        // timeout is what fired. The margin is generous because loaded CI runners
        // (especially Windows) can starve the single-threaded test runtime for
        // several seconds before the timer is serviced.
        assert!(
            elapsed < Duration::from_secs(30),
            "should bail out via the connect timeout (took {elapsed:?})"
        );
    }

    #[tokio::test]
    async fn test_semaphore_limits_concurrency() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let max_concurrency: usize = 3;
        let total_tasks: usize = 20;

        let semaphore = Arc::new(Semaphore::new(max_concurrency));
        let active = Arc::new(AtomicUsize::new(0));
        let max_observed = Arc::new(AtomicUsize::new(0));

        let futures = (0..total_tasks).map(|_| {
            let semaphore = semaphore.clone();
            let active = active.clone();
            let max_observed = max_observed.clone();
            async move {
                let _permit = semaphore.acquire().await.unwrap();
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                max_observed.fetch_max(current, Ordering::SeqCst);
                // Simulate work
                tokio::task::yield_now().await;
                active.fetch_sub(1, Ordering::SeqCst);
            }
        });

        futures_util::stream::iter(futures)
            .buffer_unordered(usize::MAX)
            .collect::<Vec<_>>()
            .await;

        assert!(
            max_observed.load(Ordering::SeqCst) <= max_concurrency,
            "observed {} concurrent tasks, expected at most {}",
            max_observed.load(Ordering::SeqCst),
            max_concurrency,
        );
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN with IPFS — no heph equivalent yet"]
    async fn test_upload_to_ipfs() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let data = b"hello aleph ipfs";

        let hash = client
            .upload_to_ipfs(data, None, false)
            .await
            .expect("upload should succeed");

        // Verify the hash is an IPFS CID, not a native hash
        assert!(matches!(hash, ItemHash::Ipfs(_)));

        // Verify the file is retrievable
        let size = client
            .get_file_size(&hash)
            .await
            .expect("file should exist");
        assert_eq!(size, Bytes::from(data.len() as u64));
    }

    mod submit_message_tests {
        use super::*;
        use aleph_types::address;
        use aleph_types::chain::{Chain, Signature};
        use aleph_types::item_hash::{AlephItemHash, ItemHash};
        use aleph_types::message::MessageType;
        use aleph_types::message::item_type::ItemType;
        use aleph_types::message::pending::PendingMessage;
        use aleph_types::timestamp::Timestamp;
        use std::future::Future;
        use std::sync::atomic::{AtomicBool, Ordering};

        const TEST_CONTENT: &str = r#"{"type":"test","address":"0xABCD","time":1234.0}"#;

        fn make_pending(item_type: ItemType, item_hash: ItemHash) -> PendingMessage {
            PendingMessage {
                chain: Chain::Ethereum,
                sender: address!("0xABCD"),
                signature: Signature::from("0xSIG".to_string()),
                message_type: MessageType::Post,
                item_type,
                item_content: TEST_CONTENT.to_string(),
                item_hash,
                time: Timestamp::from(1234.0),
                channel: None,
            }
        }

        struct MockClient {
            upload_storage_called: AtomicBool,
            upload_ipfs_called: AtomicBool,
            post_called: AtomicBool,
            /// Hash returned by upload_to_storage / upload_to_ipfs
            upload_hash: ItemHash,
        }

        impl MockClient {
            fn new(upload_hash: ItemHash) -> Self {
                Self {
                    upload_storage_called: AtomicBool::new(false),
                    upload_ipfs_called: AtomicBool::new(false),
                    post_called: AtomicBool::new(false),
                    upload_hash,
                }
            }
        }

        impl AlephMessageClient for MockClient {
            async fn get_message(
                &self,
                _item_hash: &ItemHash,
            ) -> Result<MessageWithStatus<Message>, MessageError> {
                unimplemented!()
            }

            async fn get_messages(
                &self,
                _filter: &MessageFilter,
                _pagination: PaginationParams,
            ) -> Result<Vec<Message>, MessageError> {
                unimplemented!()
            }

            fn get_messages_iterator(
                &self,
                _filter: MessageFilter,
                _pagination: Option<u32>,
            ) -> impl Stream<Item = Result<Message, MessageError>> + Send + '_ {
                futures_util::stream::empty()
            }

            async fn subscribe_to_messages(
                &self,
                _filter: &MessageFilter,
                _history: Option<u32>,
            ) -> Result<
                impl Stream<Item = Result<Message, MessageError>> + Send + Unpin,
                MessageError,
            > {
                Ok(tokio_stream::empty())
            }

            async fn post_message(
                &self,
                _message: &PendingMessage,
                _sync: bool,
            ) -> Result<PostMessageResponse, MessageError> {
                self.post_called.store(true, Ordering::SeqCst);

                Ok(PostMessageResponse {
                    publication_status: PublicationStatus {
                        status: "success".to_string(),
                        failed: vec![],
                    },
                    message_status: "processed".to_string(),
                })
            }

            async fn get_messages_and_verify(
                &self,
                _filter: &MessageFilter,
            ) -> Result<Vec<MessageVerification>, MessageError>
            where
                Self: AlephStorageClient + Sync,
            {
                unimplemented!()
            }
        }

        impl AlephStorageClient for MockClient {
            async fn get_file_size(&self, _file_hash: &ItemHash) -> Result<Bytes, MessageError> {
                unimplemented!()
            }

            async fn get_file_metadata_by_message_hash(
                &self,
                _message_hash: &ItemHash,
            ) -> Result<FileMetadata, MessageError> {
                unimplemented!()
            }

            async fn get_file_metadata_by_ref(
                &self,
                _file_ref: &FileRef,
            ) -> Result<FileMetadata, MessageError> {
                unimplemented!()
            }

            async fn download_file_by_hash(
                &self,
                _file_hash: &ItemHash,
            ) -> Result<FileDownload, MessageError> {
                unimplemented!()
            }

            fn upload_to_storage(
                &self,
                _data: &[u8],
                _message: Option<&PendingMessage>,
                _sync: bool,
            ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send {
                self.upload_storage_called.store(true, Ordering::SeqCst);
                let hash = self.upload_hash.clone();
                async move { Ok(hash) }
            }

            fn upload_to_ipfs(
                &self,
                _data: &[u8],
                _message: Option<&PendingMessage>,
                _sync: bool,
            ) -> impl Future<Output = Result<ItemHash, StorageError>> + Send {
                self.upload_ipfs_called.store(true, Ordering::SeqCst);
                let hash = self.upload_hash.clone();
                async move { Ok(hash) }
            }

            async fn upload_file_to_storage(
                &self,
                _path: impl AsRef<std::path::Path> + Send,
                _message: Option<&PendingMessage>,
                _sync: bool,
            ) -> Result<ItemHash, StorageError> {
                unimplemented!()
            }

            async fn upload_file_to_ipfs(
                &self,
                _path: impl AsRef<std::path::Path> + Send,
                _message: Option<&PendingMessage>,
                _sync: bool,
            ) -> Result<ItemHash, StorageError> {
                unimplemented!()
            }
        }

        #[tokio::test]
        async fn submit_inline_skips_upload() {
            let inline_hash = ItemHash::from(AlephItemHash::from_bytes(TEST_CONTENT.as_bytes()));
            let client = MockClient::new(inline_hash.clone());
            let msg = make_pending(ItemType::Inline, inline_hash);

            let resp = client.submit_message(&msg, false).await.unwrap();
            assert_eq!(resp.message_status, "processed");
            assert!(client.post_called.load(Ordering::SeqCst));
            assert!(!client.upload_storage_called.load(Ordering::SeqCst));
            assert!(!client.upload_ipfs_called.load(Ordering::SeqCst));
        }

        #[tokio::test]
        async fn submit_storage_uploads_then_posts() {
            let storage_hash = ItemHash::from(AlephItemHash::from_bytes(TEST_CONTENT.as_bytes()));
            let client = MockClient::new(storage_hash.clone());
            let msg = make_pending(ItemType::Storage, storage_hash);

            let resp = client.submit_message(&msg, false).await.unwrap();
            assert_eq!(resp.message_status, "processed");
            assert!(client.upload_storage_called.load(Ordering::SeqCst));
            assert!(client.post_called.load(Ordering::SeqCst));
        }

        #[tokio::test]
        async fn submit_ipfs_uploads_then_posts() {
            let cid = crate::verify::compute_cid(TEST_CONTENT.as_bytes());
            let ipfs_hash = ItemHash::Ipfs(cid);
            let client = MockClient::new(ipfs_hash.clone());
            let msg = make_pending(ItemType::Ipfs, ipfs_hash);

            let resp = client.submit_message(&msg, false).await.unwrap();
            assert_eq!(resp.message_status, "processed");
            assert!(client.upload_ipfs_called.load(Ordering::SeqCst));
            assert!(client.post_called.load(Ordering::SeqCst));
        }

        #[tokio::test]
        async fn submit_storage_hash_mismatch_returns_error() {
            let expected_hash = ItemHash::from(AlephItemHash::from_bytes(TEST_CONTENT.as_bytes()));
            // Mock returns a different hash
            let wrong_hash = ItemHash::from(AlephItemHash::from_bytes(b"wrong content"));
            let client = MockClient::new(wrong_hash.clone());
            let msg = make_pending(ItemType::Storage, expected_hash.clone());

            let err = client.submit_message(&msg, false).await.unwrap_err();
            match err {
                MessageError::HashMismatch { expected, actual } => {
                    assert_eq!(expected, expected_hash);
                    assert_eq!(actual, wrong_hash);
                }
                other => panic!("expected HashMismatch, got: {other:?}"),
            }
            assert!(!client.post_called.load(Ordering::SeqCst));
        }
    }

    mod serialize_storage_metadata_tests {
        use super::*;
        use crate::messages::StoreBuilder;
        use aleph_types::account::{Account, SignError};
        use aleph_types::chain::{Address, Chain, Signature};
        use aleph_types::message::StorageEngine;

        struct TestAccount {
            address: Address,
        }

        impl TestAccount {
            fn new() -> Self {
                Self {
                    address: Address::from(
                        "0xB68B9D4f3771c246233823ed1D3Add451055F9Ef".to_string(),
                    ),
                }
            }
        }

        impl Account for TestAccount {
            fn chain(&self) -> Chain {
                Chain::Ethereum
            }
            fn address(&self) -> &Address {
                &self.address
            }
            fn sign_raw(&self, _buffer: &[u8]) -> Result<Signature, SignError> {
                Ok(Signature::from("0xDUMMY".to_string()))
            }
        }

        #[test]
        fn serialize_storage_metadata_emits_message_and_sync() {
            let account = TestAccount::new();
            let hash = aleph_types::item_hash!("QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8");
            let msg = StoreBuilder::new(&account, hash, StorageEngine::Ipfs)
                .build()
                .unwrap();

            let json_str = serialize_storage_metadata(&msg, true);
            let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();
            assert_eq!(v["sync"], serde_json::Value::Bool(true));
            assert!(v["message"].is_object(), "message should be an object");
            assert_eq!(v["message"]["type"], "STORE");
        }

        #[test]
        fn serialize_storage_metadata_respects_sync_false() {
            let account = TestAccount::new();
            let hash = aleph_types::item_hash!("QmYULJoNGPDmoRq4WNWTDTUvJGJv1hosox8H6vVd1kCsY8");
            let msg = StoreBuilder::new(&account, hash, StorageEngine::Ipfs)
                .build()
                .unwrap();
            let json_str = serialize_storage_metadata(&msg, false);
            let v: serde_json::Value = serde_json::from_str(&json_str).unwrap();
            assert_eq!(v["sync"], serde_json::Value::Bool(false));
        }
    }

    mod classify_status_and_body_tests {
        use super::*;

        #[test]
        fn classify_status_402_insufficient_balance() {
            let err = classify_status_and_body(reqwest::StatusCode::PAYMENT_REQUIRED, "");
            assert!(matches!(err, Some(StorageError::InsufficientBalance)));
        }

        #[test]
        fn classify_status_403_ipfs_disabled() {
            let err = classify_status_and_body(
                reqwest::StatusCode::FORBIDDEN,
                "403: IPFS is disabled on this node",
            );
            assert!(matches!(err, Some(StorageError::IpfsDisabled)));
        }

        #[test]
        fn classify_status_403_invalid_signature_when_body_does_not_match() {
            let err = classify_status_and_body(reqwest::StatusCode::FORBIDDEN, "");
            assert!(matches!(err, Some(StorageError::InvalidSignature)));
            let err = classify_status_and_body(reqwest::StatusCode::FORBIDDEN, "Invalid signature");
            assert!(matches!(err, Some(StorageError::InvalidSignature)));
        }

        #[test]
        fn classify_status_403_partial_substring_does_not_match_ipfs_disabled() {
            // Adversarial / proxy-injected body that quotes the fragment but is not
            // the canonical pyaleph "IPFS is disabled on this node" reason — must
            // fall through to InvalidSignature, not IpfsDisabled.
            let err = classify_status_and_body(
                reqwest::StatusCode::FORBIDDEN,
                "User said: 'IPFS is disabled' is wrong",
            );
            assert!(matches!(err, Some(StorageError::InvalidSignature)));
        }

        #[test]
        fn classify_status_403_aiohttp_default_body_is_invalid_signature() {
            let err = classify_status_and_body(reqwest::StatusCode::FORBIDDEN, "403: Forbidden");
            assert!(matches!(err, Some(StorageError::InvalidSignature)));
        }

        #[test]
        fn classify_status_413_file_too_large() {
            let err = classify_status_and_body(reqwest::StatusCode::PAYLOAD_TOO_LARGE, "anything");
            assert!(matches!(err, Some(StorageError::FileTooLarge)));
        }

        #[test]
        fn classify_status_422_invalid_metadata_carries_body() {
            let body = "File hash does not match (X != Y)";
            let err = classify_status_and_body(reqwest::StatusCode::UNPROCESSABLE_ENTITY, body);
            match err {
                Some(StorageError::InvalidMetadata(s)) => assert_eq!(s, body),
                other => panic!("expected InvalidMetadata, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_422_car_header_root_mismatch() {
            let body = "Root CID does not match (bafy1 != bafy2)";
            let err = classify_status_and_body(reqwest::StatusCode::UNPROCESSABLE_ENTITY, body);
            match err {
                Some(StorageError::CarHeaderRootMismatch {
                    car_root,
                    metadata_root,
                }) => {
                    assert_eq!(car_root, "bafy1");
                    assert_eq!(metadata_root, "bafy2");
                }
                other => panic!("expected CarHeaderRootMismatch, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_422_imported_root_mismatch() {
            let body = "Imported root does not match expected (bafy3 != bafy4); CAR header declared a root that does not correspond to the imported DAG";
            let err = classify_status_and_body(reqwest::StatusCode::UNPROCESSABLE_ENTITY, body);
            match err {
                Some(StorageError::ImportedRootMismatch {
                    kubo_root,
                    expected_root,
                }) => {
                    assert_eq!(kubo_root, "bafy3");
                    assert_eq!(expected_root, "bafy4");
                }
                other => panic!("expected ImportedRootMismatch, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_422_unknown_body_falls_back_to_invalid_metadata() {
            let body = "some other 422 reason";
            let err = classify_status_and_body(reqwest::StatusCode::UNPROCESSABLE_ENTITY, body);
            match err {
                Some(StorageError::InvalidMetadata(s)) => assert_eq!(s, body),
                other => panic!("expected InvalidMetadata, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_502_bad_gateway_is_ipfs_backend_unavailable() {
            let err = classify_status_and_body(
                reqwest::StatusCode::BAD_GATEWAY,
                "Failed to import CAR into IPFS: kubo unreachable",
            );
            match err {
                Some(StorageError::IpfsBackendUnavailable(s)) => {
                    assert!(s.contains("kubo unreachable"));
                }
                other => panic!("expected IpfsBackendUnavailable, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_504_gateway_timeout_is_ipfs_backend_unavailable() {
            let err = classify_status_and_body(
                reqwest::StatusCode::GATEWAY_TIMEOUT,
                "Timed out waiting for IPFS stat",
            );
            match err {
                Some(StorageError::IpfsBackendUnavailable(s)) => {
                    assert!(s.contains("Timed out"));
                }
                other => panic!("expected IpfsBackendUnavailable, got {other:?}"),
            }
        }

        #[test]
        fn classify_status_returns_none_for_success_and_unknown() {
            assert!(classify_status_and_body(reqwest::StatusCode::OK, "").is_none());
            assert!(
                classify_status_and_body(reqwest::StatusCode::INTERNAL_SERVER_ERROR, "").is_none()
            );
        }
    }

    mod aggregate_helper_tests {
        use super::*;
        use crate::aggregate_models::domains::{
            DOMAINS_AGGREGATE_KEY, DomainTargetType, DomainsAggregate,
        };
        use crate::aggregate_models::websites::{WEBSITES_AGGREGATE_KEY, WebsitesAggregate};
        use std::future::Future;

        // --- extract_aggregate_value: pure JSON-descent logic ---

        #[test]
        fn extract_returns_empty_when_data_is_null() {
            // CCN responded `{"data": null}` -> get_aggregate yields None.
            let agg: WebsitesAggregate = extract_aggregate_value(None, WEBSITES_AGGREGATE_KEY)
                .expect("null data should map to empty aggregate");
            assert!(agg.is_empty());
        }

        #[test]
        fn extract_returns_empty_when_inner_object_lacks_key() {
            // CCN responded `{"data": {}}` (other keys but not ours).
            let raw = serde_json::json!({"someOtherKey": {}});
            let agg: WebsitesAggregate =
                extract_aggregate_value(Some(raw), WEBSITES_AGGREGATE_KEY).unwrap();
            assert!(agg.is_empty());
        }

        #[test]
        fn extract_returns_empty_when_inner_key_is_null() {
            // CCN responded `{"data": {"websites": null}}`.
            let raw = serde_json::json!({ WEBSITES_AGGREGATE_KEY: serde_json::Value::Null });
            let agg: WebsitesAggregate =
                extract_aggregate_value(Some(raw), WEBSITES_AGGREGATE_KEY).unwrap();
            assert!(agg.is_empty());
        }

        #[test]
        fn extract_returns_empty_on_malformed_envelope() {
            let bogus = Some(serde_json::Value::String("nope".into()));
            let result: Result<WebsitesAggregate, _> =
                extract_aggregate_value(bogus, WEBSITES_AGGREGATE_KEY);
            assert!(result.unwrap().is_empty());
        }

        #[test]
        fn extract_parses_real_websites_shape() {
            let raw = serde_json::json!({
                "websites": {
                    "my-site": {
                        "metadata": { "name": "my-site", "tags": [], "framework": "nextjs" },
                        "payment": { "chain": "ETH", "type": "hold" },
                        "version": 2,
                        "volume_id": "vol_abc",
                        "history": {},
                        "ens": [],
                        "created_at": 1.0,
                        "updated_at": 2.0
                    },
                    "deleted-site": null
                }
            });
            let agg: WebsitesAggregate =
                extract_aggregate_value(Some(raw), WEBSITES_AGGREGATE_KEY).unwrap();
            let entry = agg.get("my-site").unwrap().as_ref().unwrap();
            assert_eq!(entry.version, 2);
            assert_eq!(entry.volume_id, "vol_abc");
            assert!(agg.get("deleted-site").unwrap().is_none());
        }

        #[test]
        fn extract_parses_real_domains_shape() {
            let raw = serde_json::json!({
                "domains": {
                    "site.example.com": {
                        "type": "ipfs",
                        "programType": "ipfs",
                        "message_id": "vol1",
                        "updated_at": 1.0
                    }
                }
            });
            let agg: DomainsAggregate =
                extract_aggregate_value(Some(raw), DOMAINS_AGGREGATE_KEY).unwrap();
            let entry = agg.get("site.example.com").unwrap().as_ref().unwrap();
            assert_eq!(entry.kind, DomainTargetType::Ipfs);
            assert_eq!(entry.message_id, "vol1");
        }

        #[test]
        fn extract_propagates_invalid_aggregate_as_api_error() {
            // Inner key is present but the shape can't be deserialized.
            let raw = serde_json::json!({
                "websites": "not-an-object"
            });
            let err =
                extract_aggregate_value::<WebsitesAggregate>(Some(raw), WEBSITES_AGGREGATE_KEY)
                    .expect_err("non-object inner should fail to deserialize");
            match err {
                MessageError::ApiError { status, body } => {
                    assert_eq!(status, 200);
                    assert!(
                        body.contains("invalid websites aggregate"),
                        "body was: {body}"
                    );
                }
                other => panic!("expected ApiError, got: {other:?}"),
            }
        }

        // --- AlephAggregateClient mock: returns Ok(empty) by default ---

        struct MockAggregateClient;

        impl AlephAggregateClient for MockAggregateClient {
            #[allow(clippy::manual_async_fn)]
            fn get_aggregate<T: DeserializeOwned>(
                &self,
                _address: &Address,
                _key: &str,
            ) -> impl Future<Output = Result<T, MessageError>> + Send {
                // Not used by these tests; keeps the trait satisfied.
                // Written as `fn -> impl Future` to mirror the trait declaration —
                // `async fn` would silently drop the `+ Send` bound.
                async { unimplemented!("MockAggregateClient::get_aggregate") }
            }

            async fn get_websites_aggregate(
                &self,
                _address: &Address,
            ) -> Result<WebsitesAggregate, MessageError> {
                Ok(WebsitesAggregate::new())
            }

            async fn get_domains_aggregate(
                &self,
                _address: &Address,
            ) -> Result<DomainsAggregate, MessageError> {
                Ok(DomainsAggregate::new())
            }

            async fn get_port_forwarding_aggregate(
                &self,
                _address: &Address,
            ) -> Result<PortForwardingAggregate, MessageError> {
                Ok(PortForwardingAggregate::new())
            }

            async fn get_aggregates(
                &self,
                _address: &Address,
                _keys: &[&str],
            ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
                Ok(HashMap::new())
            }

            async fn get_all_aggregates(
                &self,
                _address: &Address,
            ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
                Ok(HashMap::new())
            }
        }

        #[tokio::test]
        async fn mock_returns_empty_websites_aggregate_by_default() {
            let client = MockAggregateClient;
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");
            let agg = client.get_websites_aggregate(&addr).await.unwrap();
            assert!(agg.is_empty());
        }

        #[tokio::test]
        async fn mock_returns_empty_domains_aggregate_by_default() {
            let client = MockAggregateClient;
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");
            let agg = client.get_domains_aggregate(&addr).await.unwrap();
            assert!(agg.is_empty());
        }

        #[tokio::test]
        async fn mock_returns_empty_port_forwarding_aggregate_by_default() {
            let client = MockAggregateClient;
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");
            let agg = client.get_port_forwarding_aggregate(&addr).await.unwrap();
            assert!(agg.is_empty());
        }

        // --- map_aggregate_404_to_empty: 404 -> empty, others propagated ---

        /// Spin up a TCP listener that responds to every request with the given
        /// hand-rolled HTTP/1.1 response. Each accepted connection serves one
        /// response and closes.
        async fn start_canned_response_server(response: &'static [u8]) -> Url {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = listener.accept().await else {
                        break;
                    };
                    tokio::spawn(async move {
                        // Drain request headers so close() is graceful on
                        // Windows (otherwise the kernel sends RST instead of
                        // FIN and the client never parses our response).
                        let mut buf = [0u8; 1024];
                        let mut req = Vec::with_capacity(1024);
                        loop {
                            match stream.read(&mut buf).await {
                                Ok(0) => break,
                                Ok(n) => {
                                    req.extend_from_slice(&buf[..n]);
                                    if req.windows(4).any(|w| w == b"\r\n\r\n") {
                                        break;
                                    }
                                }
                                Err(_) => break,
                            }
                        }
                        let _ = stream.write_all(response).await;
                        let _ = stream.shutdown().await;
                    });
                }
            });
            Url::parse(&format!("http://{addr}")).unwrap()
        }

        const HTTP_404_RESPONSE: &[u8] = b"HTTP/1.1 404 Not Found\r\n\
            Content-Type: text/plain\r\n\
            Content-Length: 9\r\n\
            Connection: close\r\n\
            \r\n\
            not found";

        #[tokio::test]
        async fn get_websites_aggregate_returns_empty_on_404() {
            let url = start_canned_response_server(HTTP_404_RESPONSE).await;
            let client = AlephClient::builder(url)
                .retry_config(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build();
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

            let agg = client
                .get_websites_aggregate(&addr)
                .await
                .expect("404 should map to an empty aggregate, not an error");
            assert!(agg.is_empty());
        }

        #[tokio::test]
        async fn get_domains_aggregate_returns_empty_on_404() {
            let url = start_canned_response_server(HTTP_404_RESPONSE).await;
            let client = AlephClient::builder(url)
                .retry_config(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build();
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

            let agg = client
                .get_domains_aggregate(&addr)
                .await
                .expect("404 should map to an empty aggregate, not an error");
            assert!(agg.is_empty());
        }

        #[tokio::test]
        async fn get_port_forwarding_aggregate_returns_empty_on_404() {
            let url = start_canned_response_server(HTTP_404_RESPONSE).await;
            let client = AlephClient::builder(url)
                .retry_config(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build();
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

            let agg = client
                .get_port_forwarding_aggregate(&addr)
                .await
                .expect("404 should map to an empty aggregate, not an error");
            assert!(agg.is_empty());
        }

        #[tokio::test]
        async fn map_aggregate_404_to_empty_propagates_non_404_errors() {
            // A 500 should NOT be swallowed — only 404 is special-cased.
            const HTTP_500_RESPONSE: &[u8] = b"HTTP/1.1 500 Internal Server Error\r\n\
                Content-Type: text/plain\r\n\
                Content-Length: 5\r\n\
                Connection: close\r\n\
                \r\n\
                boom!";

            let url = start_canned_response_server(HTTP_500_RESPONSE).await;
            let client = AlephClient::builder(url)
                .retry_config(RetryConfig {
                    max_retries: 0,
                    ..Default::default()
                })
                .build();
            let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

            let err = client
                .get_websites_aggregate(&addr)
                .await
                .expect_err("5xx should propagate, not be swallowed as empty");
            // The exact variant doesn't matter — what matters is that we did NOT
            // get an Ok(empty) result for a non-404 transport failure.
            assert!(matches!(err, MessageError::HttpError(_)), "got: {err:?}");
        }
    }
}

#[cfg(test)]
mod ipfs_gateway_tests {
    use super::*;

    #[test]
    fn default_client_uses_default_ipfs_gateway() {
        let client = AlephClient::new(Url::parse("https://example.com").unwrap());
        // Url normalizes to include a trailing slash for the empty-path root.
        assert_eq!(client.ipfs_gateway.as_str(), "https://ipfs.aleph.cloud/");
    }

    #[test]
    fn with_ipfs_gateway_overrides() {
        let client = AlephClient::new(Url::parse("https://example.com").unwrap())
            .with_ipfs_gateway(Url::parse("http://localhost:5001").unwrap());
        assert_eq!(client.ipfs_gateway.as_str(), "http://localhost:5001/");
    }
}

#[cfg(test)]
mod credit_history_serde_tests {
    use super::*;

    #[test]
    fn item_handles_missing_and_null_optionals() {
        // Source-of-truth row with every optional field populated.
        let purchase = serde_json::json!({
            "amount": 1_000_000,
            "price": "0.000001",
            "bonus_amount": 50_000,
            "tx_hash": "0xdeadbeef",
            "token": "USDC",
            "chain": "ETH",
            "provider": "stripe",
            "origin": "web",
            "origin_ref": "ord_42",
            "payment_method": "card",
            "credit_ref": "purchase:0xdeadbeef:0",
            "credit_index": 0,
            "expiration_date": "2027-01-01T00:00:00Z",
            "message_timestamp": "2026-05-01T12:00:00Z",
        });
        let _: CreditHistoryItem = serde_json::from_value(purchase).unwrap();

        // Optional fields explicitly null (the shape the reviewer flagged).
        let null_optionals = serde_json::json!({
            "amount": -500,
            "price": null,
            "bonus_amount": null,
            "tx_hash": null,
            "token": null,
            "chain": null,
            "provider": null,
            "origin": null,
            "origin_ref": null,
            "payment_method": null,
            "credit_ref": "transfer:abc:0",
            "credit_index": 0,
            "expiration_date": null,
            "message_timestamp": "2026-05-01T12:00:00Z",
        });
        let item: CreditHistoryItem = serde_json::from_value(null_optionals).unwrap();
        assert!(item.expiration_date.is_none());
        assert!(item.payment_method.is_none());
        assert!(item.price.is_none());

        // Optional fields entirely missing from the payload.
        let missing_optionals = serde_json::json!({
            "amount": 1,
            "credit_ref": "x:0",
            "credit_index": 1,
            "message_timestamp": "2026-05-01T12:00:00Z",
        });
        let item: CreditHistoryItem = serde_json::from_value(missing_optionals).unwrap();
        assert!(item.expiration_date.is_none());
        assert!(item.payment_method.is_none());
    }

    #[test]
    fn credit_direction_wire_values() {
        assert_eq!(CreditDirection::Incoming.as_wire(), "incoming");
        assert_eq!(CreditDirection::Outgoing.as_wire(), "outgoing");
    }

    #[test]
    fn empty_filters_emit_no_query_params() {
        assert!(CreditHistoryFilters::default().query_params().is_empty());
    }

    #[test]
    fn filters_render_camelcase_query_params() {
        let filters = CreditHistoryFilters {
            start_date: Some(1_769_990_400),
            end_date: Some(1_770_000_000),
            direction: Some(CreditDirection::Outgoing),
            resource_types: vec![MessageType::Store, MessageType::Program],
        };
        let params = filters.query_params();
        assert_eq!(
            params,
            vec![
                ("startDate", "1769990400".to_string()),
                ("endDate", "1770000000".to_string()),
                ("direction", "outgoing".to_string()),
                ("resourceTypes", "STORE,PROGRAM".to_string()),
            ]
        );
    }

    #[test]
    fn summary_deserializes() {
        let body = serde_json::json!({
            "address": "0xabc",
            "entry_count": 42,
            "total_amount": -1_250_000,
            "total_incoming": 3_000_000,
            "total_outgoing": -4_250_000,
        });
        let summary: CreditHistorySummary = serde_json::from_value(body).unwrap();
        assert_eq!(summary.entry_count, 42);
        assert_eq!(summary.total_amount, -1_250_000);
        assert_eq!(summary.total_incoming, 3_000_000);
        assert_eq!(summary.total_outgoing, -4_250_000);
    }
}

#[cfg(test)]
mod credit_history_summary_tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn summary_sends_filters_and_parses_totals() {
        let server = MockServer::start().await;
        let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

        Mock::given(method("GET"))
            .and(path(format!(
                "/api/v0/addresses/{addr}/credit_history/summary"
            )))
            .and(query_param("startDate", "1769990400"))
            .and(query_param("direction", "outgoing"))
            .and(query_param("resourceTypes", "STORE,PROGRAM"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "address": addr.to_string(),
                "entry_count": 3,
                "total_amount": -900,
                "total_incoming": 0,
                "total_outgoing": -900,
            })))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let filters = CreditHistoryFilters {
            start_date: Some(1_769_990_400),
            end_date: None,
            direction: Some(CreditDirection::Outgoing),
            resource_types: vec![MessageType::Store, MessageType::Program],
        };
        let summary = client
            .get_credit_history_summary(&addr, &filters)
            .await
            .unwrap();
        assert_eq!(summary.entry_count, 3);
        assert_eq!(summary.total_outgoing, -900);
    }

    #[tokio::test]
    async fn summary_404_is_an_error_not_silent_zeros() {
        // An old CCN without the summary route 404s. We must not report that
        // as zero spend; it has to surface as an error so the caller knows the
        // server is unsupported.
        let server = MockServer::start().await;
        let addr = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");

        Mock::given(method("GET"))
            .and(path(format!(
                "/api/v0/addresses/{addr}/credit_history/summary"
            )))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let err = client
            .get_credit_history_summary(&addr, &CreditHistoryFilters::default())
            .await
            .expect_err("404 must be an error, not zeroed totals");
        match err {
            MessageError::ApiError { status, body } => {
                assert_eq!(status, 404);
                assert!(body.contains("summary endpoint not found"), "got: {body}");
            }
            other => panic!("expected ApiError, got: {other:?}"),
        }
    }
}

#[cfg(test)]
mod account_files_tests {
    use super::*;
    use futures_util::TryStreamExt;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn account_file_deserializes_cursor_mode() {
        // Cursor-mode rows ship `created` as a unix-epoch float.
        let body = json!({
            "file_hash": "Qmabc",
            "size": 1234,
            "type": "file",
            "created": 1779373210.7274384,
            "item_hash": "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c"
        });
        let f: AccountFile = serde_json::from_value(body).unwrap();
        assert_eq!(f.file_hash, "Qmabc");
        assert_eq!(f.size.count(), 1234);
        assert_eq!(f.storage_engine, "file");
        assert!((f.created.as_f64() - 1779373210.7274384).abs() < 1e-6);
    }

    #[test]
    fn cursor_response_accepts_float_total_size() {
        // pyaleph emits total_size as a float in cursor mode.
        let body = json!({
            "files": [],
            "total_size": 78051738.0,
            "next_cursor": null,
        });
        let r: AccountFilesCursorResponse = serde_json::from_value(body).unwrap();
        assert_eq!(r.total_size_bytes().count(), 78_051_738);
    }

    #[tokio::test]
    async fn iterator_walks_cursor_until_exhausted() {
        let server = MockServer::start().await;
        let address = "0x0B8ee617A08AC051a8A3b430ACf7233a462A0187";

        // First page: 2 rows + next_cursor=ABC
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/addresses/{address}/files")))
            .and(query_param("cursor", ""))
            .and(query_param("pagination", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "files": [
                    {"file_hash": "Qm1", "size": 100, "type": "file",
                     "created": 1.0,
                     "item_hash": "0000000000000000000000000000000000000000000000000000000000000001"},
                    {"file_hash": "Qm2", "size": 200, "type": "file",
                     "created": 2.0,
                     "item_hash": "0000000000000000000000000000000000000000000000000000000000000002"},
                ],
                "total_size": 600.0,
                "next_cursor": "ABC",
            })))
            .mount(&server)
            .await;

        // Second page: 1 row, no next_cursor.
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/addresses/{address}/files")))
            .and(query_param("cursor", "ABC"))
            .and(query_param("pagination", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "files": [
                    {"file_hash": "Qm3", "size": 300, "type": "file",
                     "created": 3.0,
                     "item_hash": "0000000000000000000000000000000000000000000000000000000000000003"},
                ],
                "total_size": 600.0,
                "next_cursor": null,
            })))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from(address.to_string());
        let files: Vec<AccountFile> = client
            .get_account_files_iterator(&addr, Some(2), None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].file_hash, "Qm1");
        assert_eq!(files[2].file_hash, "Qm3");
    }

    #[tokio::test]
    async fn iterator_returns_empty_stream_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v0/addresses/0x0000000000000000000000000000000000000001/files",
            ))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0x0000000000000000000000000000000000000001".to_string());
        let files: Vec<AccountFile> = client
            .get_account_files_iterator(&addr, Some(25), None)
            .try_collect()
            .await
            .unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn get_total_storage_size_reads_float_total_via_cursor() {
        let server = MockServer::start().await;
        let address = "0x0B8ee617A08AC051a8A3b430ACf7233a462A0187";
        Mock::given(method("GET"))
            .and(path(format!("/api/v0/addresses/{address}/files")))
            .and(query_param("cursor", ""))
            .and(query_param("pagination", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "files": [],
                "total_size": 78_051_738.0,
                "next_cursor": null,
            })))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from(address.to_string());
        let total = client.get_total_storage_size(&addr).await.unwrap();
        assert_eq!(total.count(), 78_051_738);
    }

    #[tokio::test]
    async fn get_total_storage_size_returns_zero_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/v0/addresses/0x0000000000000000000000000000000000000001/files",
            ))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = AlephClient::new(Url::parse(&server.uri()).unwrap());
        let addr = Address::from("0x0000000000000000000000000000000000000001".to_string());
        assert_eq!(
            client.get_total_storage_size(&addr).await.unwrap().count(),
            0
        );
    }

    #[test]
    fn rejected_message_deserializes_item_type_and_item_content_inline() {
        let body = serde_json::json!({
            "sender": "0xABCD",
            "chain": "ETH",
            "signature": "0xSIG",
            "type": "POST",
            "item_type": "inline",
            "item_content": "{\"type\":\"test\"}",
            "item_hash": "0".repeat(64),
            "time": 1234.0,
            "channel": "TEST",
            "content": null,
        });
        let rejected: RejectedMessage = serde_json::from_value(body).unwrap();
        assert_eq!(rejected.item_type, ItemType::Inline);
        assert_eq!(
            rejected.item_content.as_deref(),
            Some("{\"type\":\"test\"}")
        );
    }

    #[test]
    fn rejected_message_deserializes_storage_with_null_item_content() {
        let body = serde_json::json!({
            "sender": "0xABCD",
            "chain": "ETH",
            "signature": "0xSIG",
            "type": "STORE",
            "item_type": "storage",
            "item_content": null,
            "item_hash": "0".repeat(64),
            "time": 1234.0,
            "channel": null,
            "content": null,
        });
        let rejected: RejectedMessage = serde_json::from_value(body).unwrap();
        assert_eq!(rejected.item_type, ItemType::Storage);
        assert!(rejected.item_content.is_none());
    }
}
