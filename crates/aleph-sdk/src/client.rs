use crate::aggregate_models::corechannel::CoreChannelAggregate;
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::pending::PendingMessage;
use aleph_types::message::{
    ContentSource, FileRef, Message, MessageConfirmation, MessageContent, MessageContentEnum,
    MessageHeader, MessageStatus, MessageType, RawFileRef, SignatureVerificationError,
};
use aleph_types::timestamp::Timestamp;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt, TryFutureExt, TryStreamExt};
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
use tokio::io::AsyncWriteExt;
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

#[derive(Clone)]
pub struct AlephClient {
    http_client: ClientWithMiddleware,
    ccn_url: Url,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("File not found: {0}")]
    NotFound(ItemHash),
    #[error("File reference not found: {0}")]
    RefNotFound(FileRef),
    #[error("Failed to read file size: {0}")]
    InvalidSize(String),
    #[error("Integrity verification failed: {0}")]
    IntegrityError(#[from] crate::verify::VerifyError),
    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),
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
    #[error(transparent)]
    HttpError(#[from] reqwest_middleware::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WebSocket connection error: {0}")]
    WebsocketConnection(String),
    #[error("WebSocket message error: {0}")]
    WebsocketMessage(String),
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
    #[error("Failed to deserialize verified content: {0}")]
    ContentDeserializationFailed(String),

    /// The cryptographic signature does not match the message sender.
    #[error("Signature verification failed: {0}")]
    SignatureVerificationFailed(#[from] SignatureVerificationError),
}

/// A message that passed verification.
///
/// Simple wrapper around Message. This type forces callers to verify the integrity of the message
/// before calling other functions.
#[derive(Debug)]
pub struct VerifiedMessage {
    /// The message
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

/// Internal error type for `verify_message_header` that distinguishes fetch failures
/// (which should abort the batch) from integrity failures (which are per-message).
#[derive(Debug)]
enum VerifyMessageError {
    /// The data could not be fetched (network error, 404, etc.).
    Fetch(MessageError),
    /// The data was fetched but failed integrity checks.
    Integrity(Box<InvalidMessage>),
}

/// Assembles a [`VerifiedMessage`] from a header and deserialization result,
/// mapping deserialization failures to integrity errors.
fn build_verified(
    header: MessageHeader,
    content: Result<MessageContent, serde_json::Error>,
) -> Result<VerifiedMessage, VerifyMessageError> {
    match content {
        Ok(content) => Ok(VerifiedMessage {
            message: header.with_content(content),
        }),
        Err(e) => Err(VerifyMessageError::Integrity(Box::new(InvalidMessage {
            header,
            error: IntegrityError::ContentDeserializationFailed(e.to_string()),
        }))),
    }
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
#[derive(Debug, Serialize, Deserialize)]
pub struct RawPendingMessage {
    pub sender: Address,
    pub chain: Chain,
    pub signature: Option<Signature>,
    pub content_source: ContentSource,
    pub message_type: MessageType,
    pub item_hash: ItemHash,
    pub time: Timestamp,
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
pub enum SortBy {
    Time,
    TxTime,
}

#[derive(Debug, Copy, Clone, Serialize)]
pub enum SortOrder {
    Asc,
    Desc,
}
impl std::fmt::Display for SortOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            SortOrder::Asc => "asc",
            SortOrder::Desc => "desc",
        })
    }
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

    pub pagination: Option<u32>,
    pub page: Option<u32>,
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

    /// Maximum number of posts to return per page.
    pub pagination: Option<u32>,

    /// Page number (starts at 1).
    pub page: Option<u32>,

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

#[derive(Debug, Deserialize)]
pub struct PublicationStatus {
    pub status: String,
    pub failed: Vec<String>,
}

#[derive(Debug, Deserialize)]
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
    ) -> impl Future<Output = Result<Vec<Message>, MessageError>> + Send;

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

    /// Verifies raw content and builds a [`VerifiedMessage`] from a [`MessageHeader`].
    ///
    /// For inline messages, verification is done locally by hashing the `item_content` string.
    /// For non-inline messages (storage/ipfs), the raw content is downloaded from
    /// `/api/v0/storage/raw/{item_hash}` and its hash is verified.
    ///
    /// The message content is always deserialized from the verified raw bytes, never from
    /// the CCN's pre-deserialized `content` field.
    fn verify_message_header(
        &self,
        header: MessageHeader,
    ) -> impl Future<Output = Result<VerifiedMessage, VerifyMessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        async {
            // Verify signature first — it's cheap (no I/O) and catches forgeries
            // before we spend time downloading or hashing content.
            if let Err(e) = header.verify_signature() {
                return Err(VerifyMessageError::Integrity(Box::new(InvalidMessage {
                    header,
                    error: IntegrityError::SignatureVerificationFailed(e),
                })));
            }

            match &header.content_source {
                ContentSource::Inline { item_content } => {
                    if let Some(Err((expected, actual))) =
                        header.content_source.verify_inline_hash(&header.item_hash)
                    {
                        return Err(VerifyMessageError::Integrity(Box::new(InvalidMessage {
                            header,
                            error: IntegrityError::HashMismatch { expected, actual },
                        })));
                    }
                    // NLL: the borrow of item_content ends after this call since
                    // the returned MessageContent owns its data.
                    let content = MessageContent::deserialize_with_type(
                        header.message_type,
                        item_content.as_bytes(),
                    );
                    build_verified(header, content)
                }
                ContentSource::Storage | ContentSource::Ipfs => {
                    let download = self
                        .download_file_by_hash(&header.item_hash)
                        .await
                        .map_err(VerifyMessageError::Fetch)?;
                    let raw_bytes = match download.with_verification().bytes().await {
                        Ok(bytes) => bytes,
                        Err(MessageError::Storage(StorageError::IntegrityError(
                            crate::verify::VerifyError::IntegrityMismatch { expected, actual },
                        ))) => {
                            return Err(VerifyMessageError::Integrity(Box::new(InvalidMessage {
                                header,
                                error: IntegrityError::HashMismatch { expected, actual },
                            })));
                        }
                        Err(e) => return Err(VerifyMessageError::Fetch(e)),
                    };
                    let content =
                        MessageContent::deserialize_with_type(header.message_type, &raw_bytes);
                    build_verified(header, content)
                }
            }
        }
    }

    /// Verifies a fully-fetched [`Message`] by re-checking its raw content.
    ///
    /// Takes ownership of the message, verifies the raw content hash, and returns
    /// a [`VerifiedMessage`] whose content is deserialized from the verified raw bytes
    /// (discarding the original content from the CCN).
    fn verify_message(
        &self,
        message: Message,
    ) -> impl Future<Output = Result<VerifiedMessage, MessageError>> + Send
    where
        Self: AlephStorageClient + Sync,
    {
        self.verify_message_header(MessageHeader::from(message))
            .map_err(|e| match e {
                VerifyMessageError::Fetch(e) => e,
                VerifyMessageError::Integrity(invalid) => invalid.error.into(),
            })
    }

    /// Fetches a single message and verifies its integrity.
    ///
    /// Returns `Err(MessageError::Integrity(..))` if verification fails. Verification is only
    /// performed for statuses that carry a full [`Message`] (Processed, Removing, Removed);
    /// other statuses (Pending, Forgotten, Rejected) are returned as-is.
    ///
    /// The returned message's content is deserialized from verified raw bytes.
    fn get_message_and_verify(
        &self,
        item_hash: &ItemHash,
    ) -> impl Future<Output = Result<MessageWithStatus<VerifiedMessage>, MessageError>> + Send
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
    /// The outer `Result` fails on fetch errors (network, 404, etc.) — these abort the
    /// entire batch since they likely indicate a systemic issue. Per-message integrity
    /// failures (hash mismatch, deserialization errors) are returned as `Err(InvalidMessage)`
    /// in the inner `Result`, letting callers decide how to handle them.
    ///
    /// **Note:** Non-inline messages require a sequential HTTP round-trip each to
    /// `/storage/raw/{item_hash}`, so verifying a page of N non-inline messages incurs N
    /// additional requests.
    ///
    /// ```ignore
    /// let results = client.get_messages_and_verify(&filter).await?;
    ///
    /// // Collect only verified messages, logging integrity failures
    /// let messages: Vec<Message> = results
    ///     .into_iter()
    ///     .filter_map(|r| match r {
    ///         Ok(vm) => Some(vm.into()),
    ///         Err(invalid) => {
    ///             log::warn!("integrity check failed for {}: {}", invalid.header.item_hash, invalid.error);
    ///             None
    ///         }
    ///     })
    ///     .collect();
    /// ```
    fn get_messages_and_verify(
        &self,
        filter: &MessageFilter,
    ) -> impl Future<Output = Result<Vec<Result<VerifiedMessage, InvalidMessage>>, MessageError>> + Send
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

    /// Gets the price of a VM in Aleph tokens using the holder tier, i.e. the minimum amount
    /// of Aleph tokens that the user needs to hold in his account.
    fn get_vm_price(
        &self,
        item_hash: &ItemHash,
    ) -> impl Future<Output = Result<f64, MessageError>> + Send;
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
}

pub trait AlephPostClient {
    /// Queries posts matching the given filter using the v0 (legacy) format.
    ///
    /// The v0 format includes the full message envelope (chain, signature,
    /// confirmations, etc.). Returns the full response including pagination metadata.
    fn get_posts_v0(
        &self,
        filter: &PostFilter,
    ) -> impl Future<Output = Result<GetPostsV0Response, MessageError>> + Send;

    /// Queries posts matching the given filter using the v1 format.
    ///
    /// The v1 format is leaner: it omits message-level fields and uses ISO 8601
    /// timestamps (`created`, `last_updated`) instead of unix floats.
    fn get_posts_v1(
        &self,
        filter: &PostFilter,
    ) -> impl Future<Output = Result<GetPostsV1Response, MessageError>> + Send;
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

const DEFAULT_MAX_CONCURRENT_REQUESTS: usize = 16;

/// Builder for [`AlephClient`].
///
/// ```
/// # use url::Url;
/// # use aleph_sdk::client::{AlephClient, RetryConfig};
/// let client = AlephClient::builder(Url::parse("https://api3.aleph.im").unwrap())
///     .max_concurrent_requests(32)
///     .retry_config(RetryConfig { max_retries: 5, ..Default::default() })
///     .build();
/// ```
pub struct AlephClientBuilder {
    ccn_url: Url,
    retry_config: RetryConfig,
    max_concurrent_requests: usize,
}

impl AlephClientBuilder {
    pub fn retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
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

    pub fn build(self) -> AlephClient {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(self.retry_config.min_backoff, self.retry_config.max_backoff)
            .build_with_max_retries(self.retry_config.max_retries);

        let concurrency_limit = ConcurrencyLimit {
            semaphore: Arc::new(Semaphore::new(self.max_concurrent_requests)),
        };

        // Retry is the outer middleware: it decides whether to retry.
        // ConcurrencyLimit is the inner middleware: each attempt (including retries)
        // acquires a permit only for the duration of actual network I/O.
        let http_client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .with(concurrency_limit)
            .build();

        AlephClient {
            http_client,
            ccn_url: self.ccn_url,
        }
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
            max_concurrent_requests: DEFAULT_MAX_CONCURRENT_REQUESTS,
        }
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

    async fn get_messages(&self, filter: &MessageFilter) -> Result<Vec<Message>, MessageError> {
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

        let get_messages_response: GetMessagesResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(get_messages_response.messages)
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

        let response = self
            .http_client
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(response)
    }

    async fn get_messages_and_verify(
        &self,
        filter: &MessageFilter,
    ) -> Result<Vec<Result<VerifiedMessage, InvalidMessage>>, MessageError> {
        let headers = self.get_message_headers(filter).await?;

        let verify_futures = headers.into_iter().map(|header| async {
            match self.verify_message_header(header).await {
                Ok(verified) => Ok(Ok(verified)),
                Err(VerifyMessageError::Integrity(invalid)) => Ok(Err(*invalid)),
                Err(VerifyMessageError::Fetch(e)) => Err(e),
            }
        });

        // All verifications run concurrently. Inline messages complete instantly (no I/O);
        // non-inline downloads are gated by the ConcurrencyLimit middleware.
        futures_util::stream::iter(verify_futures)
            .buffer_unordered(usize::MAX)
            .try_collect()
            .await
    }
}

impl AlephClient {
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
                    .map_err(|_| StorageError::InvalidSize(s.to_string()))
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

#[derive(Debug, Deserialize)]
struct GetAccountFilesResponse {
    // We purposefully ignore the files themselves at the moment as the only feature of the client
    // at this moment is to retrieve the total size, not the files themselves.
    total_size: Bytes,
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
        let url = self
            .ccn_url
            .join(&format!("/api/v0/addresses/{}/files", address))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?;
        // The endpoint returns a 404 if the address has no files.
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(Bytes::from(0));
        }
        // Otherwise, process errors then deserialize the response.
        let response = response
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;
        let get_balance_response: GetAccountFilesResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(get_balance_response.total_size)
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
            .query(&[("key", key)])
            .send()
            .await?;
        let aggregate_response: AggregateResponse<T> = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;

        Ok(aggregate_response.data)
    }
}

impl AlephPostClient for AlephClient {
    async fn get_posts_v0(&self, filter: &PostFilter) -> Result<GetPostsV0Response, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
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

    async fn get_posts_v1(&self, filter: &PostFilter) -> Result<GetPostsV1Response, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v1/posts.json")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self
            .http_client
            .get(url)
            .query(&filter)
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_models::corechannel::CORECHANNEL_ADDRESS;
    use aleph_types::{address, channel, item_hash};

    const FORGOTTEN_MESSAGE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/api-responses/forgotten-message.json"
    ));
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

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
    async fn test_get_file_size() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash =
            item_hash!("47959b5e166ed22fc78ed402236beeb234687d34d8edd4cb78fe7e4963b135e0");

        let size = client
            .get_file_size(&file_hash)
            .await
            .unwrap_or_else(|e| panic!("failed to fetch file: {:?}", e));
        assert_eq!(size, Bytes::from(297));
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
    async fn test_download_file_by_hash() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash =
            item_hash!("47959b5e166ed22fc78ed402236beeb234687d34d8edd4cb78fe7e4963b135e0");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        let content = download.bytes().await.expect("should read bytes");
        assert_eq!(content.len(), 297);
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
    async fn test_download_file_to_disk() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash =
            item_hash!("47959b5e166ed22fc78ed402236beeb234687d34d8edd4cb78fe7e4963b135e0");

        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join("aleph-test-download");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        download
            .to_file(&tmp_file)
            .await
            .expect("should write to file");

        let metadata = std::fs::metadata(&tmp_file).expect("file should exist");
        assert_eq!(metadata.len(), 297);

        std::fs::remove_file(&tmp_file).ok();
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
    async fn test_download_file_with_verification() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash =
            item_hash!("47959b5e166ed22fc78ed402236beeb234687d34d8edd4cb78fe7e4963b135e0");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        let content = download
            .with_verification()
            .bytes()
            .await
            .expect("verified download should succeed");
        assert_eq!(content.len(), 297);
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
    async fn test_download_file_to_disk_with_verification() {
        let client = AlephClient::new(Url::parse("https://api3.aleph.im").expect("valid url"));
        let file_hash =
            item_hash!("47959b5e166ed22fc78ed402236beeb234687d34d8edd4cb78fe7e4963b135e0");

        let tmp_dir = std::env::temp_dir();
        let tmp_file = tmp_dir.join("aleph-test-download-verified");

        let download = client
            .download_file_by_hash(&file_hash)
            .await
            .expect("download should succeed");

        download
            .with_verification()
            .to_file(&tmp_file)
            .await
            .expect("verified write to file should succeed");

        let metadata = std::fs::metadata(&tmp_file).expect("file should exist");
        assert_eq!(metadata.len(), 297);

        std::fs::remove_file(&tmp_file).ok();
    }

    #[tokio::test]
    #[ignore = "uses a remote CCN"]
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
    #[ignore = "uses a remote CCN"]
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
    #[ignore = "uses a remote CCN"]
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
    #[ignore = "uses a remote CCN websocket"]
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
}
