use crate::aggregate_models::corechannel::{CORECHANNEL_ADDRESS, CoreChannelAggregate};
use aleph_types::chain::{Address, Chain, Signature};
use aleph_types::channel::Channel;
use aleph_types::item_hash::ItemHash;
use aleph_types::memory_size::{Bytes, MemorySize};
use aleph_types::message::{
    ContentSource, FileRef, Message, MessageStatus, MessageType, RawFileRef,
};
use aleph_types::timestamp::Timestamp;
use chrono::{DateTime, Utc};
use futures_util::{Stream, StreamExt};
use reqwest::StatusCode;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{RetryTransientMiddleware, policies::ExponentialBackoff};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{StringWithSeparator, formats::CommaSeparator, serde_as, skip_serializing_none};
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use url::Url;

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
    #[error(transparent)]
    HttpError(#[from] reqwest_middleware::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("WebSocket connection error: {0}")]
    WebsocketConnection(String),
    #[error("WebSocket message error: {0}")]
    WebsocketMessage(String),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovalReason {
    BalanceInsufficient,
    CreditInsufficient,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingMessage {
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
pub enum MessageWithStatus {
    // More than one message with the same item hash can be pending at the same time.
    Pending {
        messages: Vec<PendingMessage>,
    },
    Processed {
        message: Message,
    },
    Removing {
        message: Message,
        reason: RemovalReason,
    },
    Removed {
        message: Message,
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

impl MessageWithStatus {
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
}

#[derive(Debug, Deserialize)]
struct GetMessageResponse {
    #[serde(flatten)]
    message: MessageWithStatus,
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

/// Serialization-only struct for POSTing a message to a node.
/// Contains only the fields accepted by POST /api/v0/messages.
#[derive(Serialize)]
struct RawMessage<'a> {
    sender: &'a Address,
    chain: &'a Chain,
    signature: &'a Signature,
    #[serde(rename = "type")]
    message_type: &'a MessageType,
    item_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    item_content: Option<&'a str>,
    item_hash: &'a ItemHash,
    time: &'a Timestamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    channel: Option<&'a Channel>,
}

impl<'a> RawMessage<'a> {
    fn from_message(message: &'a Message) -> Self {
        let (item_type, item_content) = match &message.content_source {
            ContentSource::Inline { item_content } => ("inline", Some(item_content.as_str())),
            ContentSource::Storage => ("storage", None),
            ContentSource::Ipfs => ("ipfs", None),
        };
        RawMessage {
            sender: &message.sender,
            chain: &message.chain,
            signature: &message.signature,
            message_type: &message.message_type,
            item_type,
            item_content,
            item_hash: &message.item_hash,
            time: &message.time,
            channel: message.channel.as_ref(),
        }
    }
}

#[derive(Serialize)]
struct PostMessageBody<'a> {
    sync: bool,
    message: RawMessage<'a>,
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
    ) -> impl Future<Output = Result<MessageWithStatus, MessageError>> + Send;
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
        message: &Message,
        sync: bool,
    ) -> impl Future<Output = Result<PostMessageResponse, MessageError>> + Send;
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
    fn get_corechahannel_aggregate(
        &self,
    ) -> impl Future<Output = Result<CoreChannelAggregate, MessageError>> + Send {
        self.get_aggregate(&CORECHANNEL_ADDRESS, "corechannel")
    }
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

impl AlephClient {
    pub fn new(ccn_url: Url) -> Self {
        Self::with_retry_config(ccn_url, RetryConfig::default())
    }

    pub fn with_retry_config(ccn_url: Url, retry_config: RetryConfig) -> Self {
        let retry_policy = ExponentialBackoff::builder()
            .retry_bounds(retry_config.min_backoff, retry_config.max_backoff)
            .build_with_max_retries(retry_config.max_retries);

        let http_client = ClientBuilder::new(reqwest::Client::new())
            .with(RetryTransientMiddleware::new_with_policy(retry_policy))
            .build();

        Self {
            http_client,
            ccn_url,
        }
    }
}

impl AlephMessageClient for AlephClient {
    /// Queries a message by item hash.
    ///
    /// Returns the message with its corresponding status.
    async fn get_message(&self, item_hash: &ItemHash) -> Result<MessageWithStatus, MessageError> {
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
        message: &Message,
        sync: bool,
    ) -> Result<PostMessageResponse, MessageError> {
        let url = self
            .ccn_url
            .join("/api/v0/messages")
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let body = PostMessageBody {
            sync,
            message: RawMessage::from_message(message),
        };

        let response = self
            .http_client
            .post(url)
            .json(&body)
            .send()
            .await?
            .error_for_status()
            .map_err(reqwest_middleware::Error::from)?;

        let post_response: PostMessageResponse = response
            .json()
            .await
            .map_err(reqwest_middleware::Error::from)?;
        Ok(post_response)
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
                    .map(Bytes::from_units)
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
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

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
            return Ok(Bytes::from_units(0));
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

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::{address, channel, item_hash};

    const FORGOTTEN_MESSAGE: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/api-responses/forgotten-message.json"
    ));
    #[test]
    fn test_deserialize_forgotten_message() {
        let message: MessageWithStatus = serde_json::from_str(FORGOTTEN_MESSAGE).unwrap();

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
        assert_eq!(size, Bytes::from_units(297));
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
        let file_hash =
            item_hash!("QmQKPXPMENCLL7HfyPiTZkmyX5iHp5QrYdWWMeP6pEhiS4");

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
}
