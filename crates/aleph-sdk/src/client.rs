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
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_with::{StringWithSeparator, formats::CommaSeparator, serde_as, skip_serializing_none};
use std::collections::HashMap;
use url::Url;

#[derive(Clone)]
pub struct AlephClient {
    http_client: reqwest::Client,
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
    HttpError(#[from] reqwest::Error),
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
    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, MessageType>>")]
    pub message_types: Option<Vec<MessageType>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub content_types: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub content_keys: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub refs: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub addresses: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub tags: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub hashes: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub channels: Option<Vec<String>>,

    #[serde_as(as = "Option<StringWithSeparator<CommaSeparator, String>>")]
    pub chains: Option<Vec<String>>,

    pub start_date: Option<Timestamp>,
    pub end_date: Option<Timestamp>,

    pub sort_by: Option<SortBy>,
    pub sort_order: Option<SortOrder>,

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
pub struct FileMetadata {
    #[serde(rename = "ref")]
    pub reference: RawFileRef,
    pub owner: Address,
    pub file_hash: ItemHash,
    pub size: Bytes,
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

impl AlephClient {
    pub fn new(ccn_url: Url) -> Self {
        Self {
            http_client: reqwest::Client::new(),
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

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(MessageError::NotFound(item_hash.clone()));
        }
        let response = response.error_for_status()?;

        let get_message_response: GetMessageResponse = response.json().await?;
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
            .error_for_status()?;

        let get_messages_response: GetMessagesResponse = response.json().await?;
        Ok(get_messages_response.messages)
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
            .error_for_status()?;
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

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(MessageError::NotFound(message_hash.clone()));
        }
        let response = response.error_for_status()?;

        let file_metadata: FileMetadata = response.json().await?;
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

        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StorageError::RefNotFound(file_ref.clone()).into());
        }
        let response = response.error_for_status()?;

        let file_metadata: FileMetadata = response.json().await?;
        Ok(file_metadata)
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

        let response = self.http_client.get(url).send().await?.error_for_status()?;
        let account_balance: AccountBalance = response.json().await?;

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
        let response = response.error_for_status()?;
        let get_balance_response: GetAccountFilesResponse = response.json().await?;

        Ok(get_balance_response.total_size)
    }

    async fn get_vm_price(&self, item_hash: &ItemHash) -> Result<f64, MessageError> {
        let url = self
            .ccn_url
            .join(&format!("/api/v0/price/{}", item_hash))
            .unwrap_or_else(|e| panic!("invalid url: {e}"));

        let response = self.http_client.get(url).send().await?.error_for_status()?;
        let get_price_response: GetPriceResponse = response.json().await?;

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
        let aggregate_response: AggregateResponse<T> = response.json().await?;

        Ok(aggregate_response.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_models::corechannel::{CORECHANNEL_ADDRESS, CoreChannelAggregate};
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
}
