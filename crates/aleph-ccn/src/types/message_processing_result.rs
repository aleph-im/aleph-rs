//! Results returned by the message processor.
//!
//! Mirrors `src/aleph/types/message_processing_result.py`.
//!
//! Python defines `MessageProcessingResult` as a `Protocol` (structural type)
//! plus three concrete classes (`ProcessedMessage`, `WillRetryMessage`,
//! `RejectedMessage`). We mirror the same shape with:
//!
//! * a `MessageProcessingResult` trait carrying the common API
//!   (`item_hash`, `status`, `origin`, `to_dict`), and
//! * concrete `ProcessedMessage`, `FailedMessage` (with builder helpers
//!   `will_retry()` / `rejected()`) types implementing it.
//!
//! The DB model types (`MessageDb`, `PendingMessageDb`) are not yet ported,
//! so we accept their *formatted* form via `serde_json::Value`, matching what
//! Python ends up putting in the dict (`format_message(self.message).model_dump()`
//! for processed; just the item hash for failed).

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::types::message_status::{ErrorCode, MessageOrigin, MessageProcessingStatus};

/// Common API for all message processing results.
///
/// Mirrors Python `MessageProcessingResult` `Protocol`.
pub trait MessageProcessingResult {
    fn item_hash(&self) -> &str;
    fn status(&self) -> MessageProcessingStatus;
    fn origin(&self) -> Option<MessageOrigin>;
    fn to_dict(&self) -> Value;
}

/// A successfully processed (new or confirmation) message.
///
/// `message` is the rendered API form of the underlying `MessageDb` (the
/// equivalent of `format_message(self.message).model_dump()` in Python).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedMessage {
    pub item_hash: String,
    pub message: Value,
    pub status: MessageProcessingStatus,
    pub origin: Option<MessageOrigin>,
}

impl ProcessedMessage {
    pub fn new(item_hash: String, message: Value, is_confirmation: bool) -> Self {
        Self {
            item_hash,
            message,
            status: if is_confirmation {
                MessageProcessingStatus::ProcessedConfirmation
            } else {
                MessageProcessingStatus::ProcessedNewMessage
            },
            origin: None,
        }
    }

    pub fn with_origin(mut self, origin: Option<MessageOrigin>) -> Self {
        self.origin = origin;
        self
    }
}

impl MessageProcessingResult for ProcessedMessage {
    fn item_hash(&self) -> &str {
        &self.item_hash
    }

    fn status(&self) -> MessageProcessingStatus {
        self.status
    }

    fn origin(&self) -> Option<MessageOrigin> {
        self.origin
    }

    fn to_dict(&self) -> Value {
        json!({
            "status": self.status.as_value_str(),
            "message": self.message,
        })
    }
}

/// A message that failed processing — either to be retried or rejected.
///
/// `WillRetryMessage` and `RejectedMessage` in Python are thin subclasses of
/// `FailedMessage`; here we model the discriminator via the `status` field
/// and provide the same construction shortcuts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedMessage {
    pub item_hash: String,
    pub error_code: ErrorCode,
    pub status: MessageProcessingStatus,
    pub origin: Option<MessageOrigin>,
}

impl FailedMessage {
    pub fn new(item_hash: String, error_code: ErrorCode, will_retry: bool) -> Self {
        Self {
            item_hash,
            error_code,
            status: if will_retry {
                MessageProcessingStatus::FailedWillRetry
            } else {
                MessageProcessingStatus::FailedRejected
            },
            origin: None,
        }
    }

    /// Mirrors Python `WillRetryMessage(pending_message, error_code)`.
    pub fn will_retry(item_hash: String, error_code: ErrorCode) -> Self {
        Self::new(item_hash, error_code, true)
    }

    /// Mirrors Python `RejectedMessage(pending_message, error_code)`.
    pub fn rejected(item_hash: String, error_code: ErrorCode) -> Self {
        Self::new(item_hash, error_code, false)
    }

    pub fn with_origin(mut self, origin: Option<MessageOrigin>) -> Self {
        self.origin = origin;
        self
    }
}

impl MessageProcessingResult for FailedMessage {
    fn item_hash(&self) -> &str {
        &self.item_hash
    }

    fn status(&self) -> MessageProcessingStatus {
        self.status
    }

    fn origin(&self) -> Option<MessageOrigin> {
        self.origin
    }

    fn to_dict(&self) -> Value {
        json!({
            "status": self.status.as_value_str(),
            "item_hash": self.item_hash,
        })
    }
}

/// Sum type covering every kind of processing outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AnyMessageProcessingResult {
    Processed(ProcessedMessage),
    Failed(FailedMessage),
}

impl MessageProcessingResult for AnyMessageProcessingResult {
    fn item_hash(&self) -> &str {
        match self {
            AnyMessageProcessingResult::Processed(p) => p.item_hash(),
            AnyMessageProcessingResult::Failed(f) => f.item_hash(),
        }
    }

    fn status(&self) -> MessageProcessingStatus {
        match self {
            AnyMessageProcessingResult::Processed(p) => p.status(),
            AnyMessageProcessingResult::Failed(f) => f.status(),
        }
    }

    fn origin(&self) -> Option<MessageOrigin> {
        match self {
            AnyMessageProcessingResult::Processed(p) => p.origin(),
            AnyMessageProcessingResult::Failed(f) => f.origin(),
        }
    }

    fn to_dict(&self) -> Value {
        match self {
            AnyMessageProcessingResult::Processed(p) => p.to_dict(),
            AnyMessageProcessingResult::Failed(f) => f.to_dict(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn processed_new_message_to_dict() {
        let m = ProcessedMessage::new(
            "abc".to_string(),
            json!({"item_hash": "abc", "type": "POST"}),
            false,
        );
        assert_eq!(m.status(), MessageProcessingStatus::ProcessedNewMessage);
        assert_eq!(m.item_hash(), "abc");
        assert_eq!(m.origin(), None);

        let d = m.to_dict();
        assert_eq!(d["status"], "processed");
        assert_eq!(d["message"]["item_hash"], "abc");
    }

    #[test]
    fn processed_confirmation_to_dict() {
        let m = ProcessedMessage::new("xyz".into(), json!({}), true)
            .with_origin(Some(MessageOrigin::P2p));
        assert_eq!(m.status(), MessageProcessingStatus::ProcessedConfirmation);
        assert_eq!(m.origin(), Some(MessageOrigin::P2p));
        assert_eq!(m.to_dict()["status"], "confirmed");
    }

    #[test]
    fn will_retry_to_dict() {
        let m = FailedMessage::will_retry("hh".into(), ErrorCode::ContentUnavailable);
        assert_eq!(m.status(), MessageProcessingStatus::FailedWillRetry);
        let d = m.to_dict();
        assert_eq!(d["status"], "retry");
        assert_eq!(d["item_hash"], "hh");
    }

    #[test]
    fn rejected_to_dict() {
        let m = FailedMessage::rejected("hh".into(), ErrorCode::InvalidSignature);
        assert_eq!(m.status(), MessageProcessingStatus::FailedRejected);
        let d = m.to_dict();
        assert_eq!(d["status"], "rejected");
    }

    #[test]
    fn any_result_dispatches() {
        let any = AnyMessageProcessingResult::Failed(FailedMessage::will_retry(
            "x".into(),
            ErrorCode::FileUnavailable,
        ));
        assert_eq!(any.status(), MessageProcessingStatus::FailedWillRetry);
        assert_eq!(any.item_hash(), "x");
        assert_eq!(any.to_dict()["status"], "retry");
    }
}
