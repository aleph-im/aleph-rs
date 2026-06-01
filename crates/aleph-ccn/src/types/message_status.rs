//! Message status, error codes and processing exceptions.
//!
//! Mirrors `src/aleph/types/message_status.py`.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Where a message came from when it was received.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageOrigin {
    Onchain,
    P2p,
    Ipfs,
}

/// Persisted lifecycle status of a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageStatus {
    Pending,
    Processed,
    Rejected,
    Forgotten,
    Removing,
    Removed,
}

/// Status reported while a message is being processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MessageProcessingStatus {
    #[serde(rename = "processed")]
    ProcessedNewMessage,
    #[serde(rename = "confirmed")]
    ProcessedConfirmation,
    #[serde(rename = "retry")]
    FailedWillRetry,
    #[serde(rename = "rejected")]
    FailedRejected,
}

impl MessageProcessingStatus {
    /// Convert a processing status into the persisted message status.
    pub fn to_message_status(self) -> MessageStatus {
        match self {
            MessageProcessingStatus::ProcessedConfirmation
            | MessageProcessingStatus::ProcessedNewMessage => MessageStatus::Processed,
            MessageProcessingStatus::FailedWillRetry => MessageStatus::Pending,
            MessageProcessingStatus::FailedRejected => MessageStatus::Rejected,
        }
    }

    /// String value used in JSON / DB representations (matches Python `.value`).
    pub fn as_value_str(&self) -> &'static str {
        match self {
            MessageProcessingStatus::ProcessedNewMessage => "processed",
            MessageProcessingStatus::ProcessedConfirmation => "confirmed",
            MessageProcessingStatus::FailedWillRetry => "retry",
            MessageProcessingStatus::FailedRejected => "rejected",
        }
    }
}

/// Numeric error codes for message processing failures (Python `IntEnum`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum ErrorCode {
    InternalError = -1,
    InvalidFormat = 0,
    InvalidSignature = 1,
    PermissionDenied = 2,
    ContentUnavailable = 3,
    FileUnavailable = 4,
    BalanceInsufficient = 5,
    CreditInsufficient = 6,
    PostAmendNoTarget = 100,
    PostAmendTargetNotFound = 101,
    PostAmendAmend = 102,
    StoreRefNotFound = 200,
    StoreUpdateUpdate = 201,
    InvalidPaymentMethod = 202,
    VmRefNotFound = 300,
    VmVolumeNotFound = 301,
    VmAmendNotAllowed = 302,
    VmUpdateUpdate = 303,
    VmVolumeTooSmall = 304,
    ForgetNoTarget = 500,
    ForgetTargetNotFound = 501,
    ForgetForget = 502,
    ForgetNotAllowed = 503,
    ForgottenDuplicate = 504,
}

impl ErrorCode {
    pub const ALL: &'static [ErrorCode] = &[
        ErrorCode::InternalError,
        ErrorCode::InvalidFormat,
        ErrorCode::InvalidSignature,
        ErrorCode::PermissionDenied,
        ErrorCode::ContentUnavailable,
        ErrorCode::FileUnavailable,
        ErrorCode::BalanceInsufficient,
        ErrorCode::CreditInsufficient,
        ErrorCode::PostAmendNoTarget,
        ErrorCode::PostAmendTargetNotFound,
        ErrorCode::PostAmendAmend,
        ErrorCode::StoreRefNotFound,
        ErrorCode::StoreUpdateUpdate,
        ErrorCode::InvalidPaymentMethod,
        ErrorCode::VmRefNotFound,
        ErrorCode::VmVolumeNotFound,
        ErrorCode::VmAmendNotAllowed,
        ErrorCode::VmUpdateUpdate,
        ErrorCode::VmVolumeTooSmall,
        ErrorCode::ForgetNoTarget,
        ErrorCode::ForgetTargetNotFound,
        ErrorCode::ForgetForget,
        ErrorCode::ForgetNotAllowed,
        ErrorCode::ForgottenDuplicate,
    ];

    pub const fn as_i32(self) -> i32 {
        self as i32
    }
}

impl TryFrom<i32> for ErrorCode {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        Ok(match value {
            -1 => ErrorCode::InternalError,
            0 => ErrorCode::InvalidFormat,
            1 => ErrorCode::InvalidSignature,
            2 => ErrorCode::PermissionDenied,
            3 => ErrorCode::ContentUnavailable,
            4 => ErrorCode::FileUnavailable,
            5 => ErrorCode::BalanceInsufficient,
            6 => ErrorCode::CreditInsufficient,
            100 => ErrorCode::PostAmendNoTarget,
            101 => ErrorCode::PostAmendTargetNotFound,
            102 => ErrorCode::PostAmendAmend,
            200 => ErrorCode::StoreRefNotFound,
            201 => ErrorCode::StoreUpdateUpdate,
            202 => ErrorCode::InvalidPaymentMethod,
            300 => ErrorCode::VmRefNotFound,
            301 => ErrorCode::VmVolumeNotFound,
            302 => ErrorCode::VmAmendNotAllowed,
            303 => ErrorCode::VmUpdateUpdate,
            304 => ErrorCode::VmVolumeTooSmall,
            500 => ErrorCode::ForgetNoTarget,
            501 => ErrorCode::ForgetTargetNotFound,
            502 => ErrorCode::ForgetForget,
            503 => ErrorCode::ForgetNotAllowed,
            504 => ErrorCode::ForgottenDuplicate,
            other => return Err(other),
        })
    }
}

impl From<ErrorCode> for i32 {
    fn from(value: ErrorCode) -> Self {
        value as i32
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i32(self.as_i32())
    }
}

impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let n = i32::deserialize(deserializer)?;
        ErrorCode::try_from(n)
            .map_err(|v| serde::de::Error::custom(format!("unknown ErrorCode value: {v}")))
    }
}

/// Reason a message was removed (post-processing administrative action).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemovedMessageReason {
    BalanceInsufficient,
    CreditInsufficient,
}

/// Base error returned by message processing routines.
///
/// Mirrors Python `MessageProcessingException` (and its concrete subclasses).
/// In Python the class hierarchy carries two pieces of information:
/// 1. whether the message should be retried (`RetryMessageException`) or
///    rejected (`InvalidMessageException`), and
/// 2. a stable `error_code` used for client-visible reporting plus any
///    structured `details()` payload.
///
/// We model that as a single enum with a `kind`, since Rust doesn't have a
/// matching inheritance mechanism. Use `is_retry()` to determine retry behavior.
#[derive(Debug, Clone, Error)]
pub enum MessageProcessingException {
    /// An unexpected situation occurred. (`INTERNAL_ERROR`, retry)
    #[error("InternalError")]
    InternalError { errors: Vec<String> },

    /// Message not properly formatted. (`INVALID_FORMAT`, reject)
    #[error("InvalidMessageFormat")]
    InvalidMessageFormat { errors: Vec<String> },

    /// Signature does not match expected value. (`INVALID_SIGNATURE`, reject)
    #[error("InvalidSignature")]
    InvalidSignature { errors: Vec<String> },

    /// Sender lacks permission for the operation. (`PERMISSION_DENIED`, reject)
    #[error("PermissionDenied")]
    PermissionDenied { errors: Vec<String> },

    /// Message content not currently available. (`CONTENT_UNAVAILABLE`, retry)
    #[error("MessageContentUnavailable")]
    MessageContentUnavailable {
        file_hash: String,
        details: Option<String>,
    },

    /// A referenced file is unavailable. (`FILE_UNAVAILABLE`, retry)
    #[error("FileUnavailable")]
    FileUnavailable {
        file_hash: String,
        details: Option<String>,
    },

    /// Amend POST without ref. (`POST_AMEND_NO_TARGET`, reject)
    #[error("NoAmendTarget")]
    NoAmendTarget { errors: Vec<String> },

    /// Amend POST target not found. (`POST_AMEND_TARGET_NOT_FOUND`, retry)
    #[error("AmendTargetNotFound")]
    AmendTargetNotFound { errors: Vec<String> },

    /// Cannot amend an amend. (`POST_AMEND_AMEND`, reject)
    #[error("CannotAmendAmend")]
    CannotAmendAmend { errors: Vec<String> },

    /// FORGET targets nothing. (`FORGET_NO_TARGET`, reject)
    #[error("NoForgetTarget")]
    NoForgetTarget { errors: Vec<String> },

    /// STORE `ref` target not found. (`STORE_REF_NOT_FOUND`, retry)
    #[error("StoreRefNotFound")]
    StoreRefNotFound { errors: Vec<String> },

    /// STORE update-tree forbidden. (`STORE_UPDATE_UPDATE`, reject)
    #[error("StoreCannotUpdateStoreWithRef")]
    StoreCannotUpdateStoreWithRef { errors: Vec<String> },

    /// Non-credit payment after cutoff. (`INVALID_PAYMENT_METHOD`, reject)
    #[error("InvalidPaymentMethod")]
    InvalidPaymentMethod { errors: Vec<String> },

    /// FORGET target file is used by a VM. (`FORGET_NOT_ALLOWED`, reject)
    #[error("ForgetNotAllowed")]
    ForgetNotAllowed { file_hash: String, vm_hash: String },

    /// VM `ref` not found. (`VM_REF_NOT_FOUND`, retry)
    #[error("VmRefNotFound")]
    VmRefNotFound { errors: Vec<String> },

    /// VM volume file not found. (`VM_VOLUME_NOT_FOUND`, retry)
    #[error("VmVolumeNotFound")]
    VmVolumeNotFound { errors: Vec<String> },

    /// Trying to amend an immutable VM. (`VM_AMEND_NOT_ALLOWED`, reject)
    #[error("VmUpdateNotAllowed")]
    VmUpdateNotAllowed { errors: Vec<String> },

    /// VM update-tree forbidden. (`VM_UPDATE_UPDATE`, reject)
    #[error("VmCannotUpdateUpdate")]
    VmCannotUpdateUpdate { errors: Vec<String> },

    /// VM child volume smaller than parent. (`VM_VOLUME_TOO_SMALL`, reject)
    #[error("VmVolumeTooSmall")]
    VmVolumeTooSmall {
        volume_name: String,
        volume_size: i64,
        parent_ref: String,
        parent_file: String,
        parent_size: i64,
    },

    /// Account doesn't have enough credits. (`CREDIT_INSUFFICIENT`, reject)
    #[error("InsufficientCreditException")]
    InsufficientCredit {
        credit_balance: i64,
        required_credits: Decimal,
        min_runtime_days: i64,
    },

    /// FORGET target hash/aggregate not found. (`FORGET_TARGET_NOT_FOUND`, retry)
    #[error("ForgetTargetNotFound")]
    ForgetTargetNotFound {
        target_hash: Option<String>,
        aggregate_key: Option<String>,
    },

    /// FORGET targeting another FORGET. (`FORGET_FORGET`, reject)
    #[error("CannotForgetForgetMessage")]
    CannotForgetForgetMessage { target_hash: String },

    /// Account doesn't have enough balance. (`BALANCE_INSUFFICIENT`, reject)
    #[error("InsufficientBalanceException")]
    InsufficientBalance {
        balance: Decimal,
        required_balance: Decimal,
    },
}

impl MessageProcessingException {
    /// Returns the protocol-level error code (Python `error_code` attribute).
    pub fn error_code(&self) -> ErrorCode {
        match self {
            MessageProcessingException::InternalError { .. } => ErrorCode::InternalError,
            MessageProcessingException::InvalidMessageFormat { .. } => ErrorCode::InvalidFormat,
            MessageProcessingException::InvalidSignature { .. } => ErrorCode::InvalidSignature,
            MessageProcessingException::PermissionDenied { .. } => ErrorCode::PermissionDenied,
            MessageProcessingException::MessageContentUnavailable { .. } => {
                ErrorCode::ContentUnavailable
            }
            MessageProcessingException::FileUnavailable { .. } => ErrorCode::FileUnavailable,
            MessageProcessingException::NoAmendTarget { .. } => ErrorCode::PostAmendNoTarget,
            MessageProcessingException::AmendTargetNotFound { .. } => {
                ErrorCode::PostAmendTargetNotFound
            }
            MessageProcessingException::CannotAmendAmend { .. } => ErrorCode::PostAmendAmend,
            MessageProcessingException::NoForgetTarget { .. } => ErrorCode::ForgetNoTarget,
            MessageProcessingException::StoreRefNotFound { .. } => ErrorCode::StoreRefNotFound,
            MessageProcessingException::StoreCannotUpdateStoreWithRef { .. } => {
                ErrorCode::StoreUpdateUpdate
            }
            MessageProcessingException::InvalidPaymentMethod { .. } => {
                ErrorCode::InvalidPaymentMethod
            }
            MessageProcessingException::ForgetNotAllowed { .. } => ErrorCode::ForgetNotAllowed,
            MessageProcessingException::VmRefNotFound { .. } => ErrorCode::VmRefNotFound,
            MessageProcessingException::VmVolumeNotFound { .. } => ErrorCode::VmVolumeNotFound,
            MessageProcessingException::VmUpdateNotAllowed { .. } => ErrorCode::VmAmendNotAllowed,
            MessageProcessingException::VmCannotUpdateUpdate { .. } => ErrorCode::VmUpdateUpdate,
            MessageProcessingException::VmVolumeTooSmall { .. } => ErrorCode::VmVolumeTooSmall,
            MessageProcessingException::InsufficientCredit { .. } => ErrorCode::CreditInsufficient,
            MessageProcessingException::ForgetTargetNotFound { .. } => {
                ErrorCode::ForgetTargetNotFound
            }
            MessageProcessingException::CannotForgetForgetMessage { .. } => ErrorCode::ForgetForget,
            MessageProcessingException::InsufficientBalance { .. } => {
                ErrorCode::BalanceInsufficient
            }
        }
    }

    /// Whether processing should be retried (Python `RetryMessageException`).
    /// Otherwise the message is rejected (`InvalidMessageException`).
    pub fn is_retry(&self) -> bool {
        matches!(
            self,
            MessageProcessingException::InternalError { .. }
                | MessageProcessingException::MessageContentUnavailable { .. }
                | MessageProcessingException::FileUnavailable { .. }
                | MessageProcessingException::AmendTargetNotFound { .. }
                | MessageProcessingException::StoreRefNotFound { .. }
                | MessageProcessingException::VmRefNotFound { .. }
                | MessageProcessingException::VmVolumeNotFound { .. }
                | MessageProcessingException::ForgetTargetNotFound { .. }
        )
    }

    /// JSON-serialisable details (Python `details()` method).
    ///
    /// Returns `None` when there is nothing structured to report.
    pub fn details(&self) -> Option<serde_json::Value> {
        use serde_json::json;
        match self {
            // Generic free-form error list variants
            MessageProcessingException::InternalError { errors }
            | MessageProcessingException::InvalidMessageFormat { errors }
            | MessageProcessingException::InvalidSignature { errors }
            | MessageProcessingException::PermissionDenied { errors }
            | MessageProcessingException::NoAmendTarget { errors }
            | MessageProcessingException::AmendTargetNotFound { errors }
            | MessageProcessingException::CannotAmendAmend { errors }
            | MessageProcessingException::NoForgetTarget { errors }
            | MessageProcessingException::StoreRefNotFound { errors }
            | MessageProcessingException::StoreCannotUpdateStoreWithRef { errors }
            | MessageProcessingException::InvalidPaymentMethod { errors }
            | MessageProcessingException::VmRefNotFound { errors }
            | MessageProcessingException::VmVolumeNotFound { errors }
            | MessageProcessingException::VmUpdateNotAllowed { errors }
            | MessageProcessingException::VmCannotUpdateUpdate { errors } => {
                if errors.is_empty() {
                    None
                } else {
                    Some(json!({ "errors": errors }))
                }
            }
            // FileNotFound subclasses: Python builds `f"File not found: {file_hash}"`
            // and appends `(details)` when present, storing the result as the
            // single error string.
            MessageProcessingException::MessageContentUnavailable { file_hash, details }
            | MessageProcessingException::FileUnavailable { file_hash, details } => {
                let message = match details {
                    Some(d) => format!("File not found: {file_hash} ({d})"),
                    None => format!("File not found: {file_hash}"),
                };
                Some(json!({ "errors": [message] }))
            }
            MessageProcessingException::ForgetNotAllowed { file_hash, vm_hash } => {
                Some(json!({ "errors": [format!("File {file_hash} used on vm {vm_hash}")] }))
            }
            MessageProcessingException::VmVolumeTooSmall {
                volume_name,
                volume_size,
                parent_ref,
                parent_file,
                parent_size,
            } => Some(json!({
                "errors": [{
                    "volume_name": volume_name,
                    "parent_ref": parent_ref,
                    "parent_file": parent_file,
                    "parent_size": parent_size,
                    "volume_size": volume_size,
                }]
            })),
            MessageProcessingException::InsufficientCredit {
                credit_balance,
                required_credits,
                min_runtime_days,
            } => Some(json!({
                "errors": [{
                    "required_credits": required_credits.to_string(),
                    "account_credits": credit_balance.to_string(),
                    "min_runtime_days": min_runtime_days,
                }]
            })),
            MessageProcessingException::ForgetTargetNotFound {
                target_hash,
                aggregate_key,
            } => {
                let mut errors: Vec<serde_json::Value> = Vec::new();
                if let Some(t) = target_hash {
                    errors.push(json!({ "message": t }));
                }
                if let Some(k) = aggregate_key {
                    errors.push(json!({ "aggregate": k }));
                }
                Some(json!({ "errors": errors }))
            }
            MessageProcessingException::CannotForgetForgetMessage { target_hash } => Some(json!({
                "errors": [{"message": target_hash}]
            })),
            MessageProcessingException::InsufficientBalance {
                balance,
                required_balance,
            } => Some(json!({
                "errors": [{
                    "required_balance": required_balance.to_string(),
                    "account_balance": balance.to_string(),
                }]
            })),
        }
    }
}

impl MessageProcessingException {
    /// Builder for `MessageContentUnavailable` / `FileUnavailable`, mirroring
    /// the Python `FileNotFoundException(file_hash, details=None)` constructor.
    pub fn message_content_unavailable(file_hash: impl Into<String>) -> Self {
        MessageProcessingException::MessageContentUnavailable {
            file_hash: file_hash.into(),
            details: None,
        }
    }

    /// `MessageContentUnavailable` with explanatory `details`, mirroring
    /// Python's `MessageContentUnavailable(item_hash, "...")`.
    pub fn message_content_unavailable_with_details(
        file_hash: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        MessageProcessingException::MessageContentUnavailable {
            file_hash: file_hash.into(),
            details: Some(details.into()),
        }
    }

    pub fn file_unavailable(file_hash: impl Into<String>) -> Self {
        MessageProcessingException::FileUnavailable {
            file_hash: file_hash.into(),
            details: None,
        }
    }

    /// `FileUnavailable` with explanatory `details`, mirroring Python's
    /// `FileUnavailable(file_hash, "...")`.
    pub fn file_unavailable_with_details(
        file_hash: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        MessageProcessingException::FileUnavailable {
            file_hash: file_hash.into(),
            details: Some(details.into()),
        }
    }
}

/// Newtype alias for "invalid message" exceptions (Python `InvalidMessageException`).
///
/// In Python this is a separate base class used for `isinstance` checks; in Rust
/// the discriminator is `MessageProcessingException::is_retry() == false`.
pub fn is_invalid_message(err: &MessageProcessingException) -> bool {
    !err.is_retry()
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_i32())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_origin_roundtrip() {
        for (variant, expected) in [
            (MessageOrigin::Onchain, "\"onchain\""),
            (MessageOrigin::P2p, "\"p2p\""),
            (MessageOrigin::Ipfs, "\"ipfs\""),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: MessageOrigin = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn message_status_roundtrip() {
        for (variant, expected) in [
            (MessageStatus::Pending, "\"pending\""),
            (MessageStatus::Processed, "\"processed\""),
            (MessageStatus::Rejected, "\"rejected\""),
            (MessageStatus::Forgotten, "\"forgotten\""),
            (MessageStatus::Removing, "\"removing\""),
            (MessageStatus::Removed, "\"removed\""),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            let parsed: MessageStatus = serde_json::from_str(&s).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn message_processing_status_roundtrip_and_mapping() {
        for (variant, expected, target) in [
            (
                MessageProcessingStatus::ProcessedNewMessage,
                "\"processed\"",
                MessageStatus::Processed,
            ),
            (
                MessageProcessingStatus::ProcessedConfirmation,
                "\"confirmed\"",
                MessageStatus::Processed,
            ),
            (
                MessageProcessingStatus::FailedWillRetry,
                "\"retry\"",
                MessageStatus::Pending,
            ),
            (
                MessageProcessingStatus::FailedRejected,
                "\"rejected\"",
                MessageStatus::Rejected,
            ),
        ] {
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, expected);
            assert_eq!(variant.to_message_status(), target);
        }
    }

    #[test]
    fn error_code_roundtrip() {
        let cases = [
            (ErrorCode::InternalError, -1),
            (ErrorCode::InvalidFormat, 0),
            (ErrorCode::CreditInsufficient, 6),
            (ErrorCode::PostAmendNoTarget, 100),
            (ErrorCode::VmVolumeTooSmall, 304),
            (ErrorCode::ForgottenDuplicate, 504),
        ];
        for (variant, n) in cases {
            assert_eq!(variant.as_i32(), n);
            assert_eq!(i32::from(variant), n);
            assert_eq!(ErrorCode::try_from(n).unwrap(), variant);
            let s = serde_json::to_string(&variant).unwrap();
            assert_eq!(s, n.to_string());
            let back: ErrorCode = serde_json::from_str(&s).unwrap();
            assert_eq!(back, variant);
        }
        assert!(ErrorCode::try_from(9999).is_err());
    }

    #[test]
    fn removed_message_reason_roundtrip() {
        assert_eq!(
            serde_json::to_string(&RemovedMessageReason::BalanceInsufficient).unwrap(),
            "\"balance_insufficient\""
        );
        let parsed: RemovedMessageReason = serde_json::from_str("\"credit_insufficient\"").unwrap();
        assert_eq!(parsed, RemovedMessageReason::CreditInsufficient);
    }

    #[test]
    fn exception_error_code_and_retry() {
        let e = MessageProcessingException::InvalidSignature {
            errors: vec!["bad sig".into()],
        };
        assert_eq!(e.error_code(), ErrorCode::InvalidSignature);
        assert!(!e.is_retry());

        let e = MessageProcessingException::message_content_unavailable("QmHash");
        assert_eq!(e.error_code(), ErrorCode::ContentUnavailable);
        assert!(e.is_retry());
        let details = e.details().unwrap();
        assert_eq!(details["errors"][0], "File not found: QmHash");
    }

    #[test]
    fn exception_details_structured() {
        let e = MessageProcessingException::VmVolumeTooSmall {
            volume_name: "vol".into(),
            volume_size: 1024,
            parent_ref: "ref".into(),
            parent_file: "file".into(),
            parent_size: 2048,
        };
        let d = e.details().unwrap();
        assert_eq!(d["errors"][0]["volume_name"], "vol");
        assert_eq!(d["errors"][0]["parent_size"], 2048);

        let e = MessageProcessingException::InsufficientBalance {
            balance: Decimal::new(1, 0),
            required_balance: Decimal::new(5, 0),
        };
        let d = e.details().unwrap();
        assert_eq!(d["errors"][0]["account_balance"], "1");
        assert_eq!(d["errors"][0]["required_balance"], "5");

        let e = MessageProcessingException::ForgetTargetNotFound {
            target_hash: Some("h".into()),
            aggregate_key: None,
        };
        let d = e.details().unwrap();
        assert_eq!(d["errors"][0]["message"], "h");
        assert_eq!(d["errors"].as_array().unwrap().len(), 1);
    }
}
