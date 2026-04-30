//! Credit transfer message schema and validation.
//!
//! These types describe the JSON payload of an `aleph_credit_transfer`
//! POST message. They live outside the `credit` module so they can be
//! used by code paths (e.g. the heph server) that don't want the on-chain
//! buy-credits dependencies.

use aleph_types::chain::Address as AlephAddress;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// POST `type` value for credit transfer messages on the Aleph network.
pub const CREDIT_TRANSFER_POST_TYPE: &str = "aleph_credit_transfer";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditTransferEntry {
    pub address: AlephAddress,
    pub amount: u64,
    #[serde(
        default,
        with = "chrono::serde::ts_seconds_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub expiration: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditTransferList {
    pub credits: Vec<CreditTransferEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreditTransferContent {
    pub transfer: CreditTransferList,
}

#[derive(Debug, thiserror::Error)]
pub enum CreditTransferError {
    #[error("credits list must not be empty")]
    EmptyCredits,
    #[error("amount must be strictly positive (got {0})")]
    NonPositiveAmount(u64),
    #[error("recipient address must not be empty")]
    EmptyAddress,
    #[error("duplicate recipient address: {0}")]
    DuplicateRecipient(AlephAddress),
    #[error("expiration must not be before the unix epoch (got {0})")]
    NegativeExpiration(DateTime<Utc>),
    #[error("sender and recipient must differ (got {0})")]
    SelfTransfer(AlephAddress),
}

impl CreditTransferContent {
    /// Mirror pyaleph's server-side validation so we fail fast before signing.
    /// Sender-vs-recipient is checked by the caller (the schema does not
    /// know the sender).
    pub fn validate(&self) -> Result<(), CreditTransferError> {
        let credits = &self.transfer.credits;
        if credits.is_empty() {
            return Err(CreditTransferError::EmptyCredits);
        }

        let mut seen = std::collections::HashSet::with_capacity(credits.len());
        for entry in credits {
            if entry.amount == 0 {
                return Err(CreditTransferError::NonPositiveAmount(entry.amount));
            }
            if entry.address.as_str().trim().is_empty() {
                return Err(CreditTransferError::EmptyAddress);
            }
            if let Some(exp) = entry.expiration
                && exp.timestamp() < 0
            {
                return Err(CreditTransferError::NegativeExpiration(exp));
            }
            if !seen.insert(entry.address.clone()) {
                return Err(CreditTransferError::DuplicateRecipient(
                    entry.address.clone(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn credit_transfer_content_round_trips_with_expiration() {
        let dt = chrono::Utc
            .with_ymd_and_hms(2026, 12, 31, 23, 59, 59)
            .unwrap();
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("0xrecipient".to_string()),
                    amount: 1500,
                    expiration: Some(dt),
                }],
            },
        };

        let json = serde_json::to_value(&content).unwrap();
        assert_eq!(json["transfer"]["credits"][0]["address"], "0xrecipient");
        assert_eq!(json["transfer"]["credits"][0]["amount"], 1500);
        assert_eq!(json["transfer"]["credits"][0]["expiration"], dt.timestamp());

        let back: CreditTransferContent = serde_json::from_value(json).unwrap();
        assert_eq!(back.transfer.credits[0].amount, 1500);
        assert_eq!(back.transfer.credits[0].expiration, Some(dt));
    }

    #[test]
    fn credit_transfer_entry_omits_expiration_when_none() {
        let entry = CreditTransferEntry {
            address: AlephAddress::from("0xrecipient".to_string()),
            amount: 1,
            expiration: None,
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert!(
            json.get("expiration").is_none(),
            "expiration should be omitted when None, got: {json}"
        );
    }

    #[test]
    fn validate_accepts_single_recipient_positive_amount() {
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("0xrecipient".to_string()),
                    amount: 1,
                    expiration: None,
                }],
            },
        };
        assert!(content.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_credits_list() {
        let content = CreditTransferContent {
            transfer: CreditTransferList { credits: vec![] },
        };
        assert!(matches!(
            content.validate(),
            Err(CreditTransferError::EmptyCredits)
        ));
    }

    #[test]
    fn validate_rejects_zero_amount() {
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("0xrecipient".to_string()),
                    amount: 0,
                    expiration: None,
                }],
            },
        };
        assert!(matches!(
            content.validate(),
            Err(CreditTransferError::NonPositiveAmount(0))
        ));
    }

    #[test]
    fn validate_rejects_blank_address() {
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("   ".to_string()),
                    amount: 1,
                    expiration: None,
                }],
            },
        };
        assert!(matches!(
            content.validate(),
            Err(CreditTransferError::EmptyAddress)
        ));
    }

    #[test]
    fn validate_rejects_duplicate_recipients() {
        let dup = AlephAddress::from("0xrecipient".to_string());
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![
                    CreditTransferEntry {
                        address: dup.clone(),
                        amount: 1,
                        expiration: None,
                    },
                    CreditTransferEntry {
                        address: dup,
                        amount: 2,
                        expiration: None,
                    },
                ],
            },
        };
        assert!(matches!(
            content.validate(),
            Err(CreditTransferError::DuplicateRecipient(_))
        ));
    }

    #[test]
    fn validate_rejects_pre_epoch_expiration() {
        let pre_epoch = chrono::Utc.with_ymd_and_hms(1969, 1, 1, 0, 0, 0).unwrap();
        let content = CreditTransferContent {
            transfer: CreditTransferList {
                credits: vec![CreditTransferEntry {
                    address: AlephAddress::from("0xrecipient".to_string()),
                    amount: 1,
                    expiration: Some(pre_epoch),
                }],
            },
        };
        assert!(matches!(
            content.validate(),
            Err(CreditTransferError::NegativeExpiration(_))
        ));
    }
}
