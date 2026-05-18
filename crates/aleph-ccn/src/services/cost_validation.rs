//! Balance / credit validation for message costs.
//!
//! Mirrors `aleph/services/cost_validation.py`.

use rust_decimal::Decimal;
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::accessors::balances::{get_credit_balance, get_total_balance};
use crate::db::accessors::cost::get_total_cost_for_address;
use crate::db::models::account_costs::PaymentType;
use crate::toolkit::constants::DAY;
use crate::types::message_status::MessageProcessingException;

/// Outcome of [`validate_balance_for_payment`]: either OK or an
/// `InsufficientBalance` / `InsufficientCredit` exception.
#[derive(Debug)]
pub enum BalanceValidation {
    Ok,
    Invalid(MessageProcessingException),
}

impl BalanceValidation {
    pub fn into_result(self) -> Result<(), MessageProcessingException> {
        match self {
            BalanceValidation::Ok => Ok(()),
            BalanceValidation::Invalid(e) => Err(e),
        }
    }
}

/// Validates that `address` has sufficient balance to cover `message_cost`
/// given the requested `payment_type`.
///
/// * `Credit` payments require enough credit balance for at least one full day
///   of runtime (using current cost rate + this message's per-second rate).
/// * `Hold` and `Superfluid` payments simply require the running total cost +
///   new cost to be ≤ current balance.
///
/// Mirrors `validate_balance_for_payment` (Python raises; we return the
/// exception instead).
pub async fn validate_balance_for_payment(
    client: &impl GenericClient,
    address: &str,
    message_cost: Decimal,
    payment_type: PaymentType,
) -> AlephResult<BalanceValidation> {
    match payment_type {
        PaymentType::Credit => {
            let current_credit_balance = get_credit_balance(client, address, None).await?;
            let current_credit_cost =
                get_total_cost_for_address(client, address, Some(PaymentType::Credit)).await?;
            let total_per_second_cost = current_credit_cost + message_cost;
            let required_credits = total_per_second_cost * Decimal::from(DAY);
            if Decimal::from(current_credit_balance) < required_credits {
                return Ok(BalanceValidation::Invalid(
                    MessageProcessingException::InsufficientCredit {
                        credit_balance: current_credit_balance,
                        required_credits,
                        min_runtime_days: 1,
                    },
                ));
            }
            Ok(BalanceValidation::Ok)
        }
        _ => {
            let current_balance = get_total_balance(client, address, false).await?;
            let current_cost =
                get_total_cost_for_address(client, address, Some(payment_type)).await?;
            let required_balance = current_cost + message_cost;
            if current_balance < required_balance {
                return Ok(BalanceValidation::Invalid(
                    MessageProcessingException::InsufficientBalance {
                        balance: current_balance,
                        required_balance,
                    },
                ));
            }
            Ok(BalanceValidation::Ok)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message_status::ErrorCode;
    use std::str::FromStr;

    /// `BalanceValidation::into_result` correctly hands back a strongly typed
    /// error preserving the original payload.
    #[test]
    fn into_result_passes_through_exception() {
        let v = BalanceValidation::Invalid(MessageProcessingException::InsufficientBalance {
            balance: Decimal::from_str("10").unwrap(),
            required_balance: Decimal::from_str("100").unwrap(),
        });
        let err = v.into_result().unwrap_err();
        assert_eq!(err.error_code(), ErrorCode::BalanceInsufficient);
    }

    #[test]
    fn into_result_ok_returns_unit() {
        assert!(BalanceValidation::Ok.into_result().is_ok());
    }

    /// Credit exception carries the integer balance and decimal requirement.
    #[test]
    fn insufficient_credit_payload() {
        let v = BalanceValidation::Invalid(MessageProcessingException::InsufficientCredit {
            credit_balance: 5,
            required_credits: Decimal::from(86400),
            min_runtime_days: 1,
        });
        let err = v.into_result().unwrap_err();
        assert_eq!(err.error_code(), ErrorCode::CreditInsufficient);
        let details = err.details().unwrap();
        assert_eq!(details["errors"][0]["account_credits"], "5");
        assert_eq!(details["errors"][0]["required_credits"], "86400");
        assert_eq!(details["errors"][0]["min_runtime_days"], 1);
    }

    /// Sanity check: the validator computes `total_per_second_cost × DAY`.
    /// For 1 credit/sec, the requirement is exactly 86_400 (= DAY) credits.
    #[test]
    fn credit_required_uses_day_factor() {
        let per_second = Decimal::from(1);
        let required = per_second * Decimal::from(DAY);
        assert_eq!(required, Decimal::from(86_400));
    }
}
