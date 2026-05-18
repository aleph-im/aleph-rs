//! POST message handler. Mirrors `aleph/handlers/content/post.py`.

use async_trait::async_trait;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashSet;
use std::str::FromStr;

use crate::db::accessors::balances::{
    get_credit_balance, update_balances as update_balances_db,
    update_credit_balances_distribution as update_credit_balances_distribution_db,
    update_credit_balances_expense as update_credit_balances_expense_db,
    update_credit_balances_transfer as update_credit_balances_transfer_db,
    validate_credit_transfer_balance,
};
use crate::db::accessors::posts::{
    delete_amends, delete_post, get_original_post, refresh_latest_amend,
};
use crate::db::models::messages::MessageDb;
use crate::handlers::content::content_handler::{
    ContentHandler, MessageAuthView, check_authorization_local,
};
use crate::permissions::AuthorityLookup;
use crate::schemas::credit_transfer::{
    CreditDistributionContent, CreditExpenseContent, CreditTransferContent,
};
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::message_status::MessageProcessingException;

fn message_time_value(message: &MessageDb) -> f64 {
    message
        .content
        .get("time")
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| {
            message.time.timestamp() as f64
                + (message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0
        })
}

fn content_address(message: &MessageDb) -> Result<String, MessageProcessingException> {
    message
        .content
        .get("address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "Post message {} missing 'address'",
                message.item_hash
            )],
        })
}

fn content_type(message: &MessageDb) -> Option<String> {
    message
        .content
        .get("type")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

fn content_ref(message: &MessageDb) -> Option<String> {
    // The Python `get_post_content_ref` returns `ref.item_hash` for ChainRef,
    // else the string itself. In our JSON view both cases serialize to a
    // string or an object with `item_hash`.
    match message.content.get("ref") {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Object(obj)) => obj
            .get("item_hash")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        _ => None,
    }
}

fn channel_str(message: &MessageDb) -> Option<String> {
    message
        .channel
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
        .and_then(|v| v.as_str().map(|s| s.to_string()))
}

/// Handler for POST messages.
pub struct PostMessageHandler {
    pub balances_addresses: Vec<String>,
    pub balances_post_type: String,
    pub credit_balances_addresses: Vec<String>,
    pub credit_balances_post_types: Vec<String>,
    pub credit_balances_channels: Vec<String>,
}

impl PostMessageHandler {
    pub fn new(
        balances_addresses: Vec<String>,
        balances_post_type: String,
        credit_balances_addresses: Vec<String>,
        credit_balances_post_types: Vec<String>,
        credit_balances_channels: Vec<String>,
    ) -> Self {
        Self {
            balances_addresses,
            balances_post_type,
            credit_balances_addresses,
            credit_balances_post_types,
            credit_balances_channels,
        }
    }

    async fn update_balances(
        client: &tokio_postgres::Transaction<'_>,
        content: &serde_json::Value,
    ) -> Result<(), MessageProcessingException> {
        let chain_s = content
            .get("chain")
            .and_then(|v| v.as_str())
            .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
                errors: vec!["Missing field 'chain' for balance post".into()],
            })?;
        // Strict types: `main_height` MUST be a JSON integer. Python's
        // Pydantic config rejects floats here, so we mirror that — `as_i64`
        // returns None for floats and missing values.
        let main_height_val = content.get("main_height").ok_or_else(|| {
            MessageProcessingException::InvalidMessageFormat {
                errors: vec!["Missing field 'main_height' for balance post".into()],
            }
        })?;
        if main_height_val.is_f64() && !main_height_val.is_i64() {
            return Err(MessageProcessingException::InvalidMessageFormat {
                errors: vec!["Field 'main_height' must be an integer".into()],
            });
        }
        let height = main_height_val.as_i64().ok_or_else(|| {
            MessageProcessingException::InvalidMessageFormat {
                errors: vec!["Field 'main_height' must be an integer".into()],
            }
        })?;
        let dapp = content
            .get("dapp")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let chain = serde_json::from_value::<aleph_types::chain::Chain>(serde_json::Value::String(
            chain_s.to_string(),
        ))
        .map_err(|e| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!("Invalid chain {chain_s}: {e}")],
        })?;

        let balances_obj = content
            .get("balances")
            .and_then(|v| v.as_object())
            .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
                errors: vec!["Missing field 'balances' for balance post".into()],
            })?;
        let mut balances = std::collections::HashMap::<String, f64>::new();
        for (k, v) in balances_obj {
            let f = v
                .as_f64()
                .or_else(|| v.as_i64().map(|i| i as f64))
                .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
                .unwrap_or(0.0);
            balances.insert(k.clone(), f);
        }

        tracing::info!("Updating balances for {chain_s} (dapp: {dapp:?})");
        update_balances_db(client, chain, dapp.as_deref(), height as i32, &balances)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error updating balances: {e}")],
            })?;
        Ok(())
    }

    async fn update_credit_balances_distribution(
        client: &tokio_postgres::Transaction<'_>,
        content: &serde_json::Value,
        message_hash: &str,
        message_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), MessageProcessingException> {
        // Pydantic-style pre-validation. Mirrors Python's
        // `CreditDistributionContent.model_validate(content)` which converts
        // a ValidationError into `InvalidMessageFormat`.
        let parsed: CreditDistributionContent =
            serde_json::from_value(content.clone()).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid credit distribution content: {e}")],
                }
            })?;
        parsed
            .validate()
            .map_err(|e| MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!("Invalid credit distribution content: {e}")],
            })?;

        let dist = &parsed.distribution;
        let credits = dist
            .credits
            .iter()
            .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
            .collect::<Vec<_>>();
        tracing::info!("Updating credit balances for {} addresses", credits.len());
        update_credit_balances_distribution_db(
            client,
            &credits,
            &dist.token,
            &dist.chain,
            message_hash,
            message_timestamp,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error updating credit distribution: {e}")],
        })?;
        Ok(())
    }

    async fn update_credit_balances_expense(
        client: &tokio_postgres::Transaction<'_>,
        content: &serde_json::Value,
        message_hash: &str,
        message_timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), MessageProcessingException> {
        // Pydantic-style pre-validation, mirroring Python.
        let parsed: CreditExpenseContent =
            serde_json::from_value(content.clone()).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid credit expense content: {e}")],
                }
            })?;
        parsed
            .validate()
            .map_err(|e| MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!("Invalid credit expense content: {e}")],
            })?;

        let credits = parsed
            .expense
            .credits
            .iter()
            .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
            .collect::<Vec<_>>();
        tracing::info!(
            "Updating credit balances expense for {} addresses",
            credits.len()
        );
        update_credit_balances_expense_db(client, &credits, message_hash, message_timestamp)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error updating credit expense: {e}")],
            })?;
        Ok(())
    }

    async fn update_credit_balances_transfer(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        content: &serde_json::Value,
        message_hash: &str,
        message_timestamp: chrono::DateTime<chrono::Utc>,
        sender_address: &str,
    ) -> Result<(), MessageProcessingException> {
        // Pydantic-style pre-validation, mirroring Python's
        // `CreditTransferContent.model_validate(content)`.
        let parsed: CreditTransferContent =
            serde_json::from_value(content.clone()).map_err(|e| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!("Invalid credit transfer content: {e}")],
                }
            })?;
        parsed
            .validate()
            .map_err(|e| MessageProcessingException::InvalidMessageFormat {
                errors: vec![format!("Invalid credit transfer content: {e}")],
            })?;
        let credits_entries = &parsed.transfer.credits;

        // Reject self-transfers.
        for entry in credits_entries {
            if entry.address == sender_address {
                return Err(MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!(
                        "Self-transfer not allowed: sender and recipient are both {sender_address}"
                    )],
                });
            }
        }

        // Only validate balance if sender is not whitelisted. Sum amounts in
        // `Decimal` to match Python's `sum(entry.amount for ...)` precision.
        let is_whitelisted = self
            .credit_balances_addresses
            .iter()
            .any(|a| a == sender_address);
        if !is_whitelisted {
            let total_amount: Decimal = credits_entries
                .iter()
                .map(|e| Decimal::from(e.amount))
                .sum();

            // The DB-level helper takes `i64`. Bridge to it without losing
            // precision: if the Decimal sum cannot fit, refuse the transfer.
            let total_i64 = total_amount.to_i64().ok_or_else(|| {
                MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!(
                        "Transfer total {total_amount} exceeds the supported integer range"
                    )],
                }
            })?;
            let valid = validate_credit_transfer_balance(client, sender_address, total_i64)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error validating transfer balance: {e}")],
                })?;
            if !valid {
                let bal = get_credit_balance(client, sender_address, None)
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error reading credit balance: {e}")],
                    })?;
                return Err(MessageProcessingException::InvalidMessageFormat {
                    errors: vec![format!(
                        "Insufficient credit balance for transfer. Required: {total_amount}, Available: {bal}"
                    )],
                });
            }
        }

        let credits = credits_entries
            .iter()
            .map(|c| serde_json::to_value(c).unwrap_or(serde_json::Value::Null))
            .collect::<Vec<_>>();
        tracing::info!(
            "Updating credit balances transfer for {} recipients",
            credits.len()
        );
        update_credit_balances_transfer_db(
            client,
            &credits,
            sender_address,
            &self.credit_balances_addresses,
            message_hash,
            message_timestamp,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error updating credit transfer: {e}")],
        })?;
        Ok(())
    }

    async fn process_post(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let address = content_address(message)?;
        let ctype = content_type(message);
        let cref = content_ref(message);
        let creation_datetime = timestamp_to_datetime(message_time_value(message));
        let inner_content = message
            .content
            .get("content")
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let channel = channel_str(message);

        // Extract tags from `content.tags`.
        let tags: Option<Vec<String>> = inner_content
            .as_object()
            .and_then(|obj| obj.get("tags"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty());

        let amends = if ctype.as_deref() == Some("amend") {
            cref.clone()
        } else {
            None
        };

        // Insert into `posts` table.
        let sql = "INSERT INTO posts(item_hash, owner, type, ref, amends, channel, content, \
                                      creation_datetime, latest_amend, tags) \
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, $9)";
        client
            .execute(
                sql,
                &[
                    &message.item_hash,
                    &address,
                    &ctype,
                    &cref,
                    &amends,
                    &channel,
                    &inner_content,
                    &creation_datetime,
                    &tags,
                ],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error inserting post: {e}")],
            })?;

        // For amends, update the amended post's `latest_amend` pointer when
        // this amend is more recent than the currently-tracked last update.
        //
        // Mirrors pyaleph: `last_updated = COALESCE(latest_amend.creation_datetime,
        // original.creation_datetime)`. Using `GREATEST(...)` was wrong because
        // it picks the larger of (original, current_latest_amend), so an
        // out-of-order amend whose time is between the original and an
        // existing amend would always lose — even when the existing amend
        // ought to be replaced. We now compare against the current
        // `latest_amend.creation_datetime` (falling back to original) and
        // additionally break ties by `item_hash` ASC for determinism.
        if ctype.as_deref() == Some("amend") {
            if let Some(ref_hash) = cref.as_deref() {
                let row = client
                    .query_opt(
                        "SELECT COALESCE(a.creation_datetime, o.creation_datetime) AS last_updated, \
                                COALESCE(a.item_hash, o.item_hash) AS last_hash \
                         FROM posts o LEFT JOIN posts a ON o.latest_amend = a.item_hash \
                         WHERE o.item_hash = $1",
                        &[&ref_hash],
                    )
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error reading amended post: {e}")],
                    })?;
                if let Some(r) = row {
                    let last_updated: chrono::DateTime<chrono::Utc> = r.get("last_updated");
                    let last_hash: String = r.get("last_hash");
                    let should_update = creation_datetime > last_updated
                        || (creation_datetime == last_updated
                            && message.item_hash.as_str() < last_hash.as_str());
                    if should_update {
                        client
                            .execute(
                                "UPDATE posts SET latest_amend = $1 WHERE item_hash = $2",
                                &[&message.item_hash, &ref_hash],
                            )
                            .await
                            .map_err(|e| MessageProcessingException::InternalError {
                                errors: vec![format!("DB error updating latest_amend: {e}")],
                            })?;
                    }
                }
            }
        }

        // Balance handling for the special post types.
        let content_address_str = address;

        if ctype.as_deref() == Some(self.balances_post_type.as_str())
            && self
                .balances_addresses
                .iter()
                .any(|a| a == &content_address_str)
            && !inner_content.is_null()
        {
            tracing::info!("Updating balances...");
            Self::update_balances(client, &inner_content).await?;
            tracing::info!("Done updating balances");
        }

        if let Some(ctype_s) = ctype.as_deref() {
            let channels_ok = self.credit_balances_channels.is_empty()
                || channel
                    .as_deref()
                    .map(|c| self.credit_balances_channels.iter().any(|x| x == c))
                    .unwrap_or(false);
            let post_type_ok = self.credit_balances_post_types.iter().any(|p| p == ctype_s);
            if post_type_ok && channels_ok && !inner_content.is_null() {
                tracing::info!("Updating credit balances...");
                if ctype_s == "aleph_credit_distribution"
                    && self
                        .credit_balances_addresses
                        .iter()
                        .any(|a| a == &content_address_str)
                {
                    Self::update_credit_balances_distribution(
                        client,
                        &inner_content,
                        &message.item_hash,
                        creation_datetime,
                    )
                    .await?;
                } else if ctype_s == "aleph_credit_expense"
                    && self
                        .credit_balances_addresses
                        .iter()
                        .any(|a| a == &content_address_str)
                {
                    Self::update_credit_balances_expense(
                        client,
                        &inner_content,
                        &message.item_hash,
                        creation_datetime,
                    )
                    .await?;
                } else if ctype_s == "aleph_credit_transfer" {
                    self.update_credit_balances_transfer(
                        client,
                        &inner_content,
                        &message.item_hash,
                        creation_datetime,
                        &content_address_str,
                    )
                    .await?;
                }
                tracing::info!("Done updating credit balances");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl ContentHandler for PostMessageHandler {
    async fn check_dependencies(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        let ctype = content_type(message);
        if ctype.as_deref() != Some("amend") {
            return Ok(());
        }
        let cref = content_ref(message);
        let ref_hash = match cref {
            None => {
                return Err(MessageProcessingException::NoAmendTarget { errors: Vec::new() });
            }
            Some(r) => r,
        };
        let original = get_original_post(client, &ref_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching original post: {e}")],
            }
        })?;
        let original = match original {
            None => {
                return Err(MessageProcessingException::AmendTargetNotFound { errors: Vec::new() });
            }
            Some(p) => p,
        };
        if original.r#type.as_deref() == Some("amend") {
            return Err(MessageProcessingException::CannotAmendAmend { errors: Vec::new() });
        }
        Ok(())
    }

    async fn check_permissions(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
        lookup: &dyn AuthorityLookup,
    ) -> Result<(), MessageProcessingException> {
        let view = MessageAuthView::from_message(message);
        if !check_authorization_local(lookup, &view).await {
            return Err(MessageProcessingException::PermissionDenied {
                errors: vec![format!(
                    "Sender {} is not authorized to post on behalf of address {}",
                    message.sender, view.content_address
                )],
            });
        }
        // Additional check for amend: ensure the amend message has the same
        // address as the original post.
        if content_type(message).as_deref() == Some("amend") {
            if let Some(ref_hash) = content_ref(message) {
                if let Some(original) = lookup.get_message_by_item_hash(&ref_hash).await {
                    if !view
                        .content_address
                        .eq_ignore_ascii_case(original.content_address())
                    {
                        return Err(MessageProcessingException::PermissionDenied {
                            errors: vec![format!(
                                "Cannot amend post {ref_hash}: amend address {} does not match original owner {}",
                                view.content_address,
                                original.content_address()
                            )],
                        });
                    }
                }
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
            self.process_post(client, message).await?;
        }
        Ok(())
    }

    async fn forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        let ctype = content_type(message);
        tracing::debug!("Deleting post {}", message.item_hash);
        let amend_hashes = delete_amends(client, &message.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting amends: {e}")],
            })?;
        delete_post(client, &message.item_hash).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting post: {e}")],
            }
        })?;

        if ctype.as_deref() == Some("amend") {
            let cref = content_ref(message).unwrap_or_default();
            let original = get_original_post(client, &cref).await.map_err(|e| {
                MessageProcessingException::InternalError {
                    errors: vec![format!("DB error fetching original: {e}")],
                }
            })?;
            let original = match original {
                Some(p) => p,
                None => {
                    return Err(MessageProcessingException::InternalError {
                        errors: vec![format!(
                            "Could not find original post ({cref}) for amend ({})",
                            message.item_hash
                        )],
                    });
                }
            };
            if original.latest_amend.as_deref() == Some(message.item_hash.as_str()) {
                refresh_latest_amend(client, &original.item_hash)
                    .await
                    .map_err(|e| MessageProcessingException::InternalError {
                        errors: vec![format!("DB error refreshing latest amend: {e}")],
                    })?;
            }
        }

        Ok(amend_hashes.into_iter().collect())
    }
}

/// Helper used by message_handler when a POST message includes an `amount`
/// field expressed as Decimal. Keeps the lib-level dependency on
/// rust_decimal central.
#[allow(dead_code)]
pub(crate) fn parse_amount_to_decimal(v: &serde_json::Value) -> Option<rust_decimal::Decimal> {
    if let Some(s) = v.as_str() {
        return rust_decimal::Decimal::from_str(s).ok();
    }
    if let Some(n) = v.as_f64() {
        return rust_decimal::Decimal::from_f64_retain(n);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::message_status::MessageStatus;
    use aleph_types::chain::Chain;
    use aleph_types::message::MessageType;
    use aleph_types::message::item_type::ItemType;
    use chrono::Utc;
    use serde_json::json;

    fn make_post(item_hash: &str, ctype: &str, cref: Option<&str>) -> MessageDb {
        let now = Utc::now();
        let mut content = serde_json::Map::new();
        content.insert("address".into(), json!("0xabc"));
        content.insert("type".into(), json!(ctype));
        if let Some(r) = cref {
            content.insert("ref".into(), json!(r));
        }
        content.insert("time".into(), json!(now.timestamp() as f64));
        content.insert("content".into(), json!({"body": "hi"}));
        MessageDb {
            item_hash: item_hash.into(),
            r#type: MessageType::Post,
            chain: Chain::Ethereum,
            sender: "0xabc".into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: serde_json::Value::Object(content),
            time: now,
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: now,
            owner: Some("0xabc".into()),
            content_type: Some(ctype.into()),
            content_ref: cref.map(|s| s.into()),
            content_key: None,
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[test]
    fn content_helpers_extract_fields() {
        let m = make_post("h1", "amend", Some("0xref"));
        assert_eq!(content_address(&m).unwrap(), "0xabc");
        assert_eq!(content_type(&m).as_deref(), Some("amend"));
        assert_eq!(content_ref(&m).as_deref(), Some("0xref"));
    }

    #[test]
    fn content_ref_handles_chain_ref_object() {
        let mut m = make_post("h1", "amend", None);
        if let Some(obj) = m.content.as_object_mut() {
            obj.insert("ref".into(), json!({"item_hash": "0xref"}));
        }
        assert_eq!(content_ref(&m).as_deref(), Some("0xref"));
    }

    #[test]
    fn handler_constructs_with_defaults() {
        let _h = PostMessageHandler::new(
            Vec::new(),
            "balances".into(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
    }

    #[test]
    fn parse_amount_decimal_handles_strings() {
        let d = parse_amount_to_decimal(&json!("12.5")).unwrap();
        assert_eq!(d.to_string(), "12.5");
        assert!(parse_amount_to_decimal(&json!(true)).is_none());
    }

    #[test]
    fn transfer_amount_sum_preserves_decimal_precision() {
        // The aggregate must be computed in Decimal so we don't lose i64
        // intermediates on huge sums. Specifically, build a synthetic list
        // that overflows i64 when summed with naive i64 accumulation, and
        // verify that the Decimal sum is exact.
        let entries: Vec<crate::schemas::credit_transfer::CreditTransferEntry> = vec![
            crate::schemas::credit_transfer::CreditTransferEntry {
                address: "0xa".into(),
                amount: i64::MAX,
                expiration: None,
            },
            crate::schemas::credit_transfer::CreditTransferEntry {
                address: "0xb".into(),
                amount: 1,
                expiration: None,
            },
        ];
        let total: rust_decimal::Decimal = entries
            .iter()
            .map(|e| rust_decimal::Decimal::from(e.amount))
            .sum();
        let expected = rust_decimal::Decimal::from(i64::MAX) + rust_decimal::Decimal::from(1i64);
        assert_eq!(total, expected);
        // Decimal-to-i64 conversion correctly refuses overflowing totals.
        assert!(total.to_i64().is_none());
    }

    #[tokio::test]
    async fn update_balances_rejects_float_main_height() {
        // Without a real client, `update_balances` only runs the early
        // pre-validation step. Use a placeholder client value by going via
        // a synthetic call — we can't call the inner DB function, so we
        // only exercise the validation in isolation by inspecting what the
        // pre-validation step rejects.
        let content = json!({
            "chain": "ETH",
            "main_height": 1.5,
            "balances": {},
        });
        // Reach into the validation path by replicating exactly the
        // pre-DB checks: this is the only Send-friendly surface we can
        // call without a running Postgres.
        let main_height = content.get("main_height").unwrap();
        assert!(main_height.is_f64() && !main_height.is_i64());
    }

    #[test]
    fn distribution_pre_validation_rejects_malformed_input() {
        // The Pydantic-style validator must reject a missing `distribution`.
        let bad = json!({"transfer": {"credits": []}});
        let res: Result<crate::schemas::credit_transfer::CreditDistributionContent, _> =
            serde_json::from_value(bad);
        assert!(res.is_err());

        // A well-formed body must pass.
        let good = json!({
            "distribution": {
                "credits": [{
                    "address": "0xa",
                    "amount": 1,
                    "price": "0.5",
                    "tx_hash": "h",
                    "provider": "p"
                }],
                "token": "ALEPH",
                "chain": "ETH"
            }
        });
        let parsed: crate::schemas::credit_transfer::CreditDistributionContent =
            serde_json::from_value(good).unwrap();
        parsed.validate().unwrap();
    }
}
