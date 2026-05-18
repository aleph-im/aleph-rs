//! AGGREGATE message handler. Mirrors
//! `aleph/handlers/content/aggregate.py`.

use async_trait::async_trait;
use std::collections::HashSet;

use crate::db::accessors::aggregates::{
    count_aggregate_elements, delete_aggregate, delete_aggregate_element, get_aggregate_by_key,
    get_aggregate_content_keys, get_aggregate_elements, insert_aggregate, insert_aggregate_element,
    mark_aggregate_as_dirty, merge_aggregate_elements, refresh_aggregate, update_aggregate,
};
use crate::db::models::aggregates::{AggregateDb, AggregateElementDb};
use crate::db::models::messages::MessageDb;
use crate::handlers::content::content_handler::ContentHandler;
use crate::toolkit::timestamp::timestamp_to_datetime;
use crate::types::message_status::MessageProcessingException;

/// Dirty-threshold above which we mark the aggregate as dirty rather than
/// recomputing on the spot. Mirrors the Python `dirty_threshold = 1000`.
const DIRTY_THRESHOLD: i64 = 1000;

/// Magic owner whose aggregates are skipped, mirroring the Python special
/// case for `"0x51A58800b26AA1451aaA803d1746687cB88E0501"`.
const SKIPPED_OWNER: &str = "0x51A58800b26AA1451aaA803d1746687cB88E0501";

fn aggregate_key(message: &MessageDb) -> Result<String, MessageProcessingException> {
    let content = &message.content;
    content
        .get("key")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "Aggregate message {} is missing the 'key' field",
                message.item_hash
            )],
        })
}

fn aggregate_address(message: &MessageDb) -> Result<String, MessageProcessingException> {
    let content = &message.content;
    content
        .get("address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| MessageProcessingException::InvalidMessageFormat {
            errors: vec![format!(
                "Aggregate message {} is missing the 'address' field",
                message.item_hash
            )],
        })
}

fn aggregate_content(message: &MessageDb) -> Result<serde_json::Value, MessageProcessingException> {
    Ok(message
        .content
        .get("content")
        .cloned()
        .unwrap_or(serde_json::Value::Null))
}

fn message_time_value(message: &MessageDb) -> f64 {
    message
        .content
        .get("time")
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| {
            let ts = message.time.timestamp() as f64
                + (message.time.timestamp_subsec_nanos() as f64) / 1_000_000_000.0;
            ts
        })
}

/// Aggregate message handler.
pub struct AggregateMessageHandler;

impl AggregateMessageHandler {
    pub fn new() -> Self {
        Self
    }

    async fn insert_aggregate_element_for(
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<AggregateElementDb, MessageProcessingException> {
        let key = aggregate_key(message)?;
        let owner = aggregate_address(message)?;
        let content = aggregate_content(message)?;
        let creation_datetime = timestamp_to_datetime(message_time_value(message));

        let element = AggregateElementDb {
            item_hash: message.item_hash.clone(),
            key: key.clone(),
            owner: owner.clone(),
            content: content.clone(),
            creation_datetime,
        };

        insert_aggregate_element(
            client,
            &element.item_hash,
            &element.key,
            &element.owner,
            &element.content,
            element.creation_datetime,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error inserting aggregate element: {e}")],
        })?;

        Ok(element)
    }

    async fn append_to_aggregate(
        client: &tokio_postgres::Transaction<'_>,
        aggregate: &AggregateDb,
        elements: &[AggregateElementDb],
    ) -> Result<(), MessageProcessingException> {
        let merged = merge_aggregate_elements(elements.iter());
        let content_val = serde_json::Value::Object(merged);
        let last_revision_hash = elements
            .last()
            .map(|e| e.item_hash.clone())
            .unwrap_or_else(|| aggregate.last_revision_hash.clone());
        update_aggregate(
            client,
            &aggregate.key,
            &aggregate.owner,
            &content_val,
            aggregate.creation_datetime,
            &last_revision_hash,
            false,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error appending to aggregate: {e}")],
        })?;
        Ok(())
    }

    async fn prepend_to_aggregate(
        client: &tokio_postgres::Transaction<'_>,
        aggregate: &AggregateDb,
        elements: &[AggregateElementDb],
    ) -> Result<(), MessageProcessingException> {
        let merged = merge_aggregate_elements(elements.iter());
        let content_val = serde_json::Value::Object(merged);
        let creation_datetime = elements
            .first()
            .map(|e| e.creation_datetime)
            .unwrap_or(aggregate.creation_datetime);
        update_aggregate(
            client,
            &aggregate.key,
            &aggregate.owner,
            &content_val,
            creation_datetime,
            &aggregate.last_revision_hash,
            true,
        )
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error prepending to aggregate: {e}")],
        })?;
        Ok(())
    }

    async fn update_aggregate(
        client: &tokio_postgres::Transaction<'_>,
        key: &str,
        owner: &str,
        elements: &[AggregateElementDb],
    ) -> Result<(), MessageProcessingException> {
        if owner == SKIPPED_OWNER {
            return Ok(());
        }
        if elements.is_empty() {
            return Ok(());
        }

        let aggregate_metadata = get_aggregate_by_key(client, owner, key, false)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching aggregate metadata: {e}")],
            })?;

        let aggregate_metadata = match aggregate_metadata {
            None => {
                tracing::info!("{key}/{owner} does not exist, creating it");
                let merged = merge_aggregate_elements(elements.iter());
                let content_val = serde_json::Value::Object(merged);
                let creation_datetime = elements[0].creation_datetime;
                let last_revision_hash = elements.last().unwrap().item_hash.clone();
                insert_aggregate(
                    client,
                    key,
                    owner,
                    &content_val,
                    creation_datetime,
                    &last_revision_hash,
                )
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error inserting aggregate: {e}")],
                })?;
                return Ok(());
            }
            Some(agg) => agg,
        };

        if aggregate_metadata.dirty {
            tracing::info!("{owner}/{key} is dirty, skipping update");
            return Ok(());
        }

        tracing::info!("{owner}/{key} already exists, updating it");

        // Locate the last revision creation_datetime by reading the element row
        // identified by `last_revision_hash`. Mirrors Python's
        // `aggregate_metadata.last_revision.creation_datetime`.
        let last_revision = client
            .query_opt(
                "SELECT item_hash, key, owner, content, creation_datetime \
                 FROM aggregate_elements WHERE item_hash = $1",
                &[&aggregate_metadata.last_revision_hash],
            )
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching last revision: {e}")],
            })?;
        let last_revision = match last_revision.as_ref().map(AggregateElementDb::from_row) {
            Some(r) => r,
            None => {
                // Inconsistent state — fall back to a full refresh.
                refresh_aggregate(client, owner, key).await.map_err(|e| {
                    MessageProcessingException::InternalError {
                        errors: vec![format!("DB error refreshing aggregate: {e}")],
                    }
                })?;
                return Ok(());
            }
        };

        // Best case: all new elements are posterior to the last revision.
        if last_revision.creation_datetime < elements[0].creation_datetime {
            return Self::append_to_aggregate(client, &aggregate_metadata, elements).await;
        }

        // Similar case: all new elements are anterior to the aggregate.
        if aggregate_metadata.creation_datetime > elements.last().unwrap().creation_datetime {
            return Self::prepend_to_aggregate(client, &aggregate_metadata, elements).await;
        }

        tracing::info!("{owner}/{key}: out of order refresh");

        // Check for key conflicts before resorting to a full refresh.
        let existing_keys: HashSet<String> = get_aggregate_content_keys(client, owner, key)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error fetching aggregate keys: {e}")],
            })?
            .into_iter()
            .collect();
        let new_keys: HashSet<String> = elements
            .iter()
            .flat_map(|e| {
                e.content
                    .as_object()
                    .map(|m| m.keys().cloned().collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect();
        let conflicting: HashSet<&String> = existing_keys.intersection(&new_keys).collect();

        if conflicting.is_empty() {
            tracing::info!("No conflicting keys for {owner}/{key}, updating it");
            return Self::append_to_aggregate(client, &aggregate_metadata, elements).await;
        }

        // Special case: if the last revision overwrote all the keys, the
        // overlap doesn't matter.
        let last_revision_keys: HashSet<String> = last_revision
            .content
            .as_object()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();
        let keys_requiring_refresh: HashSet<&String> =
            new_keys.difference(&last_revision_keys).collect();
        if keys_requiring_refresh.is_empty() {
            tracing::info!("Outdated info, skipping refresh for {owner}/{key}");
            return Ok(());
        }

        let count = count_aggregate_elements(client, owner, key)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error counting aggregate elements: {e}")],
            })?;
        if count > DIRTY_THRESHOLD {
            tracing::info!("{owner}/{key}: too many elements, marking as dirty");
            mark_aggregate_as_dirty(client, owner, key)
                .await
                .map_err(|e| MessageProcessingException::InternalError {
                    errors: vec![format!("DB error marking aggregate dirty: {e}")],
                })?;
            return Ok(());
        }

        refresh_aggregate(client, owner, key).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error refreshing aggregate: {e}")],
            }
        })?;
        Ok(())
    }
}

impl Default for AggregateMessageHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ContentHandler for AggregateMessageHandler {
    async fn fetch_related_content(
        &self,
        _client: &tokio_postgres::Transaction<'_>,
        _message: &MessageDb,
    ) -> Result<(), MessageProcessingException> {
        // Aggregates are independent of one another — nothing to do.
        Ok(())
    }

    async fn process(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        messages: &[MessageDb],
    ) -> Result<(), MessageProcessingException> {
        // Sort by (key, address, time) and group by (key, address) just like
        // Python's `sorted(... key=lambda m: (m.parsed_content.key, .address, .time))`.
        //
        // Python uses `m.time` (the message-row time, i.e. `messages.time`),
        // NOT `m.parsed_content.time`. They diverge for replayed messages
        // whose reception timestamp differs from the wire-claimed timestamp.
        // Using `content.time` here would order out-of-order arrivals
        // differently from pyaleph and break aggregate convergence.
        let mut indexed: Vec<(usize, String, String, chrono::DateTime<chrono::Utc>)> = messages
            .iter()
            .enumerate()
            .map(|(idx, m)| {
                let key = m
                    .content
                    .get("key")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let address = m
                    .content
                    .get("address")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                (idx, key, address, m.time)
            })
            .collect();
        indexed.sort_by(|a, b| a.1.cmp(&b.1).then(a.2.cmp(&b.2)).then(a.3.cmp(&b.3)));

        let mut group_start = 0usize;
        while group_start < indexed.len() {
            let (key, owner) = (
                indexed[group_start].1.clone(),
                indexed[group_start].2.clone(),
            );
            let mut group_end = group_start + 1;
            while group_end < indexed.len()
                && indexed[group_end].1 == key
                && indexed[group_end].2 == owner
            {
                group_end += 1;
            }

            let mut aggregate_elements: Vec<AggregateElementDb> = Vec::new();
            for entry in &indexed[group_start..group_end] {
                let m = &messages[entry.0];
                let element = Self::insert_aggregate_element_for(client, m).await?;
                aggregate_elements.push(element);
            }

            Self::update_aggregate(client, &key, &owner, &aggregate_elements).await?;
            group_start = group_end;
        }
        Ok(())
    }

    async fn forget_message(
        &self,
        client: &tokio_postgres::Transaction<'_>,
        message: &MessageDb,
    ) -> Result<HashSet<String>, MessageProcessingException> {
        let key = aggregate_key(message)?;
        let owner = aggregate_address(message)?;
        tracing::debug!("Deleting aggregate element {}", message.item_hash);
        delete_aggregate(client, &owner, &key).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting aggregate: {e}")],
            }
        })?;
        delete_aggregate_element(client, &message.item_hash)
            .await
            .map_err(|e| MessageProcessingException::InternalError {
                errors: vec![format!("DB error deleting aggregate element: {e}")],
            })?;
        tracing::debug!("Refreshing aggregate {owner}/{key}");
        refresh_aggregate(client, &owner, &key).await.map_err(|e| {
            MessageProcessingException::InternalError {
                errors: vec![format!("DB error refreshing aggregate: {e}")],
            }
        })?;
        Ok(HashSet::new())
    }
}

/// Stable helper used by tests to fetch the elements of an aggregate.
#[allow(dead_code)]
pub(crate) async fn list_aggregate_elements(
    client: &tokio_postgres::Transaction<'_>,
    owner: &str,
    key: &str,
) -> Result<Vec<AggregateElementDb>, MessageProcessingException> {
    get_aggregate_elements(client, owner, key)
        .await
        .map_err(|e| MessageProcessingException::InternalError {
            errors: vec![format!("DB error listing aggregate elements: {e}")],
        })
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

    fn make_message(
        item_hash: &str,
        key: &str,
        address: &str,
        content: serde_json::Value,
    ) -> MessageDb {
        let time = Utc::now();
        MessageDb {
            item_hash: item_hash.into(),
            r#type: MessageType::Aggregate,
            chain: Chain::Ethereum,
            sender: address.into(),
            signature: None,
            item_type: ItemType::Inline,
            item_content: None,
            content: json!({
                "address": address,
                "key": key,
                "time": time.timestamp() as f64,
                "content": content,
            }),
            time,
            channel: None,
            size: 0,
            status_value: MessageStatus::Processed,
            reception_time: time,
            owner: Some(address.into()),
            content_type: None,
            content_ref: None,
            content_key: Some(key.into()),
            first_confirmed_at: None,
            first_confirmed_height: None,
            payment_type: None,
            content_item_hash: None,
            tags: None,
        }
    }

    #[test]
    fn aggregate_key_returns_field() {
        let msg = make_message("h1", "profile", "0xabc", json!({"a": 1}));
        assert_eq!(aggregate_key(&msg).unwrap(), "profile");
        assert_eq!(aggregate_address(&msg).unwrap(), "0xabc");
    }

    #[test]
    fn aggregate_key_missing_raises_invalid_format() {
        let mut msg = make_message("h1", "k", "0xabc", json!({}));
        // Drop the `key` field.
        if let Some(obj) = msg.content.as_object_mut() {
            obj.remove("key");
        }
        let err = aggregate_key(&msg).unwrap_err();
        assert!(matches!(
            err,
            MessageProcessingException::InvalidMessageFormat { .. }
        ));
    }

    #[test]
    fn handler_default_constructs() {
        let _h = AggregateMessageHandler::default();
    }
}
