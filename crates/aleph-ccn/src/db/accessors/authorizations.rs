//! Authorization-aggregate accessors. Mirrors
//! `aleph/db/accessors/authorizations.py`.

use std::collections::HashMap;

use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::AlephResult;

/// Get the security aggregate content for an owner.
///
/// Mirrors `get_granted_authorizations`.
pub async fn get_granted_authorizations(
    client: &impl GenericClient,
    owner: &str,
) -> AlephResult<Option<Value>> {
    let row = client
        .query_opt(
            "SELECT content FROM aggregates WHERE key = 'security' AND owner = $1",
            &[&owner],
        )
        .await?;
    Ok(row.map(|r| r.get::<_, Value>("content")))
}

/// Reverse lookup: find all security aggregates that grant permissions to `address`.
///
/// Mirrors `get_received_authorizations`. Returns `(owner, matching_entries)`
/// pairs where each entry is the original authorization minus the redundant
/// `address` field.
pub async fn get_received_authorizations(
    client: &impl GenericClient,
    address: &str,
) -> AlephResult<Vec<(String, Vec<Value>)>> {
    // The GIN-indexed containment query: content->'authorizations' @> [{"address": ...}]
    let containment = serde_json::json!([{"address": address}]);
    let sql = "SELECT owner, content FROM aggregates \
               WHERE key = 'security' \
                 AND content->'authorizations' @> $1::jsonb \
               ORDER BY owner";
    let rows = client.query(sql, &[&containment]).await?;
    let mut results: Vec<(String, Vec<Value>)> = Vec::new();
    for row in rows {
        let owner: String = row.get("owner");
        let content: Value = row.get("content");
        let all_auths = content
            .get("authorizations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let matching: Vec<Value> = all_auths
            .iter()
            .filter(|auth| {
                auth.get("address")
                    .and_then(|v| v.as_str())
                    .map(|s| s == address)
                    .unwrap_or(false)
            })
            .map(|auth| {
                if let Some(obj) = auth.as_object() {
                    let mut clone = obj.clone();
                    clone.remove("address");
                    Value::Object(clone)
                } else {
                    auth.clone()
                }
            })
            .collect();
        if !matching.is_empty() {
            results.push((owner, matching));
        }
    }
    Ok(results)
}

/// Filter applied to grouped authorization entries before pagination.
#[derive(Debug, Clone, Default)]
pub struct AuthFilter<'a> {
    pub channels: Option<&'a [String]>,
    pub types: Option<&'a [String]>,
    pub post_types: Option<&'a [String]>,
    pub chains: Option<&'a [String]>,
    pub aggregate_keys: Option<&'a [String]>,
}

fn list_overlaps(a: &[Value], b: &[String]) -> bool {
    a.iter().any(|x| {
        if let Some(s) = x.as_str() {
            b.iter().any(|y| y == s)
        } else {
            false
        }
    })
}

fn entry_matches(entry: &Value, filter: &AuthFilter<'_>) -> bool {
    let obj = match entry.as_object() {
        Some(o) => o,
        None => return true,
    };
    let empty: Vec<Value> = Vec::new();
    let get_list = |k: &str| -> &[Value] {
        obj.get(k)
            .and_then(|v| v.as_array())
            .map(|a| a.as_slice())
            .unwrap_or(&empty)
    };

    if let Some(channels) = filter.channels {
        let entry_channels = get_list("channels");
        if !entry_channels.is_empty() && !list_overlaps(entry_channels, channels) {
            return false;
        }
    }
    if let Some(types) = filter.types {
        let entry_types = get_list("types");
        if !entry_types.is_empty() && !list_overlaps(entry_types, types) {
            return false;
        }
    }
    if let Some(post_types) = filter.post_types {
        let entry_post_types = get_list("post_types");
        if !entry_post_types.is_empty() && !list_overlaps(entry_post_types, post_types) {
            return false;
        }
    }
    if let Some(chains) = filter.chains {
        if let Some(entry_chain) = obj.get("chain").and_then(|v| v.as_str()) {
            if !entry_chain.is_empty() && !chains.iter().any(|c| c == entry_chain) {
                return false;
            }
        }
    }
    if let Some(aggregate_keys) = filter.aggregate_keys {
        let entry_akeys = get_list("aggregate_keys");
        if !entry_akeys.is_empty() && !list_overlaps(entry_akeys, aggregate_keys) {
            return false;
        }
    }
    true
}

/// Filter authorization entries and drop addresses with no remaining entries.
///
/// Mirrors `filter_authorizations`.
pub fn filter_authorizations(
    grouped_auths: &HashMap<String, Vec<Value>>,
    filter: &AuthFilter<'_>,
) -> HashMap<String, Vec<Value>> {
    let mut result = HashMap::new();
    for (address, entries) in grouped_auths {
        let filtered: Vec<Value> = entries
            .iter()
            .filter(|e| entry_matches(e, filter))
            .cloned()
            .collect();
        if !filtered.is_empty() {
            result.insert(address.clone(), filtered);
        }
    }
    result
}

/// Paginate grouped authorizations by their address keys. Mirrors Python's
/// `paginate_authorizations`. Keys are taken in insertion order — callers that
/// need deterministic ordering should pre-sort the input.
pub fn paginate_authorizations(
    grouped_auths: &HashMap<String, Vec<Value>>,
    page: i64,
    pagination: i64,
) -> (HashMap<String, Vec<Value>>, usize) {
    let total = grouped_auths.len();
    let mut keys: Vec<&String> = grouped_auths.keys().collect();
    keys.sort();
    let start = ((page - 1).max(0) as usize).saturating_mul(pagination.max(0) as usize);
    let end = start
        .saturating_add(pagination.max(0) as usize)
        .min(keys.len());
    let mut out = HashMap::new();
    if start < end {
        for k in &keys[start..end] {
            out.insert((*k).clone(), grouped_auths[*k].clone());
        }
    }
    (out, total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn auths() -> HashMap<String, Vec<Value>> {
        let mut m = HashMap::new();
        m.insert(
            "owner1".to_string(),
            vec![json!({"channels": ["A"]}), json!({"types": ["POST"]})],
        );
        m.insert(
            "owner2".to_string(),
            vec![json!({"channels": ["B"], "types": ["AGGREGATE"]})],
        );
        m
    }

    #[test]
    fn filter_by_channel() {
        let a = auths();
        let chans = vec!["A".to_string()];
        let f = AuthFilter {
            channels: Some(&chans),
            ..Default::default()
        };
        let out = filter_authorizations(&a, &f);
        // owner1 has one match on channel A and one entry with no channel
        // (the `types` entry passes because it lacks the channels filter
        // field — Python semantics: missing field = unrestricted).
        assert_eq!(out["owner1"].len(), 2);
        assert!(!out.contains_key("owner2"));
    }

    #[test]
    fn filter_empty_removes_address() {
        let a = auths();
        let chans = vec!["Z".to_string()];
        let f = AuthFilter {
            channels: Some(&chans),
            ..Default::default()
        };
        let out = filter_authorizations(&a, &f);
        assert!(out["owner1"].len() == 1); // the entry without channels passes
        assert!(!out.contains_key("owner2"));
    }

    #[test]
    fn paginate_basic() {
        let mut m = HashMap::new();
        for i in 0..5 {
            m.insert(format!("a{i}"), vec![json!(i)]);
        }
        let (page, total) = paginate_authorizations(&m, 1, 2);
        assert_eq!(total, 5);
        assert_eq!(page.len(), 2);
    }
}
