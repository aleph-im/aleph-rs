use aleph_types::message::SecurityAggregateContent;
use serde::Deserialize;

/// Wrapper for deserializing the "security" aggregate from the API.
///
/// The aggregate API returns `{ "data": { "security": { "authorizations": [...] } } }`.
/// `get_aggregate::<T>` deserializes `data` as `T`, so `T` must include the key-name wrapper.
#[derive(Debug, Clone, Deserialize)]
pub struct SecurityAggregate {
    #[serde(default)]
    pub security: SecurityAggregateContent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_security_aggregate() {
        // This is what `get_aggregate` receives as the `data` field
        let json = r#"{
            "security": {
                "authorizations": [
                    {
                        "address": "0xdelegate",
                        "types": ["POST"]
                    }
                ]
            }
        }"#;
        let agg: SecurityAggregate = serde_json::from_str(json).unwrap();
        assert_eq!(agg.security.authorizations.len(), 1);
        assert_eq!(
            agg.security.authorizations[0].address.as_str(),
            "0xdelegate"
        );
    }

    #[test]
    fn test_deserialize_empty_security_aggregate() {
        // When the security key exists but has no authorizations
        let json = r#"{"security": {"authorizations": []}}"#;
        let agg: SecurityAggregate = serde_json::from_str(json).unwrap();
        assert!(agg.security.authorizations.is_empty());
    }

    #[test]
    fn test_deserialize_missing_security_key() {
        // When the aggregate data doesn't contain a "security" key at all
        let json = r#"{}"#;
        let agg: SecurityAggregate = serde_json::from_str(json).unwrap();
        assert!(agg.security.authorizations.is_empty());
    }
}
