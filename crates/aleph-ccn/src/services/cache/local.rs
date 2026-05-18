//! Process-local keyed cache. Mirrors `aleph/cache.py`.
//!
//! Used by ORM-style accessors that can't be `async` (and therefore can't use
//! Redis directly). Keys are arbitrary, namespaced strings; values are JSON
//! blobs so the cache can hold any serializable payload without locking the
//! consumer into a specific type.

use std::collections::HashMap;
use std::sync::RwLock;

use once_cell::sync::Lazy;
use serde_json::Value;

#[derive(Debug, Default)]
pub struct LocalCache {
    inner: RwLock<HashMap<String, HashMap<String, Value>>>,
}

impl LocalCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, key: &str, namespace: &str) -> Option<Value> {
        self.inner
            .read()
            .ok()
            .and_then(|g| g.get(namespace)?.get(key).cloned())
    }

    pub fn set(&self, key: &str, value: Value, namespace: &str) {
        let mut g = self.inner.write().unwrap();
        g.entry(namespace.to_string())
            .or_default()
            .insert(key.to_string(), value);
    }

    pub fn exists(&self, key: &str, namespace: &str) -> bool {
        self.inner
            .read()
            .ok()
            .and_then(|g| g.get(namespace).map(|ns| ns.contains_key(key)))
            .unwrap_or(false)
    }

    pub fn delete(&self, key: &str, namespace: &str) {
        if let Ok(mut g) = self.inner.write() {
            if let Some(ns) = g.get_mut(namespace) {
                ns.remove(key);
            }
        }
    }

    pub fn delete_namespace(&self, namespace: &str) {
        if let Ok(mut g) = self.inner.write() {
            if let Some(ns) = g.get_mut(namespace) {
                ns.clear();
            }
        }
    }
}

/// Process-wide singleton matching Python's `aleph.cache.cache`.
pub static GLOBAL_CACHE: Lazy<LocalCache> = Lazy::new(LocalCache::new);

/// `LocalCache` doubles as an in-process [`ApiServerLookup`] for tests that
/// don't want a real Redis. The API-server list is stored under namespace
/// `"node_cache"` / key `"api_servers"` as a JSON array of strings.
#[async_trait::async_trait]
impl crate::services::p2p::jobs::ApiServerLookup for LocalCache {
    async fn get_api_servers(&self) -> crate::AlephResult<Vec<String>> {
        match self.get("api_servers", "node_cache") {
            Some(Value::Array(arr)) => Ok(arr
                .into_iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()),
            _ => Ok(Vec::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_get_roundtrip() {
        let c = LocalCache::new();
        c.set("k", json!({"a": 1}), "ns");
        assert_eq!(c.get("k", "ns"), Some(json!({"a": 1})));
    }

    #[test]
    fn exists_after_set() {
        let c = LocalCache::new();
        assert!(!c.exists("k", "ns"));
        c.set("k", json!(null), "ns");
        assert!(c.exists("k", "ns"));
    }

    #[test]
    fn delete_clears_key() {
        let c = LocalCache::new();
        c.set("k", json!(42), "ns");
        c.delete("k", "ns");
        assert!(!c.exists("k", "ns"));
    }

    #[test]
    fn namespace_isolation() {
        let c = LocalCache::new();
        c.set("k", json!(1), "ns1");
        c.set("k", json!(2), "ns2");
        assert_eq!(c.get("k", "ns1"), Some(json!(1)));
        assert_eq!(c.get("k", "ns2"), Some(json!(2)));
    }

    #[test]
    fn delete_namespace_clears_all() {
        let c = LocalCache::new();
        c.set("a", json!(1), "ns");
        c.set("b", json!(2), "ns");
        c.delete_namespace("ns");
        assert!(!c.exists("a", "ns"));
        assert!(!c.exists("b", "ns"));
    }
}
