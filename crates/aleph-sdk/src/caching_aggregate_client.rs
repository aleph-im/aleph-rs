//! A caching decorator for [`AlephAggregateClient`].
//!
//! Several code paths fetch the same aggregate more than once per command: the
//! interactive `instance create` flow, for instance, reads the pricing and
//! settings aggregates in the size/GPU picker, again in the CRN filter, and
//! again during sizing. Wrapping a client in [`CachingAggregateClient`]
//! memoizes single-key aggregate reads so each is fetched at most once.
//!
//! Every aggregate is identified by `(address, key)`, and the typed convenience
//! methods (`get_pricing_aggregate`, `get_domains_aggregate`, ...) are just
//! [`get_aggregate`] with a fixed key, so caching that one method behind a
//! single `(address, key)` map transparently covers all of them.
//!
//! # Scope of the cache
//!
//! This is a *short-lived, read-oriented* cache. It memoizes indefinitely and
//! never invalidates, so it must not be reused as a long-lived client that also
//! writes aggregates: a read after a write would return the stale, pre-write
//! value. The intended lifetime is a single command (or a single logical read
//! phase). Only successful reads are cached; a failed fetch is retried.
//!
//! The multi-key / whole-namespace reads ([`get_aggregates`] and
//! [`get_all_aggregates`]) are batch operations over a set of keys, not a single
//! `(address, key)`, so they cannot be served from this cache and delegate to
//! the inner client unchanged.
//!
//! [`get_aggregate`]: AlephAggregateClient::get_aggregate
//! [`get_aggregates`]: AlephAggregateClient::get_aggregates
//! [`get_all_aggregates`]: AlephAggregateClient::get_all_aggregates

use std::collections::HashMap;
use std::sync::Mutex;

use aleph_types::chain::Address;
use serde::de::DeserializeOwned;

use crate::client::{AlephAggregateClient, MessageError};

/// Wraps an [`AlephAggregateClient`] and memoizes single-key `get_aggregate`
/// reads after their first success. See the [module docs](self) for the cache's
/// scope and the immutability assumption it relies on.
pub struct CachingAggregateClient<'a, C: AlephAggregateClient> {
    inner: &'a C,
    /// Raw aggregate `data` payloads keyed by `(address, key)`. Stored as JSON
    /// and re-deserialized on each hit so a single cached entry can serve
    /// callers that want different concrete types.
    cache: Mutex<HashMap<(Address, String), serde_json::Value>>,
}

impl<'a, C: AlephAggregateClient> CachingAggregateClient<'a, C> {
    /// Wrap `inner`, starting with an empty cache.
    pub fn new(inner: &'a C) -> Self {
        Self {
            inner,
            cache: Mutex::new(HashMap::new()),
        }
    }
}

fn deserialize<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, MessageError> {
    serde_json::from_value(value).map_err(|e| MessageError::ApiError {
        status: 200,
        body: format!("failed to deserialize cached aggregate: {e}"),
    })
}

impl<C: AlephAggregateClient + Sync> AlephAggregateClient for CachingAggregateClient<'_, C> {
    async fn get_aggregate<T: DeserializeOwned>(
        &self,
        address: &Address,
        key: &str,
    ) -> Result<T, MessageError> {
        let cache_key = (address.clone(), key.to_string());

        // The guard is dropped before every `.await`, so the returned future
        // stays `Send`.
        if let Some(value) = self
            .cache
            .lock()
            .expect("aggregate cache mutex poisoned")
            .get(&cache_key)
            .cloned()
        {
            return deserialize(value);
        }

        let value: serde_json::Value = self.inner.get_aggregate(address, key).await?;
        self.cache
            .lock()
            .expect("aggregate cache mutex poisoned")
            .entry(cache_key)
            .or_insert_with(|| value.clone());
        deserialize(value)
    }

    // get_pricing_aggregate, get_settings_aggregate, get_vm_images_aggregate,
    // get_corechannel_aggregate and the per-address get_{websites,domains,
    // port_forwarding}_aggregate are all trait default methods over
    // get_aggregate, so they are cached by the override above with no extra code.

    async fn get_aggregates(
        &self,
        address: &Address,
        keys: &[&str],
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        self.inner.get_aggregates(address, keys).await
    }

    async fn get_all_aggregates(
        &self,
        address: &Address,
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        self.inner.get_all_aggregates(address).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate_models::domains::{DOMAINS_AGGREGATE_KEY, DomainsAggregate};
    use crate::aggregate_models::pricing::PRICING_ADDRESS;
    use crate::aggregate_models::settings::SETTINGS_KEY;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn pricing_data() -> serde_json::Value {
        let entity = serde_json::json!({
            "compute_unit": { "vcpus": 1, "memory_mib": 2048, "disk_mib": 20480 },
            "tiers": [],
            "price": {},
        });
        serde_json::json!({
            "pricing": {
                "instance": entity,
                "instance_confidential": entity,
                "instance_gpu_standard": entity,
                "instance_gpu_premium": entity,
            }
        })
    }

    /// A mock that serves canned aggregate `data` payloads and counts how many
    /// times each key is fetched, so tests can assert the cache collapses repeat
    /// reads. The first fetch of `fail_first_key` returns an error to exercise
    /// the errors-are-not-cached path. `get_aggregates` / `get_all_aggregates`
    /// are left unimplemented as the caching tests never call them.
    #[derive(Default)]
    struct CountingClient {
        pricing_calls: AtomicUsize,
        settings_calls: AtomicUsize,
        domains_calls: AtomicUsize,
        fail_first_key: Option<&'static str>,
    }

    impl AlephAggregateClient for CountingClient {
        async fn get_aggregate<T: DeserializeOwned>(
            &self,
            _address: &Address,
            key: &str,
        ) -> Result<T, MessageError> {
            let prior = match key {
                "pricing" => self.pricing_calls.fetch_add(1, Ordering::SeqCst),
                SETTINGS_KEY => self.settings_calls.fetch_add(1, Ordering::SeqCst),
                DOMAINS_AGGREGATE_KEY => self.domains_calls.fetch_add(1, Ordering::SeqCst),
                other => unimplemented!("unexpected key {other} in test"),
            };
            if self.fail_first_key == Some(key) && prior == 0 {
                return Err(MessageError::ApiError {
                    status: 503,
                    body: "aggregate unavailable".to_string(),
                });
            }
            let data = match key {
                "pricing" => pricing_data(),
                SETTINGS_KEY => serde_json::json!({ "settings": { "compatible_gpus": [] } }),
                DOMAINS_AGGREGATE_KEY => serde_json::json!({ "domains": {} }),
                other => unimplemented!("unexpected key {other} in test"),
            };
            deserialize(data)
        }

        async fn get_aggregates(
            &self,
            _address: &Address,
            _keys: &[&str],
        ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
            unimplemented!()
        }

        async fn get_all_aggregates(
            &self,
            _address: &Address,
        ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn fetches_each_aggregate_once_across_repeated_calls() {
        let inner = CountingClient::default();
        let client = CachingAggregateClient::new(&inner);

        for _ in 0..3 {
            client.get_pricing_aggregate().await.unwrap();
        }
        for _ in 0..3 {
            client.get_settings_aggregate().await.unwrap();
        }
        // A raw single-key read shares the entry the typed method populated.
        let _raw: serde_json::Value = client
            .get_aggregate(&PRICING_ADDRESS, "pricing")
            .await
            .unwrap();

        assert_eq!(inner.pricing_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.settings_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn caches_per_address_convenience_methods_via_the_single_cache() {
        let inner = CountingClient::default();
        let client = CachingAggregateClient::new(&inner);

        // Domains are a per-address aggregate; use an arbitrary user address.
        let address = aleph_types::address!("0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10");
        // get_domains_aggregate is a trait default over get_aggregate, so it is
        // memoized by the same cache with no dedicated handling.
        for _ in 0..3 {
            let _: DomainsAggregate = client.get_domains_aggregate(&address).await.unwrap();
        }

        assert_eq!(inner.domains_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_cache_a_failed_fetch() {
        let inner = CountingClient {
            fail_first_key: Some(SETTINGS_KEY),
            ..Default::default()
        };
        let client = CachingAggregateClient::new(&inner);

        // First fetch fails and must not be cached; the second retries and
        // succeeds, then caches.
        assert!(client.get_settings_aggregate().await.is_err());
        assert!(client.get_settings_aggregate().await.is_ok());
        client.get_settings_aggregate().await.unwrap();

        assert_eq!(inner.settings_calls.load(Ordering::SeqCst), 2);
    }
}
