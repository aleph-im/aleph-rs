//! A caching decorator for [`AlephAggregateClient`].
//!
//! Several code paths fetch the same aggregate more than once per command: the
//! interactive `instance create` flow, for instance, reads the pricing and
//! settings aggregates in the size/GPU picker, again in the CRN filter, and
//! again during sizing. Wrapping a client in [`CachingAggregateClient`]
//! memoizes every aggregate read so each distinct request is served from the
//! network at most once.
//!
//! # Scope of the cache
//!
//! This is a *short-lived, read-oriented* cache. It memoizes indefinitely and
//! never invalidates, so it must not be reused as a long-lived client that also
//! writes aggregates: a read after a write would return the stale, pre-write
//! value. The intended lifetime is a single command (or a single logical read
//! phase). Only successful reads are cached; a failed fetch is retried.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::sync::Mutex;

use aleph_types::chain::Address;
use serde::de::DeserializeOwned;

use crate::aggregate_models::domains::DomainsAggregate;
use crate::aggregate_models::port_forwarding::PortForwardingAggregate;
use crate::aggregate_models::websites::WebsitesAggregate;
use crate::client::{AlephAggregateClient, MessageError};

/// Wraps an [`AlephAggregateClient`] and memoizes every read after its first
/// success. See the [module docs](self) for the cache's scope and the
/// immutability assumption it relies on.
///
/// Each method keeps its own cache because their key and value shapes differ;
/// there is no cross-population between, say, `get_all_aggregates` and a
/// single-key `get_aggregate`.
pub struct CachingAggregateClient<'a, C: AlephAggregateClient> {
    inner: &'a C,
    /// Single-key reads, keyed by `(address, key)`. Stored as raw JSON and
    /// re-deserialized on each hit so one entry can serve callers that want
    /// different concrete types (this backs `get_pricing_aggregate`,
    /// `get_settings_aggregate`, etc. via the trait's default methods).
    aggregate: Mutex<HashMap<(Address, String), serde_json::Value>>,
    websites: Mutex<HashMap<Address, WebsitesAggregate>>,
    domains: Mutex<HashMap<Address, DomainsAggregate>>,
    port_forwarding: Mutex<HashMap<Address, PortForwardingAggregate>>,
    /// Multi-key reads, keyed by `(address, comma-joined keys)`.
    aggregates: Mutex<HashMap<(Address, String), HashMap<String, serde_json::Value>>>,
    all_aggregates: Mutex<HashMap<Address, HashMap<String, serde_json::Value>>>,
}

impl<'a, C: AlephAggregateClient> CachingAggregateClient<'a, C> {
    /// Wrap `inner`, starting with every cache empty.
    pub fn new(inner: &'a C) -> Self {
        Self {
            inner,
            aggregate: Mutex::new(HashMap::new()),
            websites: Mutex::new(HashMap::new()),
            domains: Mutex::new(HashMap::new()),
            port_forwarding: Mutex::new(HashMap::new()),
            aggregates: Mutex::new(HashMap::new()),
            all_aggregates: Mutex::new(HashMap::new()),
        }
    }
}

fn deserialize<T: DeserializeOwned>(value: serde_json::Value) -> Result<T, MessageError> {
    serde_json::from_value(value).map_err(|e| MessageError::ApiError {
        status: 200,
        body: format!("failed to deserialize cached aggregate: {e}"),
    })
}

/// Return the cached value for `key`, or run `fetch`, store its success, and
/// return it. `fetch` is not run on a hit. Only successful fetches are cached.
///
/// The mutex guard is always released before the `.await`, so the returned
/// future stays `Send`.
async fn get_or_fetch<K, V, F, Fut>(
    store: &Mutex<HashMap<K, V>>,
    key: K,
    fetch: F,
) -> Result<V, MessageError>
where
    K: Eq + Hash + Clone,
    V: Clone,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<V, MessageError>>,
{
    {
        let hit = store
            .lock()
            .expect("aggregate cache mutex poisoned")
            .get(&key)
            .cloned();
        if let Some(value) = hit {
            return Ok(value);
        }
    }
    let value = fetch().await?;
    store
        .lock()
        .expect("aggregate cache mutex poisoned")
        .entry(key)
        .or_insert_with(|| value.clone());
    Ok(value)
}

impl<C: AlephAggregateClient + Sync> AlephAggregateClient for CachingAggregateClient<'_, C> {
    async fn get_aggregate<T: DeserializeOwned>(
        &self,
        address: &Address,
        key: &str,
    ) -> Result<T, MessageError> {
        let value = get_or_fetch(&self.aggregate, (address.clone(), key.to_string()), || {
            self.inner.get_aggregate::<serde_json::Value>(address, key)
        })
        .await?;
        deserialize(value)
    }

    async fn get_websites_aggregate(
        &self,
        address: &Address,
    ) -> Result<WebsitesAggregate, MessageError> {
        get_or_fetch(&self.websites, address.clone(), || {
            self.inner.get_websites_aggregate(address)
        })
        .await
    }

    async fn get_domains_aggregate(
        &self,
        address: &Address,
    ) -> Result<DomainsAggregate, MessageError> {
        get_or_fetch(&self.domains, address.clone(), || {
            self.inner.get_domains_aggregate(address)
        })
        .await
    }

    async fn get_port_forwarding_aggregate(
        &self,
        address: &Address,
    ) -> Result<PortForwardingAggregate, MessageError> {
        get_or_fetch(&self.port_forwarding, address.clone(), || {
            self.inner.get_port_forwarding_aggregate(address)
        })
        .await
    }

    async fn get_aggregates(
        &self,
        address: &Address,
        keys: &[&str],
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        get_or_fetch(&self.aggregates, (address.clone(), keys.join(",")), || {
            self.inner.get_aggregates(address, keys)
        })
        .await
    }

    async fn get_all_aggregates(
        &self,
        address: &Address,
    ) -> Result<HashMap<String, serde_json::Value>, MessageError> {
        get_or_fetch(&self.all_aggregates, address.clone(), || {
            self.inner.get_all_aggregates(address)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// A mock that serves canned payloads and counts how many times each read
    /// hits the "network", so tests can assert the cache collapses repeats. The
    /// first fetch of `fail_first_key` returns an error to exercise the
    /// errors-are-not-cached path.
    #[derive(Default)]
    struct CountingClient {
        pricing_calls: AtomicUsize,
        settings_calls: AtomicUsize,
        all_aggregates_calls: AtomicUsize,
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
                other => unimplemented!("unexpected key {other} in test"),
            };
            deserialize(data)
        }

        async fn get_websites_aggregate(
            &self,
            _address: &Address,
        ) -> Result<WebsitesAggregate, MessageError> {
            unimplemented!()
        }

        async fn get_domains_aggregate(
            &self,
            _address: &Address,
        ) -> Result<DomainsAggregate, MessageError> {
            unimplemented!()
        }

        async fn get_port_forwarding_aggregate(
            &self,
            _address: &Address,
        ) -> Result<PortForwardingAggregate, MessageError> {
            unimplemented!()
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
            self.all_aggregates_calls.fetch_add(1, Ordering::SeqCst);
            Ok(HashMap::new())
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
    async fn caches_delegated_methods_too() {
        let inner = CountingClient::default();
        let client = CachingAggregateClient::new(&inner);

        for _ in 0..3 {
            client.get_all_aggregates(&PRICING_ADDRESS).await.unwrap();
        }

        assert_eq!(inner.all_aggregates_calls.load(Ordering::SeqCst), 1);
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
