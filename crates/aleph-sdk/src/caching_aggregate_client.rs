//! A caching decorator for [`AlephAggregateClient`].
//!
//! The pricing and settings aggregates are network-wide constants for the
//! duration of a CLI invocation, yet several independent code paths fetch them
//! separately (GPU sizing, the interactive picker, the CRN filter). Wrapping a
//! client in [`CachingAggregateClient`] memoizes those two aggregates so they
//! are fetched at most once, while every other method delegates straight to the
//! inner client.

use std::collections::HashMap;

use aleph_types::chain::Address;
use serde::de::DeserializeOwned;
use tokio::sync::OnceCell;

use crate::aggregate_models::domains::DomainsAggregate;
use crate::aggregate_models::port_forwarding::PortForwardingAggregate;
use crate::aggregate_models::pricing::PricingAggregate;
use crate::aggregate_models::settings::SettingsAggregate;
use crate::aggregate_models::websites::WebsitesAggregate;
use crate::client::{AlephAggregateClient, MessageError};

/// Wraps an [`AlephAggregateClient`] and memoizes the pricing and settings
/// aggregates after their first successful fetch. Every other method delegates
/// to the inner client unchanged.
///
/// Only successful responses are cached: if a fetch fails, the next call
/// retries the inner client rather than replaying the error.
pub struct CachingAggregateClient<'a, C: AlephAggregateClient> {
    inner: &'a C,
    pricing: OnceCell<PricingAggregate>,
    settings: OnceCell<SettingsAggregate>,
}

impl<'a, C: AlephAggregateClient> CachingAggregateClient<'a, C> {
    /// Wrap `inner`, starting with both caches empty.
    pub fn new(inner: &'a C) -> Self {
        Self {
            inner,
            pricing: OnceCell::new(),
            settings: OnceCell::new(),
        }
    }
}

impl<C: AlephAggregateClient + Sync> AlephAggregateClient for CachingAggregateClient<'_, C> {
    async fn get_aggregate<T: DeserializeOwned>(
        &self,
        address: &Address,
        key: &str,
    ) -> Result<T, MessageError> {
        self.inner.get_aggregate(address, key).await
    }

    async fn get_pricing_aggregate(&self) -> Result<PricingAggregate, MessageError> {
        self.pricing
            .get_or_try_init(|| self.inner.get_pricing_aggregate())
            .await
            .cloned()
    }

    async fn get_settings_aggregate(&self) -> Result<SettingsAggregate, MessageError> {
        self.settings
            .get_or_try_init(|| self.inner.get_settings_aggregate())
            .await
            .cloned()
    }

    async fn get_websites_aggregate(
        &self,
        address: &Address,
    ) -> Result<WebsitesAggregate, MessageError> {
        self.inner.get_websites_aggregate(address).await
    }

    async fn get_domains_aggregate(
        &self,
        address: &Address,
    ) -> Result<DomainsAggregate, MessageError> {
        self.inner.get_domains_aggregate(address).await
    }

    async fn get_port_forwarding_aggregate(
        &self,
        address: &Address,
    ) -> Result<PortForwardingAggregate, MessageError> {
        self.inner.get_port_forwarding_aggregate(address).await
    }

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
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn sample_pricing() -> PricingAggregate {
        let entity = serde_json::json!({
            "compute_unit": { "vcpus": 1, "memory_mib": 2048, "disk_mib": 20480 },
            "tiers": [],
            "price": {},
        });
        serde_json::from_value(serde_json::json!({
            "pricing": {
                "instance": entity,
                "instance_confidential": entity,
                "instance_gpu_standard": entity,
                "instance_gpu_premium": entity,
            }
        }))
        .unwrap()
    }

    /// Counts how often each aggregate is fetched from the (mock) network, and
    /// optionally fails the very first settings fetch to exercise the
    /// errors-are-not-cached path. The non-cached trait methods are never hit by
    /// these tests.
    struct CountingClient {
        pricing_calls: AtomicUsize,
        settings_calls: AtomicUsize,
        fail_first_settings: bool,
    }

    impl CountingClient {
        fn new(fail_first_settings: bool) -> Self {
            Self {
                pricing_calls: AtomicUsize::new(0),
                settings_calls: AtomicUsize::new(0),
                fail_first_settings,
            }
        }
    }

    impl AlephAggregateClient for CountingClient {
        async fn get_aggregate<T: DeserializeOwned>(
            &self,
            _address: &Address,
            _key: &str,
        ) -> Result<T, MessageError> {
            unimplemented!("not exercised by the caching tests")
        }

        async fn get_pricing_aggregate(&self) -> Result<PricingAggregate, MessageError> {
            self.pricing_calls.fetch_add(1, Ordering::SeqCst);
            Ok(sample_pricing())
        }

        async fn get_settings_aggregate(&self) -> Result<SettingsAggregate, MessageError> {
            let prior = self.settings_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_first_settings && prior == 0 {
                return Err(MessageError::ApiError {
                    status: 503,
                    body: "settings unavailable".to_string(),
                });
            }
            Ok(SettingsAggregate {
                settings: Default::default(),
            })
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
            unimplemented!()
        }
    }

    #[tokio::test]
    async fn fetches_each_aggregate_once_across_repeated_calls() {
        let inner = CountingClient::new(false);
        let client = CachingAggregateClient::new(&inner);

        for _ in 0..3 {
            client.get_pricing_aggregate().await.unwrap();
        }
        for _ in 0..3 {
            client.get_settings_aggregate().await.unwrap();
        }

        assert_eq!(inner.pricing_calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.settings_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn does_not_cache_a_failed_fetch() {
        let inner = CountingClient::new(true);
        let client = CachingAggregateClient::new(&inner);

        // First fetch fails and must not be cached; the second retries and
        // succeeds, then caches.
        assert!(client.get_settings_aggregate().await.is_err());
        assert!(client.get_settings_aggregate().await.is_ok());
        client.get_settings_aggregate().await.unwrap();

        assert_eq!(inner.settings_calls.load(Ordering::SeqCst), 2);
    }
}
