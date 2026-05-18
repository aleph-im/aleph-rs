//! Pricing-model utilities. Mirrors `aleph/services/pricing_utils.py`.

use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};
use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::accessors::aggregates::{get_aggregate_elements, merge_aggregate_elements};
use crate::db::models::aggregates::AggregateElementDb;
use crate::toolkit::constants::{
    DEFAULT_PRICE_AGGREGATE, PRICE_AGGREGATE_KEY, PRICE_AGGREGATE_OWNER,
};
use crate::types::cost::{ProductPriceType, ProductPricing};

/// Build a complete pricing model from an aggregate content dictionary.
///
/// Mirrors `build_pricing_model_from_aggregate`. Unparseable / unknown keys
/// are skipped (matching Python's `except KeyError, ValueError`).
pub fn build_pricing_model_from_aggregate(
    aggregate_content: &Value,
) -> HashMap<ProductPriceType, ProductPricing> {
    let mut pricing_model: HashMap<ProductPriceType, ProductPricing> = HashMap::new();
    let Some(obj) = aggregate_content.as_object() else {
        return pricing_model;
    };
    for (key, _) in obj {
        let price_type: ProductPriceType = match serde_json::from_value(Value::String(key.clone()))
        {
            Ok(t) => t,
            Err(_) => continue,
        };
        match ProductPricing::from_aggregate(price_type, aggregate_content) {
            Ok(p) => {
                pricing_model.insert(price_type, p);
            }
            Err(_) => continue,
        }
    }
    pricing_model
}

/// Build the default pricing model from [`DEFAULT_PRICE_AGGREGATE`].
pub fn build_default_pricing_model() -> HashMap<ProductPriceType, ProductPricing> {
    build_pricing_model_from_aggregate(&DEFAULT_PRICE_AGGREGATE)
}

/// All pricing aggregate updates ordered by creation_datetime ASC.
pub async fn get_pricing_aggregate_history(
    client: &impl GenericClient,
) -> AlephResult<Vec<AggregateElementDb>> {
    get_aggregate_elements(client, PRICE_AGGREGATE_OWNER, PRICE_AGGREGATE_KEY).await
}

/// Returns the chronological pricing timeline as `(timestamp, model)` tuples.
///
/// The initial entry uses `datetime::MIN` (UTC) to mirror Python's
/// `dt.datetime.min.replace(tzinfo=dt.timezone.utc)`.
pub async fn get_pricing_timeline(
    client: &impl GenericClient,
) -> AlephResult<Vec<(DateTime<Utc>, HashMap<ProductPriceType, ProductPricing>)>> {
    let history = get_pricing_aggregate_history(client).await?;
    let mut timeline: Vec<(DateTime<Utc>, HashMap<ProductPriceType, ProductPricing>)> = Vec::new();
    let min_ts = Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0).unwrap();
    timeline.push((min_ts, build_default_pricing_model()));
    let mut elements_so_far: Vec<&AggregateElementDb> = Vec::new();
    for element in history.iter() {
        elements_so_far.push(element);
        let merged = merge_aggregate_elements(elements_so_far.iter().copied());
        let merged_value = Value::Object(merged);
        let model = build_pricing_model_from_aggregate(&merged_value);
        timeline.push((element.creation_datetime, model));
    }
    Ok(timeline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Round-trip: the default pricing model must contain every product type
    /// defined in `DEFAULT_PRICE_AGGREGATE` and pricing fields must match.
    #[test]
    fn default_pricing_model_covers_all_product_types() {
        let model = build_default_pricing_model();
        let expected = [
            ProductPriceType::Storage,
            ProductPriceType::Web3Hosting,
            ProductPriceType::Program,
            ProductPriceType::ProgramPersistent,
            ProductPriceType::Instance,
            ProductPriceType::InstanceGpuPremium,
            ProductPriceType::InstanceConfidential,
            ProductPriceType::InstanceGpuStandard,
        ];
        for t in expected {
            assert!(model.contains_key(&t), "missing {:?}", t);
        }
        // Spot-check one value: instance compute_unit.holding = 1000
        let inst = model.get(&ProductPriceType::Instance).unwrap();
        assert_eq!(
            inst.price.compute_unit.as_ref().unwrap().holding,
            rust_decimal::Decimal::from(1000)
        );
    }

    /// Unknown / unparseable price-type keys are silently skipped, matching
    /// Python's try/except.
    #[test]
    fn build_pricing_model_skips_unknown_keys() {
        let agg = json!({
            "storage": {"price": {"storage": {"holding": "1"}}},
            "future_product": {"price": {"storage": {"holding": "1"}}}
        });
        let model = build_pricing_model_from_aggregate(&agg);
        assert_eq!(model.len(), 1);
        assert!(model.contains_key(&ProductPriceType::Storage));
    }

    #[test]
    fn build_pricing_model_handles_non_object_root() {
        let agg = json!("not an object");
        let model = build_pricing_model_from_aggregate(&agg);
        assert!(model.is_empty());
    }
}
