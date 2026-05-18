//! Mirrors `src/aleph/schemas/api/costs.py`.
//!
//! Request / response shapes for the `/api/v0/costs` and price estimation
//! endpoints.

use aleph_types::message::execution::base::PaymentType;
use serde::{Deserialize, Serialize};

use crate::schemas::messages_query_params::DEFAULT_PAGE;
use crate::toolkit::costs::{CostInput, format_cost_str};

fn default_payment_type_credit() -> PaymentType {
    PaymentType::Credit
}

fn default_pagination_costs() -> i64 {
    100
}

fn default_page() -> i64 {
    DEFAULT_PAGE
}

/// Apply Python's `check_format_price` validator: pass any cost string through
/// `format_cost_str`. Strings come in as `Decimal | str` in pyaleph; we accept
/// either form.
fn format_price_str<'de, D: serde::Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    let value = serde_json::Value::deserialize(d)?;
    let raw = match value {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s,
        other => {
            return Err(serde::de::Error::custom(format!(
                "expected price as string or number, got {other:?}"
            )));
        }
    };
    Ok(format_cost_str(CostInput::Str(raw.as_str()), None))
}

fn format_price_str_decimal_or_str<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<String, D::Error> {
    format_price_str(d)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetCostsQueryParams {
    /// Filter by owner address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    /// Filter by specific resource `item_hash`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_hash: Option<String>,
    /// Filter by payment type.
    #[serde(default = "default_payment_type_credit")]
    pub payment_type: PaymentType,
    /// Detail level: 0=summary only, 1=resource list, 2=resource list with
    /// component-level breakdown.
    #[serde(default)]
    pub include_details: i32,
    /// Include `size_mib` in cost component details.
    #[serde(default)]
    pub include_size: bool,
    /// Number of resources per page (10-1000).
    #[serde(default = "default_pagination_costs")]
    pub pagination: i64,
    #[serde(default = "default_page")]
    pub page: i64,
}

impl Default for GetCostsQueryParams {
    fn default() -> Self {
        Self {
            address: None,
            item_hash: None,
            payment_type: PaymentType::Credit,
            include_details: 0,
            include_size: false,
            pagination: 100,
            page: DEFAULT_PAGE,
        }
    }
}

impl GetCostsQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        if !(0..=2).contains(&self.include_details) {
            return Err("include_details must be in [0, 2]".to_string());
        }
        if !(10..=1000).contains(&self.pagination) {
            return Err("pagination must be in [10, 1000]".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostsSummaryResponse {
    pub total_consumed_credits: i64,
    pub total_cost_hold: String,
    pub total_cost_stream: String,
    pub total_cost_credit: String,
    pub resource_count: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostsFiltersResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub item_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payment_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostComponentDetail {
    /// Cost component type (EXECUTION, STORAGE, …).
    #[serde(rename = "type")]
    pub component_type: String,
    pub name: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_hold: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_stream: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_credit: String,
    /// Storage size in MiB; populated for volume/storage-related components.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_mib: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceCostItem {
    pub item_hash: String,
    pub owner: String,
    pub payment_type: String,
    pub consumed_credits: i64,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_hold: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_stream: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_credit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<Vec<CostComponentDetail>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetCostsResponse {
    pub summary: CostsSummaryResponse,
    pub filters: CostsFiltersResponse,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<ResourceCostItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_page: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_total: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_per_page: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination_item: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EstimatedCostDetailResponse {
    #[serde(rename = "type")]
    pub component_type: String,
    pub name: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_hold: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_stream: String,
    #[serde(deserialize_with = "format_price_str_decimal_or_str")]
    pub cost_credit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_mib: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EstimatedCostsResponse {
    pub required_tokens: f64,
    pub payment_type: String,
    pub cost: String,
    pub detail: Vec<EstimatedCostDetailResponse>,
    pub charged_address: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_costs_query_params_defaults() {
        let p = GetCostsQueryParams::default();
        assert_eq!(p.payment_type, PaymentType::Credit);
        assert_eq!(p.pagination, 100);
        assert_eq!(p.page, 1);
        p.validate().unwrap();
    }

    #[test]
    fn test_get_costs_query_params_roundtrip() {
        let json = serde_json::json!({
            "payment_type": "hold",
            "include_details": 2,
            "include_size": true,
            "pagination": 100,
            "page": 1
        });
        let parsed: GetCostsQueryParams = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.payment_type, PaymentType::Hold);
        assert!(parsed.include_size);
        assert_eq!(parsed.include_details, 2);
    }

    #[test]
    fn test_get_costs_query_params_pagination_validation() {
        let p = GetCostsQueryParams {
            pagination: 5,
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }

    #[test]
    fn test_cost_component_detail_parse_number_price() {
        let json = serde_json::json!({
            "type": "EXECUTION",
            "name": "EXEC",
            "cost_hold": 1,
            "cost_stream": "0.5",
            "cost_credit": "0",
            "size_mib": 100
        });
        let parsed: CostComponentDetail = serde_json::from_value(json).unwrap();
        assert!(parsed.cost_hold.starts_with("1."));
        assert!(parsed.cost_stream.starts_with("0.5"));
        assert_eq!(parsed.size_mib, Some(100.0));
    }

    #[test]
    fn test_get_costs_response_roundtrip() {
        let resp = GetCostsResponse {
            summary: CostsSummaryResponse {
                total_consumed_credits: 100,
                total_cost_hold: "1.0".into(),
                total_cost_stream: "0.5".into(),
                total_cost_credit: "0.1".into(),
                resource_count: 2,
            },
            filters: CostsFiltersResponse {
                address: Some("0xa".into()),
                item_hash: None,
                payment_type: Some("credit".into()),
            },
            resources: None,
            pagination_page: Some(1),
            pagination_total: Some(10),
            pagination_per_page: Some(100),
            pagination_item: Some("resources".into()),
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: GetCostsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }

    #[test]
    fn test_estimated_costs_response_roundtrip() {
        let resp = EstimatedCostsResponse {
            required_tokens: 1.5,
            payment_type: "hold".into(),
            cost: "1.5".into(),
            detail: vec![],
            charged_address: "0xa".into(),
        };
        let json = serde_json::to_value(&resp).unwrap();
        let back: EstimatedCostsResponse = serde_json::from_value(json).unwrap();
        assert_eq!(back, resp);
    }
}
