//! Mirrors `src/aleph/schemas/addresses_query_params.py`.
//!
//! Query parameters used to filter and sort the `/addresses` listing endpoint.

use serde::{Deserialize, Serialize};

use crate::schemas::messages_query_params::{DEFAULT_MESSAGES_PER_PAGE, DEFAULT_PAGE};
use crate::types::sort_order::{SortByMessageType, SortOrder};

const MAX_ADDRESS_FILTER_LENGTH: usize = 66;

fn default_sort_by_total() -> SortByMessageType {
    SortByMessageType::Total
}

fn default_sort_order_descending() -> SortOrder {
    SortOrder::Descending
}

fn default_pagination() -> i64 {
    DEFAULT_MESSAGES_PER_PAGE
}

fn default_page() -> i64 {
    DEFAULT_PAGE
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressesQueryParams {
    /// Opaque cursor for cursor-based pagination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,

    /// Case-insensitive substring filter for addresses.
    #[serde(
        rename = "addressContains",
        alias = "address_contains",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub address_contains: Option<String>,

    #[serde(
        rename = "sortBy",
        alias = "sort_by",
        default = "default_sort_by_total"
    )]
    pub sort_by: SortByMessageType,

    #[serde(
        rename = "sortOrder",
        alias = "sort_order",
        default = "default_sort_order_descending"
    )]
    pub sort_order: SortOrder,

    #[serde(default = "default_pagination")]
    pub pagination: i64,

    #[serde(default = "default_page")]
    pub page: i64,
}

impl Default for AddressesQueryParams {
    fn default() -> Self {
        Self {
            cursor: None,
            address_contains: None,
            sort_by: default_sort_by_total(),
            sort_order: default_sort_order_descending(),
            pagination: default_pagination(),
            page: default_page(),
        }
    }
}

impl AddressesQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(ref s) = self.address_contains
            && s.len() > MAX_ADDRESS_FILTER_LENGTH
        {
            return Err(format!(
                "addressContains exceeds max length of {MAX_ADDRESS_FILTER_LENGTH}"
            ));
        }
        if self.pagination < 0 {
            return Err("pagination must be >= 0".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addresses_query_params_default() {
        let p = AddressesQueryParams::default();
        assert_eq!(p.sort_by, SortByMessageType::Total);
        assert_eq!(p.sort_order, SortOrder::Descending);
        assert_eq!(p.pagination, DEFAULT_MESSAGES_PER_PAGE);
        assert_eq!(p.page, DEFAULT_PAGE);
        p.validate().unwrap();
    }

    #[test]
    fn test_addresses_query_params_roundtrip() {
        let json = serde_json::json!({
            "addressContains": "abc",
            "sortBy": "post",
            "sortOrder": 1,
            "pagination": 50,
            "page": 2
        });
        let parsed: AddressesQueryParams = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.address_contains, Some("abc".to_string()));
        assert_eq!(parsed.sort_by, SortByMessageType::Post);
        assert_eq!(parsed.sort_order, SortOrder::Ascending);
        assert_eq!(parsed.pagination, 50);
        assert_eq!(parsed.page, 2);
    }

    #[test]
    fn test_addresses_query_params_addr_too_long_rejected() {
        let p = AddressesQueryParams {
            address_contains: Some("x".repeat(67)),
            ..Default::default()
        };
        assert!(p.validate().is_err());
    }
}
