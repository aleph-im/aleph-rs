//! Mirrors `src/aleph/schemas/messages_query_params.py`.
//!
//! Query-parameter models used to filter the `/messages` and related listing
//! endpoints. Python uses Pydantic `BeforeValidator`s to split comma-separated
//! query strings into lists; we replicate that with custom deserializers.

use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use aleph_types::chain::Chain;
use aleph_types::item_hash::ItemHash;
use aleph_types::message::MessageType;
use aleph_types::message::execution::base::PaymentType;
use serde::de::{Deserializer, Visitor};
use serde::{Deserialize, Serialize};

use crate::types::content_format::ContentFormat;
use crate::types::message_status::MessageStatus;
use crate::types::sort_order::{SortBy, SortOrder};

pub const DEFAULT_WS_HISTORY: i64 = 10;
pub const DEFAULT_MESSAGES_PER_PAGE: i64 = 20;
pub const DEFAULT_PAGE: i64 = 1;
pub const LIST_FIELD_SEPARATOR: &str = ",";

fn default_sort_by_time() -> SortBy {
    SortBy::Time
}

fn default_sort_order_descending() -> SortOrder {
    SortOrder::Descending
}

fn default_message_statuses() -> Option<Vec<MessageStatus>> {
    Some(vec![MessageStatus::Processed, MessageStatus::Removing])
}

fn default_pagination() -> i64 {
    DEFAULT_MESSAGES_PER_PAGE
}

fn default_page() -> i64 {
    DEFAULT_PAGE
}

fn default_ws_history() -> Option<i64> {
    Some(DEFAULT_WS_HISTORY)
}

/// Deserialize either a JSON array or a comma-separated string into a
/// `Vec<T>` where `T: FromStr` for the string path and `Deserialize` for the
/// list path.
fn deserialize_csv_list<'de, D, T>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: fmt::Display,
{
    struct CsvListVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for CsvListVisitor<T>
    where
        T: Deserialize<'de> + FromStr,
        <T as FromStr>::Err: fmt::Display,
    {
        type Value = Option<Vec<T>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let value = T::from_str(part).map_err(serde::de::Error::custom)?;
                out.push(value);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<T>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    deserializer.deserialize_option(CsvListVisitor::<T>(PhantomData))
}

/// Deserialize either a sequence of `MessageType` or a comma-separated string.
fn deserialize_message_types<'de, D>(d: D) -> Result<Option<Vec<MessageType>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<MessageType>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string of message types")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let quoted = format!("\"{part}\"");
                let mt: MessageType = serde_json::from_str(&quoted).map_err(E::custom)?;
                out.push(mt);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<MessageType>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

/// Deserialize either a sequence of `MessageStatus` or a CSV string, falling back
/// to the default value when the field is absent.
fn deserialize_message_statuses<'de, D>(d: D) -> Result<Option<Vec<MessageStatus>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<MessageStatus>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string of message statuses")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let quoted = format!("\"{part}\"");
                let ms: MessageStatus = serde_json::from_str(&quoted).map_err(E::custom)?;
                out.push(ms);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<MessageStatus>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

fn deserialize_payment_types<'de, D>(d: D) -> Result<Option<Vec<PaymentType>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<PaymentType>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string of payment types")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let quoted = format!("\"{part}\"");
                let pt: PaymentType = serde_json::from_str(&quoted).map_err(E::custom)?;
                out.push(pt);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<PaymentType>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

fn deserialize_chains<'de, D>(d: D) -> Result<Option<Vec<Chain>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<Chain>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string of chains")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let quoted = format!("\"{part}\"");
                let c: Chain = serde_json::from_str(&quoted).map_err(E::custom)?;
                out.push(c);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<Chain>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

fn deserialize_item_hashes<'de, D>(d: D) -> Result<Option<Vec<ItemHash>>, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;

    impl<'de> Visitor<'de> for V {
        type Value = Option<Vec<ItemHash>>;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a sequence or a comma-separated string of item hashes")
        }

        fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            let mut out = Vec::new();
            for part in v.split(LIST_FIELD_SEPARATOR) {
                let ih = ItemHash::try_from(part).map_err(E::custom)?;
                out.push(ih);
            }
            Ok(Some(out))
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(value) = seq.next_element::<ItemHash>()? {
                out.push(value);
            }
            Ok(Some(out))
        }
    }

    d.deserialize_option(V)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseMessageQueryParams {
    #[serde(rename = "sortBy", alias = "sort_by", default = "default_sort_by_time")]
    pub sort_by: SortBy,

    #[serde(
        rename = "sortOrder",
        alias = "sort_order",
        default = "default_sort_order_descending"
    )]
    pub sort_order: SortOrder,

    /// Deprecated: use `message_types`/`msgTypes` instead.
    #[serde(
        rename = "msgType",
        alias = "message_type",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub message_type: Option<MessageType>,

    #[serde(
        rename = "msgTypes",
        alias = "message_types",
        default,
        deserialize_with = "deserialize_message_types",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_types: Option<Vec<MessageType>>,

    #[serde(
        rename = "msgStatuses",
        alias = "message_statuses",
        default = "default_message_statuses",
        deserialize_with = "deserialize_message_statuses",
        skip_serializing_if = "Option::is_none"
    )]
    pub message_statuses: Option<Vec<MessageStatus>>,

    #[serde(
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub addresses: Option<Vec<String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub owners: Option<Vec<String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub refs: Option<Vec<String>>,

    #[serde(
        rename = "contentHashes",
        alias = "content_hashes",
        default,
        deserialize_with = "deserialize_item_hashes",
        skip_serializing_if = "Option::is_none"
    )]
    pub content_hashes: Option<Vec<ItemHash>>,

    #[serde(
        rename = "contentKeys",
        alias = "content_keys",
        default,
        deserialize_with = "deserialize_item_hashes",
        skip_serializing_if = "Option::is_none"
    )]
    pub content_keys: Option<Vec<ItemHash>>,

    #[serde(
        rename = "contentTypes",
        alias = "content_types",
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub content_types: Option<Vec<String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_chains",
        skip_serializing_if = "Option::is_none"
    )]
    pub chains: Option<Vec<Chain>>,

    #[serde(
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub channels: Option<Vec<String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_csv_list",
        skip_serializing_if = "Option::is_none"
    )]
    pub tags: Option<Vec<String>>,

    #[serde(
        default,
        deserialize_with = "deserialize_item_hashes",
        skip_serializing_if = "Option::is_none"
    )]
    pub hashes: Option<Vec<ItemHash>>,

    #[serde(
        rename = "paymentTypes",
        alias = "payment_types",
        default,
        deserialize_with = "deserialize_payment_types",
        skip_serializing_if = "Option::is_none"
    )]
    pub payment_types: Option<Vec<PaymentType>>,

    /// Deprecated: use `contentFormat=none` instead. If true (and
    /// `contentFormat` is not set), omit the `content` field from each message.
    #[serde(rename = "excludeContent", alias = "exclude_content", default)]
    pub exclude_content: bool,

    /// Level of content detail: `full` (default) returns the complete content;
    /// `headers` returns a reduced per-type metadata subset (address, plus
    /// type/ref for POST, key for AGGREGATE, item_hash/ref for STORE); `none`
    /// omits content entirely. Takes precedence over `excludeContent` when set.
    #[serde(
        rename = "contentFormat",
        alias = "content_format",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub content_format: Option<ContentFormat>,

    #[serde(rename = "startDate", alias = "start_date", default)]
    pub start_date: f64,

    #[serde(rename = "endDate", alias = "end_date", default)]
    pub end_date: f64,

    #[serde(rename = "startBlock", alias = "start_block", default)]
    pub start_block: i64,

    #[serde(rename = "endBlock", alias = "end_block", default)]
    pub end_block: i64,
}

impl Default for BaseMessageQueryParams {
    fn default() -> Self {
        Self {
            sort_by: default_sort_by_time(),
            sort_order: default_sort_order_descending(),
            message_type: None,
            message_types: None,
            message_statuses: default_message_statuses(),
            addresses: None,
            owners: None,
            refs: None,
            content_hashes: None,
            content_keys: None,
            content_types: None,
            chains: None,
            channels: None,
            tags: None,
            hashes: None,
            payment_types: None,
            exclude_content: false,
            content_format: None,
            start_date: 0.0,
            end_date: 0.0,
            start_block: 0,
            end_block: 0,
        }
    }
}

impl BaseMessageQueryParams {
    /// Resolve the effective [`ContentFormat`], collapsing the deprecated
    /// `excludeContent` flag. Mirrors the `resolve_content_format` model
    /// validator: explicit `contentFormat` always wins; otherwise
    /// `excludeContent=true` maps to `none`, else `full`.
    pub fn resolve_content_format(&self) -> ContentFormat {
        self.content_format.unwrap_or(if self.exclude_content {
            ContentFormat::None
        } else {
            ContentFormat::Full
        })
    }

    /// Mirrors the `validate_field_dependencies` model validator.
    pub fn validate(&self) -> Result<(), String> {
        if self.start_date < 0.0 {
            return Err("startDate must be >= 0".to_string());
        }
        if self.end_date < 0.0 {
            return Err("endDate must be >= 0".to_string());
        }
        if self.start_block < 0 {
            return Err("startBlock must be >= 0".to_string());
        }
        if self.end_block < 0 {
            return Err("endBlock must be >= 0".to_string());
        }
        if self.start_date != 0.0 && self.end_date != 0.0 && self.end_date < self.start_date {
            return Err("end date cannot be lower than start date.".to_string());
        }
        if self.start_block != 0 && self.end_block != 0 && self.end_block < self.start_block {
            return Err("end block cannot be lower than start block.".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageQueryParams {
    #[serde(flatten)]
    pub base: BaseMessageQueryParams,

    #[serde(default = "default_pagination")]
    pub pagination: i64,

    #[serde(default = "default_page")]
    pub page: i64,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl Default for MessageQueryParams {
    fn default() -> Self {
        Self {
            base: BaseMessageQueryParams::default(),
            pagination: DEFAULT_MESSAGES_PER_PAGE,
            page: DEFAULT_PAGE,
            cursor: None,
        }
    }
}

impl MessageQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        self.base.validate()?;
        if self.pagination < 0 {
            return Err("pagination must be >= 0".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        if self.cursor.is_some() && self.base.sort_by == SortBy::TxTime {
            return Err("Cursor pagination is not supported with tx-time sort order.".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessageQueryParams {
    #[serde(flatten)]
    pub base: BaseMessageQueryParams,

    #[serde(
        default = "default_ws_history",
        skip_serializing_if = "Option::is_none"
    )]
    pub history: Option<i64>,
}

impl Default for WsMessageQueryParams {
    fn default() -> Self {
        Self {
            base: BaseMessageQueryParams::default(),
            history: Some(DEFAULT_WS_HISTORY),
        }
    }
}

impl WsMessageQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        self.base.validate()?;
        if let Some(h) = self.history
            && !(0..200).contains(&h)
        {
            return Err("history must be in range [0, 200)".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHashesQueryParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<MessageStatus>,

    #[serde(default = "default_page")]
    pub page: i64,

    #[serde(default = "default_pagination")]
    pub pagination: i64,

    #[serde(rename = "startDate", alias = "start_date", default)]
    pub start_date: f64,

    #[serde(rename = "endDate", alias = "end_date", default)]
    pub end_date: f64,

    #[serde(
        rename = "sortOrder",
        alias = "sort_order",
        default = "default_sort_order_descending"
    )]
    pub sort_order: SortOrder,

    #[serde(default = "default_true")]
    #[serde(deserialize_with = "deserialize_bool_flexible")]
    pub hash_only: bool,
}

fn default_true() -> bool {
    true
}

fn deserialize_bool_flexible<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    struct BoolVisitor;

    impl Visitor<'_> for BoolVisitor {
        type Value = bool;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a bool or a bool-like query string")
        }

        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
            Ok(v)
        }

        fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
            match v.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "on" => Ok(true),
                "false" | "0" | "no" | "off" => Ok(false),
                _ => Err(E::custom(format!("invalid boolean value: {v}"))),
            }
        }

        fn visit_string<E: serde::de::Error>(self, v: String) -> Result<Self::Value, E> {
            self.visit_str(&v)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
            match v {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(E::custom(format!("invalid boolean integer: {v}"))),
            }
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
            match v {
                0 => Ok(false),
                1 => Ok(true),
                _ => Err(E::custom(format!("invalid boolean integer: {v}"))),
            }
        }
    }

    deserializer.deserialize_any(BoolVisitor)
}

impl Default for MessageHashesQueryParams {
    fn default() -> Self {
        Self {
            status: None,
            page: DEFAULT_PAGE,
            pagination: DEFAULT_MESSAGES_PER_PAGE,
            start_date: 0.0,
            end_date: 0.0,
            sort_order: SortOrder::Descending,
            hash_only: true,
        }
    }
}

impl MessageHashesQueryParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.start_date < 0.0 {
            return Err("startDate must be >= 0".to_string());
        }
        if self.end_date < 0.0 {
            return Err("endDate must be >= 0".to_string());
        }
        if self.pagination < 0 {
            return Err("pagination must be >= 0".to_string());
        }
        if self.page < 1 {
            return Err("page must be >= 1".to_string());
        }
        if self.start_date != 0.0 && self.end_date != 0.0 && self.end_date < self.start_date {
            return Err("end date cannot be lower than start date.".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_message_query_params() {
        let p = MessageQueryParams::default();
        assert_eq!(p.pagination, 20);
        assert_eq!(p.page, 1);
        assert_eq!(p.base.sort_by, SortBy::Time);
        assert_eq!(p.base.sort_order, SortOrder::Descending);
        assert!(p.cursor.is_none());
        assert_eq!(
            p.base.message_statuses,
            Some(vec![MessageStatus::Processed, MessageStatus::Removing])
        );
    }

    #[test]
    fn test_message_query_params_roundtrip_csv() {
        let json = serde_json::json!({
            "sortBy": "time",
            "sortOrder": -1,
            "msgTypes": "POST,AGGREGATE",
            "addresses": "0xa,0xb",
            "pagination": 10,
            "page": 2
        });
        let parsed: MessageQueryParams = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(
            parsed.base.message_types,
            Some(vec![MessageType::Post, MessageType::Aggregate])
        );
        assert_eq!(
            parsed.base.addresses,
            Some(vec!["0xa".to_string(), "0xb".to_string()])
        );
        assert_eq!(parsed.pagination, 10);
        assert_eq!(parsed.page, 2);
    }

    #[test]
    fn test_message_query_params_cursor_with_tx_time_rejected() {
        let json = serde_json::json!({
            "sortBy": "tx-time",
            "cursor": "abc"
        });
        let parsed: MessageQueryParams = serde_json::from_value(json).unwrap();
        assert!(parsed.validate().is_err());
    }

    #[test]
    fn test_message_query_params_end_before_start_rejected() {
        let json = serde_json::json!({
            "startDate": 200.0,
            "endDate": 100.0
        });
        let parsed: MessageQueryParams = serde_json::from_value(json).unwrap();
        assert!(parsed.validate().is_err());
    }

    #[test]
    fn test_message_query_params_chains_csv() {
        let json = serde_json::json!({"chains": "ETH,SOL"});
        let parsed: MessageQueryParams = serde_json::from_value(json).unwrap();
        assert_eq!(parsed.base.chains, Some(vec![Chain::Ethereum, Chain::Sol]));
    }

    #[test]
    fn test_message_hashes_query_params_default() {
        let p = MessageHashesQueryParams::default();
        assert!(p.hash_only);
        assert_eq!(p.sort_order, SortOrder::Descending);
        assert_eq!(p.page, 1);
        assert_eq!(p.pagination, 20);
    }

    #[test]
    fn test_message_hashes_query_params_roundtrip() {
        let json = serde_json::json!({
            "status": "processed",
            "page": 3,
            "pagination": 50,
            "hash_only": false
        });
        let parsed: MessageHashesQueryParams = serde_json::from_value(json).unwrap();
        parsed.validate().unwrap();
        assert_eq!(parsed.status, Some(MessageStatus::Processed));
        assert_eq!(parsed.page, 3);
        assert_eq!(parsed.pagination, 50);
        assert!(!parsed.hash_only);
    }

    #[test]
    fn test_ws_message_query_params_default() {
        let p = WsMessageQueryParams::default();
        assert_eq!(p.history, Some(DEFAULT_WS_HISTORY));
        p.validate().unwrap();
    }

    #[test]
    fn test_payment_types_csv() {
        let json = serde_json::json!({"paymentTypes": "hold,credit"});
        let parsed: MessageQueryParams = serde_json::from_value(json).unwrap();
        assert_eq!(
            parsed.base.payment_types,
            Some(vec![PaymentType::Hold, PaymentType::Credit])
        );
    }
}
