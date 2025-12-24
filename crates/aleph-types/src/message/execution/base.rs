use crate::chain::{Address, Chain};
use crate::item_hash::ItemHash;
use crate::message::execution::environment::{HostRequirements, MachineResources};
use crate::message::execution::volume::MachineVolume;
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Code and data can be provided in plain format, as zip or as squashfs partition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Plain,
    Zip,
    Squashfs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PaymentType {
    Hold,
    Superfluid,
    Credit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payment {
    /// Chain to check for funds.
    #[serde(default)]
    pub chain: Option<Chain>,
    /// Optional alternative address to send funds to.
    #[serde(default)]
    pub receiver: Option<Address>,
    #[serde(rename = "type")]
    pub payment_type: PaymentType,
}

///Two types of program interfaces supported: plain binaries and ASGI apps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Interface {
    Asgi,
    Binary,
}

/// Deserializes a metadata field that may come as either a JSON object or an empty array.
/// Some APIs incorrectly return `[]` instead of `{}` or `null` for empty metadata.
fn deserialize_metadata_tolerant<'de, D>(
    deserializer: D,
) -> Result<Option<HashMap<String, serde_json::Value>>, D::Error>
where
    D: Deserializer<'de>,
{
    use serde_json::Value;

    let value = Option::<Value>::deserialize(deserializer)?;

    match value {
        None => Ok(None),
        Some(Value::Object(map)) => Ok(Some(map.into_iter().collect())),
        Some(Value::Array(arr)) if arr.is_empty() => {
            // Treat empty array as empty map
            Ok(Some(HashMap::new()))
        }
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected object or empty array for metadata, got {}",
            other
        ))),
    }
}

/// Fields shared by program and instance messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ExecutableContent {
    /// Allow amends to update this function.
    pub allow_amend: bool,
    /// Metadata of the VM.
    #[serde(default, deserialize_with = "deserialize_metadata_tolerant")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
    /// Environment variables to set in the VM.
    #[serde(default)]
    pub variables: Option<HashMap<String, String>>,
    /// System resources required.
    pub resources: MachineResources,
    /// Payment details.
    #[serde(default)]
    pub payment: Option<Payment>,
    #[serde(default)]
    pub requirements: Option<HostRequirements>,
    /// Volumes to mount on the filesystem.
    #[serde(default)]
    pub volumes: Vec<MachineVolume>,
    /// Previous version to replace.
    #[serde(default)]
    pub replaces: Option<ItemHash>,
    #[serde(default)]
    pub authorized_keys: Option<Vec<String>>,
}
