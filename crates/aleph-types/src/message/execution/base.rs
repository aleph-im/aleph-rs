use crate::chain::{Address, Chain};
use crate::item_hash::ItemHash;
use crate::message::execution::environment::{HostRequirements, MachineResources};
use crate::message::execution::volume::MachineVolume;
use serde::{Deserialize, Serialize};
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

/// Fields shared by program and instance messages.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct ExecutableContent {
    /// Allow amends to update this function.
    pub allow_amend: bool,
    /// Metadata of the VM.
    #[serde(default)]
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
