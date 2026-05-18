//! Cost computation engine. Mirrors `aleph/services/cost.py`.
//!
//! The Python module operates on `aleph_message.models.*Content` instances,
//! whose shape is a flat Pydantic structure with `.address`, `.payment`,
//! `.volumes`, `.resources`, `.rootfs`/`.code`/`.runtime`/`.data` and `.on`.
//! In Rust the analogous typed messages live in `aleph_types::message::*` but
//! the `address`/`time` fields are carried by the *enclosing* `MessageContent`,
//! not the inner content struct. To match Python's duck-typed entry points
//! without duplicating large data structures, this port consumes a
//! [`serde_json::Value`] view of the content — the same shape the Python tests
//! use.
//!
//! The arithmetic is bit-for-bit identical to Python: every cost amount goes
//! through [`format_cost`] which floor-quantises to 18 decimal digits.

use std::collections::HashMap;

use rust_decimal::Decimal;
use serde_json::Value;
use tokio_postgres::GenericClient;

use crate::AlephResult;
use crate::db::accessors::aggregates::get_aggregate_by_key;
use crate::db::accessors::cost::get_message_costs;
use crate::db::accessors::files::{get_file, get_file_tag, get_message_file_pin};
use crate::db::models::account_costs::{AccountCostsDb, PaymentType};
use crate::services::cache::local::GLOBAL_CACHE;
use crate::toolkit::constants::{
    DEFAULT_PRICE_AGGREGATE, DEFAULT_SETTINGS_AGGREGATE, HOUR, MIN_CREDIT_COST_PER_HOUR,
    MIN_STORE_COST_MIB, MiB, PRICE_AGGREGATE_KEY, PRICE_AGGREGATE_OWNER, SETTINGS_AGGREGATE_KEY,
    SETTINGS_AGGREGATE_OWNER,
};
use crate::toolkit::costs::format_cost;
use crate::types::cost::{
    CostType, ProductComputeUnit, ProductPriceType, ProductPricing, RefVolume, SizedVolume,
};
use crate::types::files::FileTag;

// ---------------------------------------------------------------------------
// Settings view
// ---------------------------------------------------------------------------

/// Single GPU compatibility entry as carried in the settings aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibleGpu {
    pub name: String,
    pub model: String,
    pub vendor: String,
    pub device_id: String,
}

/// Settings extracted from the settings aggregate. Mirrors `aleph.types.settings.Settings`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    pub compatible_gpus: Vec<CompatibleGpu>,
    pub community_wallet_address: String,
    pub community_wallet_timestamp: i64,
}

impl Settings {
    /// Build `Settings` from the JSON content of a settings aggregate.
    pub fn from_aggregate(content: &Value) -> Self {
        let community_wallet_address = content
            .get("community_wallet_address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let community_wallet_timestamp = content
            .get("community_wallet_timestamp")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let compatible_gpus = content
            .get("compatible_gpus")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|g| {
                        Some(CompatibleGpu {
                            name: g.get("name")?.as_str()?.to_string(),
                            model: g.get("model")?.as_str()?.to_string(),
                            vendor: g.get("vendor")?.as_str()?.to_string(),
                            device_id: g.get("device_id")?.as_str()?.to_string(),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Self {
            compatible_gpus,
            community_wallet_address,
            community_wallet_timestamp,
        }
    }
}

// ---------------------------------------------------------------------------
// Aggregate fetchers (with cache, mirroring the TODO in Python)
// ---------------------------------------------------------------------------

const COST_AGGREGATE_CACHE_NAMESPACE: &str = "cost_aggregates";
const PRICE_CACHE_KEY: &str = "price";
const SETTINGS_CACHE_KEY: &str = "settings";

/// Fetch the settings aggregate, falling back to the default. Mirrors
/// `_get_settings_aggregate`.
pub async fn get_settings_aggregate(client: &impl GenericClient) -> AlephResult<Value> {
    if let Some(cached) = GLOBAL_CACHE.get(SETTINGS_CACHE_KEY, COST_AGGREGATE_CACHE_NAMESPACE) {
        return Ok(cached);
    }
    let agg = get_aggregate_by_key(
        client,
        SETTINGS_AGGREGATE_OWNER,
        SETTINGS_AGGREGATE_KEY,
        true,
    )
    .await?;
    let value = match agg {
        Some(a) => a.content,
        None => DEFAULT_SETTINGS_AGGREGATE.clone(),
    };
    GLOBAL_CACHE.set(
        SETTINGS_CACHE_KEY,
        value.clone(),
        COST_AGGREGATE_CACHE_NAMESPACE,
    );
    Ok(value)
}

/// Settings derived from the settings aggregate.
pub async fn get_settings(client: &impl GenericClient) -> AlephResult<Settings> {
    let aggregate = get_settings_aggregate(client).await?;
    Ok(Settings::from_aggregate(&aggregate))
}

/// Fetch the pricing aggregate, falling back to the default. Mirrors
/// `_get_price_aggregate`.
pub async fn get_price_aggregate(client: &impl GenericClient) -> AlephResult<Value> {
    if let Some(cached) = GLOBAL_CACHE.get(PRICE_CACHE_KEY, COST_AGGREGATE_CACHE_NAMESPACE) {
        return Ok(cached);
    }
    let agg =
        get_aggregate_by_key(client, PRICE_AGGREGATE_OWNER, PRICE_AGGREGATE_KEY, true).await?;
    let value = match agg {
        Some(a) => a.content,
        None => DEFAULT_PRICE_AGGREGATE.clone(),
    };
    GLOBAL_CACHE.set(
        PRICE_CACHE_KEY,
        value.clone(),
        COST_AGGREGATE_CACHE_NAMESPACE,
    );
    Ok(value)
}

/// Clear the cached pricing/settings aggregates. Used by tests and by callers
/// that just wrote a new aggregate revision.
pub fn invalidate_aggregate_cache() {
    GLOBAL_CACHE.delete_namespace(COST_AGGREGATE_CACHE_NAMESPACE);
}

// ---------------------------------------------------------------------------
// Content shapes (JSON-view)
// ---------------------------------------------------------------------------

/// Top-level message kind we know how to price. Determined from a JSON content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostContentKind {
    Instance,
    Program,
    Store,
}

/// View over a content JSON. Mirrors what `cost.py` reads from the Python
/// content object. Construct via [`CostContent::from_value`].
#[derive(Debug, Clone)]
pub struct CostContent<'a> {
    pub kind: CostContentKind,
    pub value: &'a Value,
}

impl<'a> CostContent<'a> {
    /// Build a `CostContent` from a JSON value. The caller asserts the
    /// content kind; auto-detection is performed only if the value has the
    /// canonical fields.
    pub fn from_value(value: &'a Value) -> Option<Self> {
        let kind = if value.get("rootfs").is_some() {
            CostContentKind::Instance
        } else if value.get("code").is_some() && value.get("on").is_some() {
            CostContentKind::Program
        } else if value.get("item_hash").is_some() {
            CostContentKind::Store
        } else {
            return None;
        };
        Some(Self { kind, value })
    }

    pub fn new(kind: CostContentKind, value: &'a Value) -> Self {
        Self { kind, value }
    }

    pub fn address(&self) -> &str {
        self.value
            .get("address")
            .and_then(|v| v.as_str())
            .unwrap_or("")
    }
}

// ---------------------------------------------------------------------------
// Payment type
// ---------------------------------------------------------------------------

/// Determine the payment type from `content.payment`. Mirrors
/// `get_payment_type`.
pub fn get_payment_type(content: &CostContent<'_>) -> PaymentType {
    let pt = content
        .value
        .get("payment")
        .and_then(|p| p.get("type"))
        .and_then(|t| t.as_str());
    match pt {
        Some("credit") => PaymentType::Credit,
        Some("superfluid") => PaymentType::Superfluid,
        _ => PaymentType::Hold,
    }
}

// ---------------------------------------------------------------------------
// Instance helpers
// ---------------------------------------------------------------------------

fn is_confidential_vm(content: &CostContent<'_>) -> bool {
    content
        .value
        .get("environment")
        .and_then(|e| e.get("trusted_execution"))
        .map(|v| !v.is_null())
        .unwrap_or(false)
}

fn instance_gpus(content: &CostContent<'_>) -> Vec<Value> {
    content
        .value
        .get("requirements")
        .and_then(|r| r.get("gpu"))
        .and_then(|g| g.as_array())
        .cloned()
        .unwrap_or_default()
}

fn is_gpu_vm(content: &CostContent<'_>) -> bool {
    !instance_gpus(content).is_empty()
}

fn lookup_gpu_model(settings: &Settings, device_id: &str) -> Option<String> {
    settings
        .compatible_gpus
        .iter()
        .find(|g| g.device_id == device_id)
        .map(|g| g.model.clone())
}

/// Mirrors `_get_gpu_tier_breakdown`. Returns total compute units per tier
/// (premium / standard). Errors when the GPU device_id or model can't be
/// resolved.
fn get_gpu_tier_breakdown(
    content: &CostContent<'_>,
    settings: &Settings,
    premium_pricing: &ProductPricing,
    standard_pricing: &ProductPricing,
) -> Result<HashMap<ProductPriceType, u64>, CostError> {
    let mut breakdown: HashMap<ProductPriceType, u64> = HashMap::new();
    let gpus = instance_gpus(content);
    let premium_tiers = premium_pricing.tiers.as_deref().unwrap_or(&[]);
    let standard_tiers = standard_pricing.tiers.as_deref().unwrap_or(&[]);

    for gpu in &gpus {
        let device_id = gpu.get("device_id").and_then(|v| v.as_str()).unwrap_or("");
        let model =
            lookup_gpu_model(settings, device_id).ok_or_else(|| CostError::GpuNotCompatible {
                device_id: device_id.to_string(),
            })?;
        let mut matched_tier = None;
        for tier in premium_tiers {
            if tier.model.as_deref() == Some(model.as_str()) {
                matched_tier = Some((ProductPriceType::InstanceGpuPremium, tier.compute_units));
                break;
            }
        }
        if matched_tier.is_none() {
            for tier in standard_tiers {
                if tier.model.as_deref() == Some(model.as_str()) {
                    matched_tier =
                        Some((ProductPriceType::InstanceGpuStandard, tier.compute_units));
                    break;
                }
            }
        }
        let (tier_type, cus) = matched_tier.ok_or(CostError::GpuModelNotPriced { model })?;
        *breakdown.entry(tier_type).or_insert(0) += cus as u64;
    }
    Ok(breakdown)
}

fn get_product_instance_type(
    content: &CostContent<'_>,
    settings: &Settings,
    price_aggregate: &Value,
) -> Result<ProductPriceType, CostError> {
    if is_confidential_vm(content) {
        return Ok(ProductPriceType::InstanceConfidential);
    }
    if !is_gpu_vm(content) {
        return Ok(ProductPriceType::Instance);
    }
    let premium =
        ProductPricing::from_aggregate(ProductPriceType::InstanceGpuPremium, price_aggregate)
            .map_err(CostError::Pricing)?;
    let standard =
        ProductPricing::from_aggregate(ProductPriceType::InstanceGpuStandard, price_aggregate)
            .map_err(CostError::Pricing)?;
    let breakdown = get_gpu_tier_breakdown(content, settings, &premium, &standard)?;
    let tiers: std::collections::HashSet<_> = breakdown.keys().copied().collect();
    if tiers.len() == 1 && tiers.contains(&ProductPriceType::InstanceGpuStandard) {
        Ok(ProductPriceType::InstanceGpuStandard)
    } else {
        Ok(ProductPriceType::InstanceGpuPremium)
    }
}

/// Mirrors `_get_product_price_type`.
pub fn get_product_price_type(
    content: &CostContent<'_>,
    settings: &Settings,
    price_aggregate: &Value,
) -> Result<ProductPriceType, CostError> {
    match content.kind {
        CostContentKind::Store => Ok(ProductPriceType::Storage),
        CostContentKind::Program => {
            let is_on_demand = !content
                .value
                .get("on")
                .and_then(|o| o.get("persistent"))
                .and_then(|p| p.as_bool())
                .unwrap_or(false);
            Ok(if is_on_demand {
                ProductPriceType::Program
            } else {
                ProductPriceType::ProgramPersistent
            })
        }
        CostContentKind::Instance => get_product_instance_type(content, settings, price_aggregate),
    }
}

/// Mirrors `_get_product_price`.
pub async fn get_product_price(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    settings: &Settings,
) -> Result<ProductPricing, CostError> {
    let aggregate = get_price_aggregate(client).await?;
    let price_type = get_product_price_type(content, settings, &aggregate)?;
    ProductPricing::from_aggregate(price_type, &aggregate).map_err(CostError::Pricing)
}

// ---------------------------------------------------------------------------
// Compute unit helpers
// ---------------------------------------------------------------------------

fn read_resource_vcpus(content: &CostContent<'_>) -> u64 {
    content
        .value
        .get("resources")
        .and_then(|r| r.get("vcpus"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
}

fn read_resource_memory_mib(content: &CostContent<'_>) -> u64 {
    content
        .value
        .get("resources")
        .and_then(|r| r.get("memory"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0)
}

/// Mirrors `_get_nb_compute_units`. Computes `max(vcpus, ceil(memory / cu.memory_mib))`.
fn get_nb_compute_units(content: &CostContent<'_>, cu: Option<&ProductComputeUnit>) -> u64 {
    let default = ProductComputeUnit::new(1, 2048, 2048);
    let cu_ref = cu.unwrap_or(&default);
    let cpu = read_resource_vcpus(content);
    let memory = read_resource_memory_mib(content);
    // ceil(memory / cu_ref.memory_mib)
    let memory_units = memory.div_ceil(cu_ref.memory_mib);
    cpu.max(memory_units)
}

/// Mirrors `_get_compute_unit_multiplier`. Programs with `internet=true` get a
/// +1 multiplier (so 2× compute), persistent programs/instances stay at 1×.
fn get_compute_unit_multiplier(content: &CostContent<'_>) -> u64 {
    let mut multiplier = 1u64;
    if content.kind == CostContentKind::Program {
        let on_persistent = content
            .value
            .get("on")
            .and_then(|o| o.get("persistent"))
            .and_then(|p| p.as_bool())
            .unwrap_or(false);
        let internet = content
            .value
            .get("environment")
            .and_then(|e| e.get("internet"))
            .and_then(|p| p.as_bool())
            .unwrap_or(false);
        if !on_persistent && internet {
            multiplier += 1;
        }
    }
    multiplier
}

// ---------------------------------------------------------------------------
// Volume costs
// ---------------------------------------------------------------------------

/// One entry in the volumes list passed to [`get_volumes_costs`]. Mirrors
/// Python's `RefVolume | SizedVolume`.
#[derive(Debug, Clone)]
enum VolumeEntry {
    Sized(SizedVolume),
    Ref(RefVolume),
}

impl VolumeEntry {
    fn cost_type(&self) -> CostType {
        match self {
            VolumeEntry::Sized(s) => s.cost_type,
            VolumeEntry::Ref(r) => r.cost_type,
        }
    }
    fn name(&self) -> &str {
        match self {
            VolumeEntry::Sized(s) => &s.name,
            VolumeEntry::Ref(r) => &r.name,
        }
    }
    fn ref_(&self) -> Option<&str> {
        match self {
            VolumeEntry::Sized(s) => s.r#ref.as_deref(),
            VolumeEntry::Ref(r) => Some(r.r#ref.as_str()),
        }
    }
}

async fn get_file_from_ref(
    client: &impl GenericClient,
    r#ref: &str,
    use_latest: bool,
) -> AlephResult<Option<i64>> {
    // Returns the file size in bytes if the file is found.
    if use_latest {
        if let Some(tag) = get_file_tag(client, &FileTag::from(r#ref.to_string())).await? {
            if let Some(file) = get_file(client, &tag.file_hash).await? {
                return Ok(Some(file.size));
            }
        }
        Ok(None)
    } else {
        if let Some(pin) = get_message_file_pin(client, r#ref).await? {
            if let Some(file) = get_file(client, &pin.file_hash).await? {
                return Ok(Some(file.size));
            }
        }
        Ok(None)
    }
}

#[allow(clippy::too_many_arguments)]
async fn get_volumes_costs(
    client: &impl GenericClient,
    volumes: Vec<VolumeEntry>,
    payment_type: PaymentType,
    price_per_mib: Decimal,
    price_per_mib_second: Decimal,
    price_per_mib_credit: Decimal,
    owner: &str,
    item_hash: &str,
) -> AlephResult<Vec<AccountCostsDb>> {
    let mut costs: Vec<AccountCostsDb> = Vec::new();
    let mib_decimal = Decimal::from(MiB);
    let min_credit_per_second = Decimal::from(MIN_CREDIT_COST_PER_HOUR) / Decimal::from(HOUR);

    for volume in volumes {
        let storage_mib = match &volume {
            VolumeEntry::Sized(s) => s.size_mib,
            VolumeEntry::Ref(r) => {
                let file_size = get_file_from_ref(client, &r.r#ref, r.use_latest).await?;
                let Some(size_bytes) = file_size else {
                    continue;
                };
                Decimal::from(size_bytes) / mib_decimal
            }
        };

        let cost_hold = format_cost(storage_mib * price_per_mib, None);
        let cost_stream = format_cost(storage_mib * price_per_mib_second, None);
        let mut cost_credit = format_cost(storage_mib * price_per_mib_credit, None);
        if payment_type == PaymentType::Credit {
            let min = format_cost(min_credit_per_second, None);
            if min > cost_credit {
                cost_credit = min;
            }
        }

        costs.push(AccountCostsDb {
            id: 0,
            owner: owner.to_string(),
            item_hash: item_hash.to_string(),
            r#type: volume.cost_type(),
            name: volume.name().to_string(),
            r#ref: volume.ref_().map(|s| s.to_string()),
            payment_type,
            cost_hold,
            cost_stream,
            cost_credit,
        });
    }
    Ok(costs)
}

// ---------------------------------------------------------------------------
// Executable storage costs
// ---------------------------------------------------------------------------

fn read_immutable_ref_and_use_latest(volume: &Value) -> Option<(String, bool)> {
    let r = volume.get("ref")?.as_str()?.to_string();
    let use_latest = volume
        .get("use_latest")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    Some((r, use_latest))
}

fn is_immutable_volume(volume: &Value) -> bool {
    volume.get("ref").is_some()
}

fn is_ephemeral_volume(volume: &Value) -> bool {
    volume
        .get("ephemeral")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
}

fn volume_size_mib(volume: &Value) -> Option<u64> {
    volume.get("size_mib").and_then(|v| v.as_u64())
}

fn volume_mount(volume: &Value) -> Option<&str> {
    volume.get("mount").and_then(|v| v.as_str())
}

fn volume_estimated_size_mib(volume: &Value) -> Option<u64> {
    volume.get("estimated_size_mib").and_then(|v| v.as_u64())
}

fn build_execution_volumes(content: &CostContent<'_>) -> Vec<VolumeEntry> {
    let mut volumes: Vec<VolumeEntry> = Vec::new();

    match content.kind {
        CostContentKind::Instance => {
            // rootfs: SizedVolume with size_mib + parent.ref
            let rootfs = content.value.get("rootfs");
            if let Some(rootfs) = rootfs {
                let size = rootfs.get("size_mib").and_then(|v| v.as_u64()).unwrap_or(0);
                let parent_ref = rootfs
                    .get("parent")
                    .and_then(|p| p.get("ref"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                volumes.push(VolumeEntry::Sized(SizedVolume::new(
                    CostType::ExecutionInstanceVolumeRootfs,
                    Decimal::from(size),
                    parent_ref,
                    None,
                )));
            }
        }
        CostContentKind::Program => {
            // code volume
            if let Some(code) = content.value.get("code") {
                let est = code.get("estimated_size_mib").and_then(|v| v.as_u64());
                if let Some(est) = est {
                    let ref_ = code
                        .get("ref")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    volumes.push(VolumeEntry::Sized(SizedVolume::new(
                        CostType::ExecutionProgramVolumeCode,
                        Decimal::from(est),
                        ref_,
                        None,
                    )));
                } else if let Some(r) = code.get("ref").and_then(|v| v.as_str()) {
                    let use_latest = code
                        .get("use_latest")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    volumes.push(VolumeEntry::Ref(RefVolume::new(
                        CostType::ExecutionProgramVolumeCode,
                        r.to_string(),
                        use_latest,
                        None,
                    )));
                }
            }
            // runtime volume
            if let Some(rt) = content.value.get("runtime") {
                let est = rt.get("estimated_size_mib").and_then(|v| v.as_u64());
                if let Some(est) = est {
                    let ref_ = rt
                        .get("ref")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    volumes.push(VolumeEntry::Sized(SizedVolume::new(
                        CostType::ExecutionProgramVolumeRuntime,
                        Decimal::from(est),
                        ref_,
                        None,
                    )));
                } else if let Some(r) = rt.get("ref").and_then(|v| v.as_str()) {
                    let use_latest = rt
                        .get("use_latest")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    volumes.push(VolumeEntry::Ref(RefVolume::new(
                        CostType::ExecutionProgramVolumeRuntime,
                        r.to_string(),
                        use_latest,
                        None,
                    )));
                }
            }
            // data volume (optional)
            if let Some(data) = content.value.get("data") {
                if !data.is_null() {
                    let est = data.get("estimated_size_mib").and_then(|v| v.as_u64());
                    if let Some(est) = est {
                        let ref_ = data
                            .get("ref")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        volumes.push(VolumeEntry::Sized(SizedVolume::new(
                            CostType::ExecutionProgramVolumeData,
                            Decimal::from(est),
                            ref_,
                            None,
                        )));
                    } else if let Some(r) = data.get("ref").and_then(|v| v.as_str()) {
                        let use_latest = data
                            .get("use_latest")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        volumes.push(VolumeEntry::Ref(RefVolume::new(
                            CostType::ExecutionProgramVolumeData,
                            r.to_string(),
                            use_latest,
                            None,
                        )));
                    }
                }
            }
        }
        _ => {}
    }

    // additional volumes
    if let Some(arr) = content.value.get("volumes").and_then(|v| v.as_array()) {
        for (i, volume) in arr.iter().enumerate() {
            let name_prefix = format!("#{i}");
            if is_immutable_volume(volume) {
                let mount = volume_mount(volume);
                let name = format!(
                    "{name_prefix}:{}",
                    mount.unwrap_or(CostType::ExecutionVolumeInmutable.as_value_str())
                );
                if let Some(est) = volume_estimated_size_mib(volume) {
                    let ref_ = volume
                        .get("ref")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    volumes.push(VolumeEntry::Sized(SizedVolume::new(
                        CostType::ExecutionVolumeInmutable,
                        Decimal::from(est),
                        ref_,
                        Some(name),
                    )));
                } else if let Some((r, use_latest)) = read_immutable_ref_and_use_latest(volume) {
                    volumes.push(VolumeEntry::Ref(RefVolume::new(
                        CostType::ExecutionVolumeInmutable,
                        r,
                        use_latest,
                        Some(name),
                    )));
                }
            } else if is_ephemeral_volume(volume) {
                // Ephemeral volumes are also priced as persistent (using size_mib).
                // Python falls through to the `else` branch in the loop too.
                let mount = volume_mount(volume);
                let name = format!(
                    "{name_prefix}:{}",
                    mount.unwrap_or(CostType::ExecutionVolumePersistent.as_value_str())
                );
                let size = volume_size_mib(volume).unwrap_or(0);
                volumes.push(VolumeEntry::Sized(SizedVolume::new(
                    CostType::ExecutionVolumePersistent,
                    Decimal::from(size),
                    None,
                    Some(name),
                )));
            } else {
                // persistent
                let mount = volume_mount(volume);
                let name = format!(
                    "{name_prefix}:{}",
                    mount.unwrap_or(CostType::ExecutionVolumePersistent.as_value_str())
                );
                let size = volume_size_mib(volume).unwrap_or(0);
                volumes.push(VolumeEntry::Sized(SizedVolume::new(
                    CostType::ExecutionVolumePersistent,
                    Decimal::from(size),
                    None,
                    Some(name),
                )));
            }
        }
    }
    volumes
}

async fn get_execution_volumes_costs(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    pricing: &ProductPricing,
    payment_type: PaymentType,
    item_hash: &str,
) -> AlephResult<Vec<AccountCostsDb>> {
    let volumes = build_execution_volumes(content);
    let price_per_mib = pricing.price.storage.holding;
    let price_per_mib_second = pricing.price.storage.payg / Decimal::from(HOUR);
    let price_per_mib_credit = pricing.price.storage.credit / Decimal::from(HOUR);
    get_volumes_costs(
        client,
        volumes,
        payment_type,
        price_per_mib,
        price_per_mib_second,
        price_per_mib_credit,
        content.address(),
        item_hash,
    )
    .await
}

async fn get_additional_storage_price(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    pricing: &ProductPricing,
    payment_type: PaymentType,
    item_hash: &str,
) -> AlephResult<Vec<AccountCostsDb>> {
    let mut costs =
        get_execution_volumes_costs(client, content, pricing, payment_type, item_hash).await?;

    let nb_compute_units = get_nb_compute_units(content, pricing.compute_unit.as_ref());
    let execution_volume_discount_mib =
        Decimal::from(pricing.compute_unit.map(|c| c.disk_mib).unwrap_or(0))
            * Decimal::from(nb_compute_units);

    let price_per_mib = pricing.price.storage.holding;
    let price_per_mib_second = pricing.price.storage.payg / Decimal::from(HOUR);
    let price_per_mib_credit = pricing.price.storage.credit / Decimal::from(HOUR);

    let max_discount_hold = execution_volume_discount_mib * price_per_mib;
    let max_discount_stream = execution_volume_discount_mib * price_per_mib_second;
    let max_discount_credit = execution_volume_discount_mib * price_per_mib_credit;

    let sum_hold: Decimal = costs.iter().map(|c| c.cost_hold).sum();
    let sum_stream: Decimal = costs.iter().map(|c| c.cost_stream).sum();
    let sum_credit: Decimal = costs.iter().map(|c| c.cost_credit).sum();

    let discount_hold = sum_hold.min(max_discount_hold);
    let discount_stream = sum_stream.min(max_discount_stream);
    let discount_credit = sum_credit.min(max_discount_credit);

    let cost_hold = format_cost(-discount_hold, None);
    let cost_stream = format_cost(-discount_stream, None);
    let cost_credit = format_cost(-discount_credit, None);

    costs.push(AccountCostsDb {
        id: 0,
        owner: content.address().to_string(),
        item_hash: item_hash.to_string(),
        r#type: CostType::ExecutionVolumeDiscount,
        name: CostType::ExecutionVolumeDiscount.as_value_str().to_string(),
        r#ref: None,
        payment_type,
        cost_hold,
        cost_stream,
        cost_credit,
    });
    Ok(costs)
}

// ---------------------------------------------------------------------------
// GPU multi-tier execution cost
// ---------------------------------------------------------------------------

async fn calculate_multi_tier_gpu_execution_cost(
    content: &CostContent<'_>,
    settings: &Settings,
    price_aggregate: &Value,
    payment_type: PaymentType,
    item_hash: &str,
) -> Result<Vec<AccountCostsDb>, CostError> {
    let premium =
        ProductPricing::from_aggregate(ProductPriceType::InstanceGpuPremium, price_aggregate)
            .map_err(CostError::Pricing)?;
    let standard =
        ProductPricing::from_aggregate(ProductPriceType::InstanceGpuStandard, price_aggregate)
            .map_err(CostError::Pricing)?;
    let mut breakdown = get_gpu_tier_breakdown(content, settings, &premium, &standard)?;

    // Resource-based CUs are a lower bound: if vCPUs/memory require more,
    // pour the difference into the dominant tier.
    let dominant_tier_type = if breakdown.contains_key(&ProductPriceType::InstanceGpuPremium) {
        ProductPriceType::InstanceGpuPremium
    } else {
        ProductPriceType::InstanceGpuStandard
    };
    let dominant_pricing = if dominant_tier_type == ProductPriceType::InstanceGpuPremium {
        &premium
    } else {
        &standard
    };
    let resource_cus = get_nb_compute_units(content, dominant_pricing.compute_unit.as_ref());
    let total_gpu_cus: u64 = breakdown.values().sum();
    if resource_cus > total_gpu_cus {
        *breakdown.entry(dominant_tier_type).or_insert(0) += resource_cus - total_gpu_cus;
    }

    let multiplier = get_compute_unit_multiplier(content);
    let mut costs: Vec<AccountCostsDb> = Vec::new();
    // Sort for determinism (premium first, then standard).
    let mut tiers: Vec<(ProductPriceType, u64)> = breakdown.into_iter().collect();
    tiers.sort_by_key(|(t, _)| match t {
        ProductPriceType::InstanceGpuPremium => 0,
        ProductPriceType::InstanceGpuStandard => 1,
        _ => 2,
    });

    for (price_type, compute_units) in tiers {
        let pricing = if price_type == ProductPriceType::InstanceGpuPremium {
            &premium
        } else {
            &standard
        };
        let cu_price = pricing
            .price
            .compute_unit
            .as_ref()
            .ok_or_else(|| CostError::MissingComputeUnitPrice(price_type))?;

        let mul = Decimal::from(compute_units) * Decimal::from(multiplier);
        let cost_hold = format_cost(mul * cu_price.holding, None);
        let cost_stream = format_cost(mul * cu_price.payg / Decimal::from(HOUR), None);
        let cost_credit = format_cost(mul * cu_price.credit / Decimal::from(HOUR), None);

        costs.push(AccountCostsDb {
            id: 0,
            owner: content.address().to_string(),
            item_hash: item_hash.to_string(),
            r#type: CostType::Execution,
            name: price_type.as_value_str().to_string(),
            r#ref: None,
            payment_type,
            cost_hold,
            cost_stream,
            cost_credit,
        });
    }
    Ok(costs)
}

// ---------------------------------------------------------------------------
// Executable cost (non-GPU) and storage cost
// ---------------------------------------------------------------------------

async fn calculate_executable_costs(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    pricing: &ProductPricing,
    item_hash: &str,
) -> Result<Vec<AccountCostsDb>, CostError> {
    let payment_type = get_payment_type(content);
    let settings = get_settings(client).await?;
    let price_aggregate = get_price_aggregate(client).await?;

    if content.kind == CostContentKind::Instance && is_gpu_vm(content) {
        let mut execution = calculate_multi_tier_gpu_execution_cost(
            content,
            &settings,
            &price_aggregate,
            payment_type,
            item_hash,
        )
        .await?;
        // Use premium pricing as baseline for storage (matches Python).
        let storage_pricing =
            ProductPricing::from_aggregate(ProductPriceType::InstanceGpuPremium, &price_aggregate)
                .map_err(CostError::Pricing)?;
        let storage = get_additional_storage_price(
            client,
            content,
            &storage_pricing,
            payment_type,
            item_hash,
        )
        .await?;
        execution.extend(storage);
        return Ok(execution);
    }

    // Non-GPU path
    let cu_price = pricing
        .price
        .compute_unit
        .as_ref()
        .ok_or_else(|| CostError::MissingComputeUnitPrice(pricing.price_type))?;
    let nb_cus = get_nb_compute_units(content, pricing.compute_unit.as_ref());
    let multiplier = get_compute_unit_multiplier(content);
    let mul = Decimal::from(nb_cus) * Decimal::from(multiplier);

    let cost_hold = format_cost(mul * cu_price.holding, None);
    let cost_stream = format_cost(mul * cu_price.payg / Decimal::from(HOUR), None);
    let mut cost_credit = format_cost(mul * cu_price.credit / Decimal::from(HOUR), None);
    if payment_type == PaymentType::Credit {
        let min_credit_per_second = Decimal::from(MIN_CREDIT_COST_PER_HOUR) / Decimal::from(HOUR);
        let min = format_cost(min_credit_per_second, None);
        if min > cost_credit {
            cost_credit = min;
        }
    }

    let mut costs: Vec<AccountCostsDb> = vec![AccountCostsDb {
        id: 0,
        owner: content.address().to_string(),
        item_hash: item_hash.to_string(),
        r#type: CostType::Execution,
        name: pricing.price_type.as_value_str().to_string(),
        r#ref: None,
        payment_type,
        cost_hold,
        cost_stream,
        cost_credit,
    }];
    costs.extend(
        get_additional_storage_price(client, content, pricing, payment_type, item_hash).await?,
    );
    Ok(costs)
}

/// Mirrors `calculate_storage_size`. Returns the storage size in MiB based on
/// either `estimated_size_mib` (for `CostEstimationStoreContent`) or the file
/// size on record.
pub async fn calculate_storage_size(
    client: &impl GenericClient,
    content: &CostContent<'_>,
) -> AlephResult<Option<Decimal>> {
    if content.kind != CostContentKind::Store {
        return Ok(None);
    }
    if let Some(est) = content
        .value
        .get("estimated_size_mib")
        .and_then(|v| v.as_u64())
    {
        return Ok(Some(Decimal::from(est)));
    }
    let item_hash = content
        .value
        .get("item_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let file = get_file(client, item_hash).await?;
    Ok(file.map(|f| Decimal::from(f.size) / Decimal::from(MiB)))
}

async fn calculate_storage_costs(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    pricing: &ProductPricing,
    item_hash: &str,
) -> AlephResult<Vec<AccountCostsDb>> {
    let payment_type = get_payment_type(content);
    let Some(mut storage_mib) = calculate_storage_size(client, content).await? else {
        return Ok(Vec::new());
    };
    let min = Decimal::from(MIN_STORE_COST_MIB);
    if payment_type == PaymentType::Credit && storage_mib < min {
        storage_mib = min;
    }
    let volume = VolumeEntry::Sized(SizedVolume::new(
        CostType::Storage,
        storage_mib,
        Some(item_hash.to_string()),
        None,
    ));
    let price_per_mib = pricing.price.storage.holding;
    let price_per_mib_second = pricing.price.storage.payg / Decimal::from(HOUR);
    let price_per_mib_credit = pricing.price.storage.credit / Decimal::from(HOUR);
    get_volumes_costs(
        client,
        vec![volume],
        payment_type,
        price_per_mib,
        price_per_mib_second,
        price_per_mib_credit,
        content.address(),
        item_hash,
    )
    .await
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Errors raised by the cost engine. Mirrors `ValueError`/missing-key cases in
/// Python.
#[derive(Debug, thiserror::Error)]
pub enum CostError {
    #[error("GPU device_id {device_id} not found in compatible GPUs")]
    GpuNotCompatible { device_id: String },
    #[error("GPU model {model} not found in any pricing tier (premium or standard)")]
    GpuModelNotPriced { model: String },
    #[error("compute_unit price not defined for type '{}' in pricing aggregate", .0.as_value_str())]
    MissingComputeUnitPrice(ProductPriceType),
    #[error("pricing parse error: {0}")]
    Pricing(#[from] crate::types::cost::ProductPricingError),
    #[error("db error: {0}")]
    Db(#[from] crate::AlephError),
}

/// Detailed per-component cost rows. Mirrors `get_detailed_costs`.
pub async fn get_detailed_costs(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    item_hash: &str,
    pricing: Option<ProductPricing>,
    settings: Option<Settings>,
) -> Result<Vec<AccountCostsDb>, CostError> {
    let settings = match settings {
        Some(s) => s,
        None => get_settings(client).await?,
    };
    let pricing = match pricing {
        Some(p) => p,
        None => get_product_price(client, content, &settings).await?,
    };
    match content.kind {
        CostContentKind::Store => {
            Ok(calculate_storage_costs(client, content, &pricing, item_hash).await?)
        }
        _ => calculate_executable_costs(client, content, &pricing, item_hash).await,
    }
}

fn sum_payment_costs(payment_type: PaymentType, costs: &[AccountCostsDb]) -> Decimal {
    let sum: Decimal = costs
        .iter()
        .map(|c| match payment_type {
            PaymentType::Superfluid => c.cost_stream,
            PaymentType::Credit => c.cost_credit,
            PaymentType::Hold => c.cost_hold,
        })
        .sum();
    format_cost(sum, None)
}

/// Total cost (as Decimal) + the detailed list. Mirrors
/// `get_total_and_detailed_costs`.
pub async fn get_total_and_detailed_costs(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    item_hash: &str,
) -> Result<(Decimal, Vec<AccountCostsDb>), CostError> {
    let payment_type = get_payment_type(content);
    let costs = get_detailed_costs(client, content, item_hash, None, None).await?;
    let total = sum_payment_costs(payment_type, &costs);
    Ok((total, costs))
}

/// Total cost (as Decimal) + the detailed list, sourced from the DB rather
/// than re-computed. Mirrors `get_total_and_detailed_costs_from_db`.
pub async fn get_total_and_detailed_costs_from_db(
    client: &impl GenericClient,
    content: &CostContent<'_>,
    item_hash: &str,
) -> AlephResult<(Decimal, Vec<AccountCostsDb>)> {
    let payment_type = get_payment_type(content);
    let costs = get_message_costs(client, item_hash).await?;
    let total = sum_payment_costs(payment_type, &costs);
    Ok((total, costs))
}

// ---------------------------------------------------------------------------
// Cost-component size helpers
// ---------------------------------------------------------------------------

async fn get_size_from_file_ref(
    client: &impl GenericClient,
    file_hash: &str,
) -> AlephResult<Option<f64>> {
    let file = get_file(client, file_hash).await?;
    Ok(file.map(|f| f.size as f64 / MiB as f64))
}

fn get_estimated_size_from_content(
    cost: &AccountCostsDb,
    content: &CostContent<'_>,
) -> Option<f64> {
    match cost.r#type {
        CostType::Storage => content
            .value
            .get("estimated_size_mib")
            .and_then(|v| v.as_u64())
            .map(|n| n as f64),
        CostType::ExecutionProgramVolumeCode => content
            .value
            .get("code")
            .and_then(|c| c.get("estimated_size_mib"))
            .and_then(|v| v.as_u64())
            .map(|n| n as f64),
        CostType::ExecutionProgramVolumeRuntime => content
            .value
            .get("runtime")
            .and_then(|c| c.get("estimated_size_mib"))
            .and_then(|v| v.as_u64())
            .map(|n| n as f64),
        CostType::ExecutionProgramVolumeData => content
            .value
            .get("data")
            .and_then(|c| c.get("estimated_size_mib"))
            .and_then(|v| v.as_u64())
            .map(|n| n as f64),
        CostType::ExecutionVolumeInmutable => {
            // name has format "#<index>:<mount>"
            let (idx_str, _) = cost.name.split_once(':')?;
            let idx: usize = idx_str.trim_start_matches('#').parse().ok()?;
            let arr = content.value.get("volumes")?.as_array()?;
            let v = arr.get(idx)?;
            v.get("estimated_size_mib")
                .and_then(|v| v.as_u64())
                .map(|n| n as f64)
        }
        _ => None,
    }
}

/// Retrieve the size in MiB for a cost component. Mirrors
/// `get_cost_component_size_mib`.
pub async fn get_cost_component_size_mib<C: GenericClient>(
    client: Option<&C>,
    cost: &AccountCostsDb,
    content: Option<&CostContent<'_>>,
) -> AlephResult<Option<f64>> {
    match cost.r#type {
        CostType::ExecutionInstanceVolumeRootfs => {
            if let Some(content) = content {
                if content.kind == CostContentKind::Instance {
                    let size = content
                        .value
                        .get("rootfs")
                        .and_then(|r| r.get("size_mib"))
                        .and_then(|v| v.as_u64())
                        .map(|n| n as f64);
                    return Ok(size);
                }
            }
            Ok(None)
        }
        CostType::ExecutionVolumePersistent => {
            if let Some(content) = content {
                if matches!(
                    content.kind,
                    CostContentKind::Instance | CostContentKind::Program
                ) {
                    if let Some((idx_str, _)) = cost.name.split_once(':') {
                        if let Ok(idx) = idx_str.trim_start_matches('#').parse::<usize>() {
                            if let Some(arr) =
                                content.value.get("volumes").and_then(|v| v.as_array())
                            {
                                if let Some(v) = arr.get(idx) {
                                    if let Some(sz) = v.get("size_mib").and_then(|v| v.as_u64()) {
                                        return Ok(Some(sz as f64));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Ok(None)
        }
        CostType::Storage
        | CostType::ExecutionProgramVolumeCode
        | CostType::ExecutionProgramVolumeRuntime
        | CostType::ExecutionProgramVolumeData
        | CostType::ExecutionVolumeInmutable => {
            if let Some(client) = client {
                let store_msg_hash: Option<&str> = match cost.r#ref.as_deref() {
                    Some(r) => Some(r),
                    None if cost.r#type == CostType::Storage => Some(cost.item_hash.as_str()),
                    _ => None,
                };
                if let Some(msg_hash) = store_msg_hash {
                    if let Some(pin) = get_message_file_pin(client, msg_hash).await? {
                        let size = get_size_from_file_ref(client, &pin.file_hash).await?;
                        if size.is_some() {
                            return Ok(size);
                        }
                    }
                }
            }
            if let Some(content) = content {
                return Ok(get_estimated_size_from_content(cost, content));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::toolkit::constants::DEFAULT_PRICE_AGGREGATE;
    use serde_json::json;
    use std::str::FromStr;

    /// Build a `Settings` from the `DEFAULT_SETTINGS_AGGREGATE` constant.
    fn default_settings() -> Settings {
        Settings::from_aggregate(&DEFAULT_SETTINGS_AGGREGATE)
    }

    /// Fetch the pricing for a given product type out of the default
    /// aggregate.
    fn default_pricing(price_type: ProductPriceType) -> ProductPricing {
        ProductPricing::from_aggregate(price_type, &DEFAULT_PRICE_AGGREGATE).unwrap()
    }

    // ----- Helper builders mirroring the Python fixtures.

    fn hold_instance_message() -> Value {
        json!({
            "time": 1701099523.849,
            "rootfs": {
                "parent": {"ref": "6e30de68", "use_latest": true},
                "size_mib": 20480,
                "persistence": "host"
            },
            "address": "0xA07B1214bAe0D5ccAA25449C3149c0aC83658874",
            "volumes": [],
            "metadata": {"name": "Test Debian 12"},
            "resources": {"vcpus": 1, "memory": 2048, "seconds": 30},
            "allow_amend": false,
            "environment": {"internet": true, "aleph_api": true, "reproducible": false, "shared_cache": false}
        })
    }

    fn confidential_instance_message() -> Value {
        let mut m = hold_instance_message();
        let env = m.get_mut("environment").unwrap().as_object_mut().unwrap();
        env.insert("hypervisor".into(), json!("qemu"));
        env.insert(
            "trusted_execution".into(),
            json!({"policy": 1, "firmware": "abc"}),
        );
        m
    }

    fn flow_instance_message() -> Value {
        let mut m = hold_instance_message();
        m.as_object_mut().unwrap().insert(
            "payment".into(),
            json!({"chain": "AVAX", "receiver": "0xA07...", "type": "superfluid"}),
        );
        m
    }

    fn gpu_instance_message_rtx4090() -> Value {
        let mut m = hold_instance_message();
        m["resources"] = json!({"vcpus": 8, "memory": 16384, "seconds": 30});
        m.as_object_mut().unwrap().insert(
            "requirements".into(),
            json!({
                "node": {"node_hash": "abc"},
                "gpu": [{
                    "vendor": "NVIDIA",
                    "device_name": "AD102 [GeForce RTX 4090]",
                    "device_class": "0300",
                    "device_id": "10de:2684"
                }]
            }),
        );
        m
    }

    fn gpu_instance_message_2x_a100() -> Value {
        let mut m = hold_instance_message();
        m["resources"] = json!({"vcpus": 16, "memory": 32768, "seconds": 30});
        m.as_object_mut().unwrap().insert(
            "requirements".into(),
            json!({
                "node": {"node_hash": "abc"},
                "gpu": [
                    {"vendor": "NVIDIA", "device_name": "GA100", "device_class": "0300", "device_id": "10de:20b2"},
                    {"vendor": "NVIDIA", "device_name": "GA100", "device_class": "0300", "device_id": "10de:20b2"}
                ]
            }),
        );
        m
    }

    fn gpu_instance_mixed_tier() -> Value {
        let mut m = hold_instance_message();
        m["resources"] = json!({"vcpus": 12, "memory": 24576, "seconds": 30});
        m.as_object_mut().unwrap().insert(
            "requirements".into(),
            json!({
                "node": {"node_hash": "abc"},
                "gpu": [
                    {"vendor": "NVIDIA", "device_name": "GA100", "device_class": "0300", "device_id": "10de:20b2"},
                    {"vendor": "NVIDIA", "device_name": "RTX 4090", "device_class": "0300", "device_id": "10de:2684"}
                ]
            }),
        );
        m
    }

    // ----- Plain function-level tests (no DB).

    #[test]
    fn payment_type_detection() {
        let m = hold_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        assert_eq!(get_payment_type(&content), PaymentType::Hold);

        let m = flow_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        assert_eq!(get_payment_type(&content), PaymentType::Superfluid);
    }

    #[test]
    fn product_type_resolution() {
        let settings = default_settings();
        let agg = &*DEFAULT_PRICE_AGGREGATE;

        // Plain instance
        let m = hold_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let t = get_product_price_type(&content, &settings, agg).unwrap();
        assert_eq!(t, ProductPriceType::Instance);

        // Confidential instance
        let m = confidential_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let t = get_product_price_type(&content, &settings, agg).unwrap();
        assert_eq!(t, ProductPriceType::InstanceConfidential);
    }

    #[test]
    fn compute_units_basic() {
        let m = hold_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let cu = ProductComputeUnit::new(1, 20480, 2048);
        // vcpus=1, memory=2048/2048=1 → max=1
        assert_eq!(get_nb_compute_units(&content, Some(&cu)), 1);
    }

    #[test]
    fn compute_unit_multiplier_for_program_with_internet() {
        let v = json!({
            "address": "0x", "resources": {"vcpus":1,"memory":128},
            "code": {"encoding":"zip","entrypoint":"main:app","ref":"x"},
            "runtime": {"ref":"x","comment":""},
            "on": {"http": true, "persistent": false},
            "environment": {"internet": true, "aleph_api": true, "reproducible": false, "shared_cache": false},
            "volumes": []
        });
        let content = CostContent::new(CostContentKind::Program, &v);
        assert_eq!(get_compute_unit_multiplier(&content), 2);
    }

    #[test]
    fn compute_unit_multiplier_for_persistent_program() {
        let v = json!({
            "address": "0x", "resources": {"vcpus":1,"memory":128},
            "code": {"encoding":"zip","entrypoint":"main:app","ref":"x"},
            "runtime": {"ref":"x","comment":""},
            "on": {"http": true, "persistent": true},
            "environment": {"internet": true, "aleph_api": true, "reproducible": false, "shared_cache": false},
            "volumes": []
        });
        let content = CostContent::new(CostContentKind::Program, &v);
        assert_eq!(get_compute_unit_multiplier(&content), 1);
    }

    // ----- GPU breakdown tests (no DB).

    #[test]
    fn gpu_breakdown_single_rtx4090() {
        let settings = default_settings();
        let m = gpu_instance_message_rtx4090();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let agg = &*DEFAULT_PRICE_AGGREGATE;
        let premium = default_pricing(ProductPriceType::InstanceGpuPremium);
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let b = get_gpu_tier_breakdown(&content, &settings, &premium, &standard).unwrap();
        assert_eq!(b.get(&ProductPriceType::InstanceGpuStandard), Some(&6));
        assert!(!b.contains_key(&ProductPriceType::InstanceGpuPremium));
        // get_product_price_type should resolve to standard
        let t = get_product_price_type(&content, &settings, agg).unwrap();
        assert_eq!(t, ProductPriceType::InstanceGpuStandard);
    }

    #[test]
    fn gpu_breakdown_2x_a100() {
        let settings = default_settings();
        let m = gpu_instance_message_2x_a100();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let premium = default_pricing(ProductPriceType::InstanceGpuPremium);
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let b = get_gpu_tier_breakdown(&content, &settings, &premium, &standard).unwrap();
        assert_eq!(b.get(&ProductPriceType::InstanceGpuPremium), Some(&32));
    }

    #[test]
    fn gpu_breakdown_mixed_tier() {
        let settings = default_settings();
        let m = gpu_instance_mixed_tier();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let premium = default_pricing(ProductPriceType::InstanceGpuPremium);
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let b = get_gpu_tier_breakdown(&content, &settings, &premium, &standard).unwrap();
        assert_eq!(b.get(&ProductPriceType::InstanceGpuPremium), Some(&16));
        assert_eq!(b.get(&ProductPriceType::InstanceGpuStandard), Some(&6));
    }

    #[test]
    fn gpu_unknown_device_id() {
        let settings = default_settings();
        let mut m = hold_instance_message();
        m.as_object_mut().unwrap().insert(
            "requirements".into(),
            json!({"gpu": [{"vendor": "NVIDIA", "device_id": "ffff:ffff", "device_name": "?", "device_class": "0300"}]}),
        );
        let content = CostContent::new(CostContentKind::Instance, &m);
        let premium = default_pricing(ProductPriceType::InstanceGpuPremium);
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let err = get_gpu_tier_breakdown(&content, &settings, &premium, &standard).unwrap_err();
        assert!(matches!(err, CostError::GpuNotCompatible { .. }));
    }

    // ----- Settings round-trip.

    #[test]
    fn settings_from_default_aggregate() {
        let s = default_settings();
        assert_eq!(
            s.community_wallet_address,
            "0x5aBd3258C5492fD378EBC2e0017416E199e5Da56"
        );
        assert_eq!(s.community_wallet_timestamp, 1_739_301_770);
        assert!(s.compatible_gpus.iter().any(|g| g.device_id == "10de:2684"));
    }

    // ----- Sanity arithmetic checks against Python fixtures.
    //
    // These reproduce the per-component math without a database. The fixture
    // expected totals come from `tests/services/test_cost_service.py`.

    /// Non-GPU compute cost: 1 vCPU, 2048 MiB → 1 CU × $1000 = $1000.
    #[test]
    fn compute_hold_instance_cost_matches_python() {
        let pricing = default_pricing(ProductPriceType::Instance);
        let m = hold_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let nb = get_nb_compute_units(&content, pricing.compute_unit.as_ref());
        let mul = get_compute_unit_multiplier(&content);
        assert_eq!(nb, 1);
        assert_eq!(mul, 1);
        let cu = pricing.price.compute_unit.as_ref().unwrap();
        let exec_cost = Decimal::from(nb) * Decimal::from(mul) * cu.holding;
        // Python: cost == 1000
        assert_eq!(format_cost(exec_cost, None), Decimal::from(1000));
    }

    /// Confidential VM: doubled compute cost ($2000).
    #[test]
    fn compute_hold_confidential_cost_matches_python() {
        let pricing = default_pricing(ProductPriceType::InstanceConfidential);
        let m = confidential_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let cu = pricing.price.compute_unit.as_ref().unwrap();
        let nb = get_nb_compute_units(&content, pricing.compute_unit.as_ref());
        let mul = get_compute_unit_multiplier(&content);
        let exec = Decimal::from(nb) * Decimal::from(mul) * cu.holding;
        assert_eq!(format_cost(exec, None), Decimal::from(2000));
    }

    /// Superfluid flow rate: $1000/hour ÷ 3600 = $0.27777.../sec, floored to
    /// 18 decimals.
    #[test]
    fn compute_flow_instance_rate_matches_python() {
        let pricing = default_pricing(ProductPriceType::Instance);
        let m = flow_instance_message();
        let content = CostContent::new(CostContentKind::Instance, &m);
        let cu = pricing.price.compute_unit.as_ref().unwrap();
        let nb = get_nb_compute_units(&content, pricing.compute_unit.as_ref());
        let mul = get_compute_unit_multiplier(&content);
        let rate = Decimal::from(nb) * Decimal::from(mul) * cu.payg / Decimal::from(HOUR);
        // Python test expects 0.000015277777777777
        let expected = Decimal::from_str("0.000015277777777777").unwrap();
        assert_eq!(format_cost(rate, None), expected);
    }

    /// GPU single RTX 4090: 8 CUs × $0.28 = $2.24/hour, holding cost $2240.
    #[test]
    fn gpu_single_rtx4090_hold_cost_matches_python() {
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let m = gpu_instance_message_rtx4090();
        let content = CostContent::new(CostContentKind::Instance, &m);
        // resource-based CUs (max(8, ceil(16384/6144)=3)) = 8
        let nb = get_nb_compute_units(&content, standard.compute_unit.as_ref());
        assert_eq!(nb, 8);
        let cu = standard.price.compute_unit.as_ref().unwrap();
        let exec_cost = Decimal::from(8) * cu.holding;
        // Python: 8 × $0.28 × 1000 = ... but holding is "280" not 0.28
        // 8 × 280 = 2240
        assert_eq!(format_cost(exec_cost, None), Decimal::from(2240));
    }

    /// Multi-tier GPU: 16 CU (premium) × $560 + 6 CU (standard) × $280 = $10640.
    #[test]
    fn gpu_mixed_tier_hold_cost_matches_python() {
        let premium = default_pricing(ProductPriceType::InstanceGpuPremium);
        let standard = default_pricing(ProductPriceType::InstanceGpuStandard);
        let premium_cost = Decimal::from(16) * premium.price.compute_unit.as_ref().unwrap().holding;
        let standard_cost =
            Decimal::from(6) * standard.price.compute_unit.as_ref().unwrap().holding;
        assert_eq!(format_cost(premium_cost, None), Decimal::from(8960));
        assert_eq!(format_cost(standard_cost, None), Decimal::from(1680));
        assert_eq!(
            format_cost(premium_cost + standard_cost, None),
            Decimal::from(10640)
        );
    }

    #[test]
    fn min_credit_cost_floor() {
        // A 1-MiB storage volume at the default credit rate
        // (0.17967489030626108 / 3600 per second) costs roughly
        // 0.0000499... credit per second, which is *less* than 1/3600
        // (0.000277...) so the floor kicks in.
        let pricing = default_pricing(ProductPriceType::Instance);
        let rate_per_s = pricing.price.storage.credit / Decimal::from(HOUR);
        let candidate = format_cost(Decimal::from(1) * rate_per_s, None);
        let floor = format_cost(
            Decimal::from(MIN_CREDIT_COST_PER_HOUR) / Decimal::from(HOUR),
            None,
        );
        let effective = candidate.max(floor);
        // floor wins
        assert_eq!(effective, floor);
        // hourly cost ceil(effective × HOUR) ≥ MIN_CREDIT_COST_PER_HOUR.
        // (Floor-quantising at 18 dp means raw multiplication may fall a hair
        // short of HOUR; Python's test uses `ceil` for the same reason.)
        let hourly = effective * Decimal::from(HOUR);
        let hourly_ceil =
            hourly.round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToPositiveInfinity);
        assert!(hourly_ceil >= Decimal::from(MIN_CREDIT_COST_PER_HOUR));
        assert!(hourly <= Decimal::from(MIN_CREDIT_COST_PER_HOUR));
    }
}
