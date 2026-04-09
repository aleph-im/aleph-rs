use crate::cli::{InstanceCommand, InstanceCreateArgs, parse_size_to_mib};
use crate::common::{resolve_account, resolve_address, submit_or_preview};
use aleph_sdk::client::{AlephAggregateClient, AlephClient};
use aleph_sdk::messages::InstanceBuilder;
use aleph_types::channel::Channel;
use aleph_types::message::execution::base::{Payment, PaymentType};
use aleph_types::message::execution::environment::Hypervisor;
use aleph_types::message::execution::volume::{
    BaseVolume, EphemeralVolume, ImmutableVolume, MachineVolume, PersistentVolume,
    PersistentVolumeSize, VolumePersistence,
};
use memsizes::MiB;
use url::Url;

pub async fn handle_instance_command(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    command: InstanceCommand,
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        InstanceCommand::Create(args) => {
            handle_instance_create(aleph_client, ccn_url, json, args).await?;
        }
    }
    Ok(())
}

const SSH_PUBKEY_PREFIXES: &[&str] = &[
    "ssh-rsa",
    "ssh-ed25519",
    "ssh-dss",
    "ecdsa-sha2-nistp256",
    "ecdsa-sha2-nistp384",
    "ecdsa-sha2-nistp521",
    "sk-ssh-ed25519@openssh.com",
    "sk-ecdsa-sha2-nistp256@openssh.com",
];

fn validate_ssh_pubkey(
    key: &str,
    path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let has_valid_prefix = SSH_PUBKEY_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix));
    if !has_valid_prefix {
        return Err(format!(
            "'{}' does not look like an SSH public key (expected a line starting with ssh-rsa, ssh-ed25519, etc.)",
            path.display()
        )
        .into());
    }
    Ok(())
}

/// Parse a "key=value,key=value" string into a list of (key, value) pairs.
fn parse_kv_pairs(s: &str) -> Result<Vec<(&str, &str)>, String> {
    s.split(',')
        .map(|pair| {
            let (k, v) = pair
                .split_once('=')
                .ok_or_else(|| format!("invalid key=value pair: '{pair}'"))?;
            Ok((k.trim(), v.trim()))
        })
        .collect()
}

fn parse_persistent_volumes(
    specs: &[String],
) -> Result<Vec<MachineVolume>, Box<dyn std::error::Error>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec)?;
            let mut name: Option<String> = None;
            let mut mount: Option<String> = None;
            let mut size_mib: Option<u64> = None;
            let mut persistence: Option<VolumePersistence> = None;
            for (k, v) in pairs {
                match k {
                    "name" => name = Some(v.to_string()),
                    "mount" => mount = Some(v.to_string()),
                    "size" => size_mib = Some(parse_size_to_mib(v)?),
                    "persistence" => {
                        persistence = Some(match v {
                            "host" => VolumePersistence::Host,
                            "store" => VolumePersistence::Store,
                            _ => return Err(format!("invalid persistence: '{v}'").into()),
                        })
                    }
                    _ => return Err(format!("unknown persistent volume key: '{k}'").into()),
                }
            }
            let size_mib = size_mib.ok_or("persistent volume requires size")?;
            let mount = mount.ok_or("persistent volume requires mount")?;
            Ok(MachineVolume::Persistent(PersistentVolume {
                base: BaseVolume {
                    comment: None,
                    mount: Some(mount.into()),
                },
                parent: None,
                persistence,
                name,
                size_mib: PersistentVolumeSize::try_from(size_mib)?,
            }))
        })
        .collect()
}

fn parse_ephemeral_volumes(
    specs: &[String],
) -> Result<Vec<MachineVolume>, Box<dyn std::error::Error>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec)?;
            let mut mount: Option<String> = None;
            let mut size_mib: Option<u64> = None;
            for (k, v) in pairs {
                match k {
                    "mount" => mount = Some(v.to_string()),
                    "size" => size_mib = Some(parse_size_to_mib(v)?),
                    _ => return Err(format!("unknown ephemeral volume key: '{k}'").into()),
                }
            }
            let size_mib = size_mib.ok_or("ephemeral volume requires size")?;
            let mount = mount.ok_or("ephemeral volume requires mount")?;
            Ok(MachineVolume::Ephemeral(EphemeralVolume::new(
                size_mib, mount,
            )?))
        })
        .collect()
}

fn parse_immutable_volumes(
    specs: &[String],
) -> Result<Vec<MachineVolume>, Box<dyn std::error::Error>> {
    specs
        .iter()
        .map(|spec| {
            let pairs = parse_kv_pairs(spec)?;
            let mut reference: Option<String> = None;
            let mut mount: Option<String> = None;
            let mut use_latest = true;
            for (k, v) in pairs {
                match k {
                    "ref" => reference = Some(v.to_string()),
                    "mount" => mount = Some(v.to_string()),
                    "use_latest" => {
                        use_latest = v
                            .parse()
                            .map_err(|_| format!("invalid use_latest: '{v}'"))?
                    }
                    _ => return Err(format!("unknown immutable volume key: '{k}'").into()),
                }
            }
            let reference = reference.ok_or("immutable volume requires ref")?;
            let mount = mount.ok_or("immutable volume requires mount")?;
            let item_hash = reference.parse().map_err(|e| format!("invalid ref: {e}"))?;
            Ok(MachineVolume::Immutable(ImmutableVolume {
                base: BaseVolume {
                    comment: None,
                    mount: Some(mount.into()),
                },
                reference: item_hash,
                use_latest,
            }))
        })
        .collect()
}

async fn handle_instance_create(
    aleph_client: &AlephClient,
    ccn_url: &Url,
    json: bool,
    args: InstanceCreateArgs,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = args.signing.dry_run;
    let account = resolve_account(&args.signing)?;

    // Read and validate SSH public keys
    let mut ssh_keys = Vec::new();
    for path in &args.ssh_pubkey_file {
        let content = std::fs::read_to_string(path).map_err(|e| {
            format!(
                "failed to read SSH public key file '{}': {e}",
                path.display()
            )
        })?;
        let key = content.trim().to_string();
        validate_ssh_pubkey(&key, path)?;
        ssh_keys.push(key);
    }

    // Resolve instance specs: either from --size (tier lookup) or explicit flags.
    let (vcpus, memory_mib, disk_size_mib) = if let Some(slug) = &args.size {
        let pricing = aleph_client
            .get_pricing_aggregate()
            .await
            .map_err(|e| format!("failed to fetch pricing tiers: {e}"))?;
        let instance_pricing = &pricing.pricing.instance;

        let tier = instance_pricing.find_tier_by_slug(slug).ok_or_else(|| {
            let available = instance_pricing.available_slugs().join(", ");
            format!("unknown size '{slug}'. Available sizes: {available}")
        })?;

        let vcpus = args.vcpus.unwrap_or(tier.vcpus);
        let memory_mib = args.memory.unwrap_or(tier.memory_mib);
        let disk_size_mib = args.disk_size.unwrap_or(tier.disk_mib);

        eprintln!(
            "Size '{slug}': {vcpus} vCPUs, {} MiB memory, {} MiB disk",
            memory_mib, disk_size_mib,
        );

        (vcpus, memory_mib, disk_size_mib)
    } else {
        let disk_size_mib = args
            .disk_size
            .ok_or("--disk-size is required when --size is not used (or use --size to specify a tier slug like 1vcpu-2gb)")?;
        let vcpus = args.vcpus.unwrap_or(1);
        let memory_mib = args.memory.unwrap_or(2048);
        (vcpus, memory_mib, disk_size_mib)
    };

    let disk_size = PersistentVolumeSize::try_from(disk_size_mib)
        .map_err(|e| format!("invalid disk size: {e}"))?;

    let mut builder = InstanceBuilder::new(&account, args.image, disk_size)
        .vcpus(vcpus)
        .memory(MiB::from(memory_mib))
        .hypervisor(Hypervisor::Qemu)
        .payment(Payment {
            chain: None,
            receiver: None,
            payment_type: PaymentType::Credit,
        })
        .ssh_keys(ssh_keys);

    if let Some(owner) = args.on_behalf_of {
        builder = builder.on_behalf_of(resolve_address(&owner)?);
    }

    if let Some(name) = args.name {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("name".to_string(), serde_json::json!(name));
        builder = builder.metadata(metadata);
    }

    // Parse volumes
    let mut volumes = Vec::new();
    if let Some(specs) = &args.persistent_volume {
        volumes.extend(parse_persistent_volumes(specs)?);
    }
    if let Some(specs) = &args.ephemeral_volume {
        volumes.extend(parse_ephemeral_volumes(specs)?);
    }
    if let Some(specs) = &args.immutable_volume {
        volumes.extend(parse_immutable_volumes(specs)?);
    }
    if !volumes.is_empty() {
        builder = builder.volumes(volumes);
    }

    if let Some(ch) = args.channel {
        builder = builder.channel(Channel::from(ch));
    }

    let pending = builder.build()?;
    submit_or_preview(aleph_client, ccn_url, &pending, dry_run, json).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{parse_image, parse_size_to_mib};

    #[test]
    fn parse_kv_pairs_basic() {
        let pairs = parse_kv_pairs("name=data,mount=/opt/data,size=1GiB").unwrap();
        assert_eq!(
            pairs,
            vec![("name", "data"), ("mount", "/opt/data"), ("size", "1GiB")]
        );
    }

    #[test]
    fn parse_kv_pairs_missing_equals() {
        assert!(parse_kv_pairs("invalid").is_err());
    }

    #[test]
    fn parse_size_binary_units() {
        assert_eq!(parse_size_to_mib("100MiB").unwrap(), 100);
        assert_eq!(parse_size_to_mib("1GiB").unwrap(), 1024);
        assert_eq!(parse_size_to_mib("2GiB").unwrap(), 2048);
        assert_eq!(parse_size_to_mib("1TiB").unwrap(), 1024 * 1024);
    }

    #[test]
    fn parse_size_decimal_units() {
        // 1 GB = 1_000_000_000 bytes = ~953.674 MiB
        assert_eq!(parse_size_to_mib("1GB").unwrap(), 954);
        // 20 GB = ~19073.486 MiB
        assert_eq!(parse_size_to_mib("20GB").unwrap(), 19073);
        // 100 MB = ~95.367 MiB
        assert_eq!(parse_size_to_mib("100MB").unwrap(), 95);
    }

    #[test]
    fn parse_size_case_insensitive() {
        assert_eq!(parse_size_to_mib("1gib").unwrap(), 1024);
        assert_eq!(parse_size_to_mib("1GIB").unwrap(), 1024);
        assert_eq!(
            parse_size_to_mib("1gb").unwrap(),
            parse_size_to_mib("1GB").unwrap()
        );
    }

    #[test]
    fn parse_size_rejects_bare_numbers() {
        assert!(parse_size_to_mib("1024").is_err());
    }

    #[test]
    fn parse_size_rejects_unknown_units() {
        assert!(parse_size_to_mib("100KiB").is_err());
    }

    #[test]
    fn parse_persistent_volume_basic() {
        let specs = vec!["name=data,mount=/opt/data,size=1GiB".to_string()];
        let volumes = parse_persistent_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        assert!(matches!(volumes[0], MachineVolume::Persistent(_)));
    }

    #[test]
    fn parse_persistent_volume_with_persistence() {
        let specs = vec!["name=db,mount=/var/db,size=500MiB,persistence=store".to_string()];
        let volumes = parse_persistent_volumes(&specs).unwrap();
        if let MachineVolume::Persistent(v) = &volumes[0] {
            assert_eq!(v.persistence, Some(VolumePersistence::Store));
            assert_eq!(v.name, Some("db".to_string()));
        } else {
            panic!("expected persistent volume");
        }
    }

    #[test]
    fn parse_persistent_volume_missing_size() {
        let specs = vec!["name=data,mount=/opt/data".to_string()];
        assert!(parse_persistent_volumes(&specs).is_err());
    }

    #[test]
    fn parse_persistent_volume_missing_mount() {
        let specs = vec!["name=data,size=1GiB".to_string()];
        assert!(parse_persistent_volumes(&specs).is_err());
    }

    #[test]
    fn parse_ephemeral_volume_basic() {
        let specs = vec!["mount=/tmp/scratch,size=100MiB".to_string()];
        let volumes = parse_ephemeral_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        assert!(matches!(volumes[0], MachineVolume::Ephemeral(_)));
    }

    #[test]
    fn parse_ephemeral_volume_missing_mount() {
        let specs = vec!["size=100MiB".to_string()];
        assert!(parse_ephemeral_volumes(&specs).is_err());
    }

    #[test]
    fn parse_immutable_volume_basic() {
        let specs = vec![
            "ref=d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c,mount=/opt/pkg"
                .to_string(),
        ];
        let volumes = parse_immutable_volumes(&specs).unwrap();
        assert_eq!(volumes.len(), 1);
        if let MachineVolume::Immutable(v) = &volumes[0] {
            assert!(v.use_latest); // default
        } else {
            panic!("expected immutable volume");
        }
    }

    #[test]
    fn parse_immutable_volume_use_latest_false() {
        let specs = vec![
            "ref=d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c,mount=/opt/pkg,use_latest=false"
                .to_string(),
        ];
        let volumes = parse_immutable_volumes(&specs).unwrap();
        if let MachineVolume::Immutable(v) = &volumes[0] {
            assert!(!v.use_latest);
        } else {
            panic!("expected immutable volume");
        }
    }

    #[test]
    fn parse_immutable_volume_missing_ref() {
        let specs = vec!["mount=/opt/pkg".to_string()];
        assert!(parse_immutable_volumes(&specs).is_err());
    }

    #[test]
    fn parse_multiple_volumes() {
        let persistent = vec![
            "name=a,mount=/a,size=100MiB".to_string(),
            "name=b,mount=/b,size=200MiB".to_string(),
        ];
        let volumes = parse_persistent_volumes(&persistent).unwrap();
        assert_eq!(volumes.len(), 2);
    }

    #[test]
    fn validate_ssh_pubkey_accepts_valid_keys() {
        let path = std::path::Path::new("test.pub");
        validate_ssh_pubkey("ssh-rsa AAAAB3... user@host", path).unwrap();
        validate_ssh_pubkey("ssh-ed25519 AAAAC3... user@host", path).unwrap();
        validate_ssh_pubkey("ecdsa-sha2-nistp256 AAAAE2... user@host", path).unwrap();
        validate_ssh_pubkey("sk-ssh-ed25519@openssh.com AAAAG... user@host", path).unwrap();
    }

    #[test]
    fn validate_ssh_pubkey_rejects_private_key() {
        let path = std::path::Path::new("id_rsa");
        assert!(validate_ssh_pubkey("-----BEGIN OPENSSH PRIVATE KEY-----", path).is_err());
    }

    #[test]
    fn validate_ssh_pubkey_rejects_garbage() {
        let path = std::path::Path::new("garbage.txt");
        assert!(validate_ssh_pubkey("not a key at all", path).is_err());
    }

    #[test]
    fn parse_image_preset_ubuntu24() {
        let hash = parse_image("ubuntu24").unwrap();
        assert_eq!(
            hash.to_string(),
            "5330dcefe1857bcd97b7b7f24d1420a7d46232d53f27be280c8a7071d88bd84e"
        );
    }

    #[test]
    fn parse_image_preset_case_insensitive() {
        let hash = parse_image("Ubuntu22").unwrap();
        assert_eq!(
            hash.to_string(),
            "4a0f62da42f4478544616519e6f5d58adb1096e069b392b151d47c3609492d0c"
        );
    }

    #[test]
    fn parse_image_raw_hash() {
        let hash = parse_image("d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c")
            .unwrap();
        assert_eq!(
            hash.to_string(),
            "d281eb8a69ba1f4dda2d71aaf3ded06caa92edd690ef3d0632f41aa91167762c"
        );
    }

    #[test]
    fn parse_image_ipfs_cid() {
        let hash = parse_image("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG").unwrap();
        assert!(matches!(hash, aleph_types::item_hash::ItemHash::Ipfs(_)));
    }

    #[test]
    fn parse_image_invalid() {
        assert!(parse_image("windows11").is_err());
        assert!(parse_image("abc").is_err());
    }
}
