//! Local UnixFS DAG construction for folder uploads.
//!
//! Computes the same root CID kubo produces via `ipfs add -r`, given the
//! same flat list of file paths. Used by `AlephClient::upload_folder_to_ipfs`
//! to verify that the IPFS gateway returns the expected CID after upload.
//!
//! Two public entry points share the same walk:
//! `hash_folder_root` discards the block bytes and returns just the root
//! CID; `build_folder_dag` invokes a caller-supplied sink for each
//! `(cid_bytes, block_bytes)` pair as the walk progresses, which the
//! authenticated CAR upload path uses to stream the DAG into a tempfile.
//!
//! # Test goldens
//!
//! Golden CIDs in `tests/folder_hash.rs` are regenerated against real kubo
//! via `tests/regen-folder-hash-goldens.sh`. Run it after any change that
//! could affect output bytes.

use crate::ipfs::{CidVersion, FolderEntry, UploadFolderOptions};
use crate::proto::{merkledag, unixfs};
use crate::verify::{DAG_PB_CODEC, DagNode, Hasher, encode_multihash, encode_pbnode_canonical};
use aleph_types::item_hash::ItemHash;
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum FolderHashError {
    #[error("I/O error reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("HAMT recursion exceeded 8 levels (impossible hash collision)")]
    HamtDepthExceeded,
    #[error("sink error: {0}")]
    Sink(#[source] std::io::Error),
}

/// Visitor sink for `build_folder_dag`.
///
/// Called once per emitted block as `(cid_bytes, block_bytes)`. For dag-pb
/// nodes `block_bytes` is the canonical pbnode encoding; for raw leaves it is
/// the raw chunk. The root block is always emitted last.
pub type BlockSink<'a> = dyn FnMut(&[u8], &[u8]) -> std::io::Result<()> + 'a;

/// A node in the in-memory folder tree, before hashing.
#[derive(Debug)]
enum TreeNode {
    File(PathBuf),
    Dir(BTreeMap<String, TreeNode>),
}

/// Build a nested tree from a flat list of `FolderEntry` whose `relative_path`
/// is forward-slash separated. The returned map represents the root directory.
fn build_tree(entries: &[FolderEntry]) -> BTreeMap<String, TreeNode> {
    let mut root: BTreeMap<String, TreeNode> = BTreeMap::new();
    for entry in entries {
        let mut cursor = &mut root;
        let segments: Vec<&str> = entry.relative_path.split('/').collect();
        let (last, parents) = segments.split_last().expect("non-empty relative path");
        for part in parents {
            let next = cursor
                .entry((*part).to_string())
                .or_insert_with(|| TreeNode::Dir(BTreeMap::new()));
            cursor = match next {
                TreeNode::Dir(map) => map,
                TreeNode::File(_) => {
                    panic!(
                        "folder tree: {} is both a file and a directory prefix",
                        part
                    );
                }
            };
        }
        cursor.insert(
            (*last).to_string(),
            TreeNode::File(entry.absolute_path.clone()),
        );
    }
    root
}

/// One entry to attach under a directory or HAMT node.
#[derive(Debug, Clone)]
pub(crate) struct ChildLink {
    pub name: String,
    /// CID bytes as stored in PBLink.Hash. CIDv0: bare multihash. CIDv1: full binary.
    pub cid_bytes: Vec<u8>,
    pub cumulative_size: u64,
}

/// Wrap a sha2-256 multihash as either a CIDv1 dag-pb binary (version byte
/// 0x01 + codec 0x70 + multihash) or a bare CIDv0 multihash.
fn build_cid_bytes(multihash: Vec<u8>, cid_v1: bool) -> Vec<u8> {
    if cid_v1 {
        let mut cid = Vec::with_capacity(2 + multihash.len());
        cid.push(0x01);
        cid.push(DAG_PB_CODEC as u8);
        cid.extend_from_slice(&multihash);
        cid
    } else {
        multihash
    }
}

/// Build a plain UnixFS Directory node from a list of children.
///
/// `cid_v1`: when `true`, returns a CIDv1 dag-pb CID (codec 0x70). When `false`,
/// returns a bare multihash (CIDv0).
///
/// Children must already be sorted by `name` (callers use a `BTreeMap` so this
/// is automatic).
// .expect() is safe here: the only fallible path in the sinkful variant is
// the sink itself, and a no-op sink never errors. (build_hamt_root cannot
// take the same shape because its sinkful variant can also fail with
// HamtDepthExceeded.)
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn build_plain_directory(children: &[ChildLink], cid_v1: bool) -> DagNode {
    build_plain_directory_with_sink(children, cid_v1, &mut |_, _| Ok(()))
        .expect("no-op sink cannot fail")
}

fn build_plain_directory_with_sink(
    children: &[ChildLink],
    cid_v1: bool,
    sink: &mut BlockSink<'_>,
) -> Result<DagNode, FolderHashError> {
    let links: Vec<merkledag::PbLink> = children
        .iter()
        .map(|c| merkledag::PbLink {
            hash: Some(c.cid_bytes.clone()),
            name: Some(c.name.clone()),
            tsize: Some(c.cumulative_size),
        })
        .collect();

    let dir_data = unixfs::Data {
        r#type: unixfs::DataType::Directory as i32,
        data: None,
        filesize: None,
        blocksizes: vec![],
        hash_type: None,
        fanout: None,
    };
    let mut dir_bytes = Vec::new();
    dir_data
        .encode(&mut dir_bytes)
        .expect("protobuf encoding cannot fail");

    let node = merkledag::PbNode {
        links,
        data: Some(dir_bytes),
    };
    let node_bytes = encode_pbnode_canonical(&node);
    let digest = Sha256::digest(&node_bytes);
    let mh = encode_multihash(&digest);
    let cid_bytes = build_cid_bytes(mh, cid_v1);

    let node_size = node_bytes.len() as u64;
    let children_cumulative: u64 = children.iter().map(|c| c.cumulative_size).sum();

    let dag_node = DagNode {
        cid_bytes,
        cumulative_size: node_size + children_cumulative,
        data_size: 0, // directories don't expose a payload size
    };
    sink(&dag_node.cid_bytes, &node_bytes).map_err(FolderHashError::Sink)?;
    Ok(dag_node)
}

/// Kubo's HAMT sharding threshold.
///
/// Source: `github.com/ipfs/boxo/ipld/unixfs/io.HAMTShardingSize = 256 * 1024`.
const HAMT_SHARDING_SIZE: usize = 256 * 1024; // 262144 bytes

/// Estimate the bare-directory size as kubo does.
///
/// Source: boxo `io/directory.go` `productionLinkSize`:
/// `len(linkName) + linkCid.ByteLen()` — name bytes plus the binary CID size.
pub(crate) fn estimated_dir_size(children: &[ChildLink]) -> usize {
    children
        .iter()
        .map(|c| c.name.len() + c.cid_bytes.len())
        .sum()
}

/// Returns true when the directory should be encoded as a HAMT shard.
pub(crate) fn should_shard(children: &[ChildLink]) -> bool {
    estimated_dir_size(children) >= HAMT_SHARDING_SIZE
}

/// HAMT bitfield: set the bit for slot index `slot`.
///
/// Bit ordering matches go-unixfs HAMT:
/// - Bits within each byte are LSB-first (slot % 8 = 0 → bit 0, i.e. 0x01).
/// - Bytes are stored in reverse order: byte at serialized index 0 holds the
///   highest-numbered slots (slots 248–255), byte at index 31 holds slots 0–7.
///   Equivalently: serialized_byte_index = 31 - (slot / 8).
pub(crate) fn set_bit(bf: &mut [u8; 32], slot: u8) {
    let byte = 31 - (slot / 8) as usize;
    let bit = slot % 8;
    bf[byte] |= 1 << bit;
}

/// Test a HAMT bitfield bit. Same ordering as `set_bit`.
#[cfg(test)]
pub(crate) fn test_bit(bf: &[u8; 32], slot: u8) -> bool {
    let byte = 31 - (slot / 8) as usize;
    let bit = slot % 8;
    (bf[byte] & (1 << bit)) != 0
}

/// HAMT fanout: 256 buckets per shard.
const HAMT_FANOUT: u32 = 256;

/// UnixFS hashType code for murmur3-x64-64.
const HAMT_HASH_TYPE: u64 = 0x22;

/// Murmur3 seed used by go-ipfs HAMT.
const HAMT_MURMUR_SEED: u32 = 0;

/// Hash a directory entry name to a 64-bit murmur3 value (kubo-compatible).
///
/// Kubo uses spaolacci/murmur3 `Sum64`, which returns the LOW 64 bits of the
/// x64_128 hash. We reproduce that by computing x64_128 and downcasting.
fn hamt_hash_name(name: &str) -> u64 {
    use murmur3::murmur3_x64_128;
    use std::io::Cursor;
    let h128 = murmur3_x64_128(&mut Cursor::new(name.as_bytes()), HAMT_MURMUR_SEED)
        .expect("in-memory cursor read cannot fail");
    h128 as u64
}

/// Extract the 8-bit slot index for HAMT level `level` from a 64-bit hash.
///
/// Kubo consumes bits from the HIGH end of the hash first — level 0 uses
/// bits [56..64), level 1 uses bits [48..56), etc.
///
/// This matches go-unixfs `hashBits.Next(8)` which reads:
///   `(h.val >> (maxHashLen - consumed)) & mask`
/// where `maxHashLen = 64` and `consumed` advances by 8 per level.
fn hamt_slot(hash: u64, level: u32) -> u8 {
    ((hash >> (56 - 8 * level)) & 0xff) as u8
}

/// Build the root of a HAMT shard tree from `children`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn build_hamt_root(
    children: &[ChildLink],
    cid_v1: bool,
) -> Result<DagNode, FolderHashError> {
    build_hamt_root_with_sink(children, cid_v1, &mut |_, _| Ok(()))
}

fn build_hamt_root_with_sink(
    children: &[ChildLink],
    cid_v1: bool,
    sink: &mut BlockSink<'_>,
) -> Result<DagNode, FolderHashError> {
    let hashed: Vec<(u64, ChildLink)> = children
        .iter()
        .map(|c| (hamt_hash_name(&c.name), c.clone()))
        .collect();
    build_hamt_node_with_sink(&hashed, 0, cid_v1, sink)
}

fn build_hamt_node_with_sink(
    entries: &[(u64, ChildLink)],
    level: u32,
    cid_v1: bool,
    sink: &mut BlockSink<'_>,
) -> Result<DagNode, FolderHashError> {
    if level >= 8 {
        return Err(FolderHashError::HamtDepthExceeded);
    }

    // Bucket entries by slot.
    let mut buckets: [Vec<(u64, ChildLink)>; 256] = std::array::from_fn(|_| Vec::new());
    for (h, c) in entries {
        let slot = hamt_slot(*h, level) as usize;
        buckets[slot].push((*h, c.clone()));
    }

    let mut bitfield = [0u8; 32];
    let mut links: Vec<merkledag::PbLink> = Vec::new();
    let mut children_cumulative: u64 = 0;

    for (slot_idx, bucket) in buckets.iter().enumerate() {
        if bucket.is_empty() {
            continue;
        }
        set_bit(&mut bitfield, slot_idx as u8);

        let (link_name, child_dag) = if bucket.len() == 1 {
            // Leaf: link directly to the entry's CID with name "{slot_hex}{actual}".
            let (_, c) = &bucket[0];
            let name = format!("{:02X}{}", slot_idx, c.name);
            (
                name,
                DagNode {
                    cid_bytes: c.cid_bytes.clone(),
                    cumulative_size: c.cumulative_size,
                    data_size: 0,
                },
            )
        } else {
            // Subshard: recurse, link with name "{slot_hex}".
            let sub = build_hamt_node_with_sink(bucket, level + 1, cid_v1, sink)?;
            (format!("{slot_idx:02X}"), sub)
        };

        children_cumulative += child_dag.cumulative_size;
        links.push(merkledag::PbLink {
            hash: Some(child_dag.cid_bytes),
            name: Some(link_name),
            tsize: Some(child_dag.cumulative_size),
        });
    }

    // Sort links by name to match kubo. Since prefix is fixed 2-hex-char
    // width, this equals sort-by-slot.
    links.sort_by(|a, b| a.name.cmp(&b.name));

    // Serialize the bitfield like go-bitfield's Bytes(): trim leading zero bytes
    // (bytes at low indices, which hold the highest-numbered slots).
    let bf_vec: Vec<u8> = {
        let start = bitfield.iter().position(|&b| b != 0).unwrap_or(32);
        bitfield[start..].to_vec()
    };

    let unixfs_data = unixfs::Data {
        r#type: unixfs::DataType::HamtShard as i32,
        data: Some(bf_vec),
        filesize: None,
        blocksizes: vec![],
        hash_type: Some(HAMT_HASH_TYPE),
        fanout: Some(HAMT_FANOUT as u64),
    };
    let mut data_bytes = Vec::new();
    unixfs_data
        .encode(&mut data_bytes)
        .expect("protobuf encoding cannot fail");

    let node = merkledag::PbNode {
        links,
        data: Some(data_bytes),
    };
    let node_bytes = encode_pbnode_canonical(&node);
    let digest = Sha256::digest(&node_bytes);
    let mh = encode_multihash(&digest);
    let cid_bytes = build_cid_bytes(mh, cid_v1);

    let node_size = node_bytes.len() as u64;

    let dag_node = DagNode {
        cid_bytes,
        cumulative_size: node_size + children_cumulative,
        data_size: 0,
    };
    sink(&dag_node.cid_bytes, &node_bytes).map_err(FolderHashError::Sink)?;
    Ok(dag_node)
}

/// Walk the folder DAG, emitting every block to `sink`. Returns the root CID.
///
/// `sink` is invoked once per emitted block as `(cid_bytes, block_bytes)`. For
/// dag-pb nodes `block_bytes` is the canonical pbnode encoding; for raw leaves
/// it is the raw chunk. The root block is emitted last.
pub fn build_folder_dag(
    entries: &[FolderEntry],
    opts: &UploadFolderOptions,
    sink: &mut BlockSink<'_>,
) -> Result<ItemHash, FolderHashError> {
    let tree = build_tree(entries);
    let cid_v1 = matches!(opts.cid_version, CidVersion::V1);
    let root = hash_dir_with_sink(&tree, cid_v1, sink)?;
    cid_bytes_to_item_hash(&root.cid_bytes)
}


/// Build the local UnixFS root CID for `entries`, matching what kubo's
/// HTTP `/api/v0/add?wrap-with-directory=true` produces given the same flat
/// list of files via multipart.
///
/// This is a thin wrapper over `build_folder_dag` that discards block bytes.
///
/// # Symlinks
///
/// This function hashes only the byte content of `entries` — there is no
/// notion of a UnixFS `Symlink` node. With `follow_symlinks=true` (the
/// `collect_folder_files` default), symlinks are dereferenced by the walker
/// and their target's bytes are uploaded as regular files. The resulting CID
/// matches `ipfs add -r --no-symlinks` on the same source, NOT plain
/// `ipfs add -r` (which would create UnixFS `Symlink` nodes for each
/// symlink and produce a different CID).
///
/// This is a deliberate tradeoff that mirrors the multipart upload path; if
/// symlink-preservation semantics are needed, both the walker and the upload
/// would have to switch to `application/x-symlink` parts together.
pub fn hash_folder_root(
    entries: &[FolderEntry],
    opts: &UploadFolderOptions,
) -> Result<ItemHash, FolderHashError> {
    build_folder_dag(entries, opts, &mut |_, _| Ok(()))
}

fn hash_dir_with_sink(
    tree: &BTreeMap<String, TreeNode>,
    cid_v1: bool,
    sink: &mut BlockSink<'_>,
) -> Result<DagNode, FolderHashError> {
    let mut children: Vec<ChildLink> = Vec::with_capacity(tree.len());
    for (name, node) in tree {
        let dag = match node {
            TreeNode::File(path) => hash_file_with_sink(path, cid_v1, sink)?,
            TreeNode::Dir(sub) => hash_dir_with_sink(sub, cid_v1, sink)?,
        };
        children.push(ChildLink {
            name: name.clone(),
            cid_bytes: dag.cid_bytes,
            cumulative_size: dag.cumulative_size,
        });
    }

    if should_shard(&children) {
        build_hamt_root_with_sink(&children, cid_v1, sink)
    } else {
        build_plain_directory_with_sink(&children, cid_v1, sink)
    }
}

fn hash_file_with_sink(
    path: &std::path::Path,
    cid_v1: bool,
    sink: &mut BlockSink<'_>,
) -> Result<DagNode, FolderHashError> {
    let mut hasher = if cid_v1 {
        Hasher::for_ipfs_v1_raw_leaves()
    } else {
        Hasher::for_ipfs()
    };

    let mut f = File::open(path).map_err(|e| FolderHashError::Io {
        path: path.to_path_buf(),
        source: e,
    })?;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf).map_err(|e| FolderHashError::Io {
            path: path.to_path_buf(),
            source: e,
        })?;
        if n == 0 {
            break;
        }
        hasher
            .update_with_sink(&buf[..n], sink)
            .map_err(FolderHashError::Sink)?;
    }

    // finalize_with_sink returns the root DagNode with the correct cumulative_size
    // (i.e. the sum of all block sizes in the subtree, not just the raw file bytes).
    // This is what kubo stores in PBLink.Tsize for the parent directory's link.
    hasher
        .finalize_with_sink(sink)
        .map_err(FolderHashError::Sink)
}

fn cid_bytes_to_item_hash(bytes: &[u8]) -> Result<ItemHash, FolderHashError> {
    // CIDv1 binary starts with 0x01 (version byte); CIDv0 binary is a bare
    // sha2-256 multihash starting with 0x12 (multihash code). Mirrors the
    // byte-sniff in `verify::Hasher::finalize`.
    let cid_str = if bytes.first() == Some(&0x01) {
        let parsed = ::cid::Cid::try_from(bytes).expect("hash_folder_root produces valid CIDs");
        parsed.to_string()
    } else {
        let mh = multihash::Multihash::<64>::from_bytes(bytes)
            .expect("CIDv0 multihash bytes must parse");
        let parsed = ::cid::Cid::new_v0(mh).expect("valid sha2-256 multihash");
        parsed.to_string()
    };
    let aleph_cid =
        aleph_types::cid::Cid::try_from(cid_str.as_str()).expect("round-trip CID parse");
    Ok(ItemHash::Ipfs(aleph_cid))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(rel: &str) -> FolderEntry {
        FolderEntry {
            relative_path: rel.to_string(),
            absolute_path: PathBuf::from(format!("/abs/{rel}")),
        }
    }

    #[test]
    fn build_tree_flat() {
        let entries = vec![entry("a.txt"), entry("b.txt")];
        let tree = build_tree(&entries);
        assert_eq!(tree.len(), 2);
        assert!(matches!(tree.get("a.txt"), Some(TreeNode::File(_))));
        assert!(matches!(tree.get("b.txt"), Some(TreeNode::File(_))));
    }

    #[test]
    fn build_tree_nested() {
        let entries = vec![
            entry("top.txt"),
            entry("sub/inner.txt"),
            entry("sub/deeper/leaf.txt"),
        ];
        let tree = build_tree(&entries);
        assert!(matches!(tree.get("top.txt"), Some(TreeNode::File(_))));
        let sub = match tree.get("sub") {
            Some(TreeNode::Dir(m)) => m,
            _ => panic!("sub must be a Dir"),
        };
        assert!(matches!(sub.get("inner.txt"), Some(TreeNode::File(_))));
        let deeper = match sub.get("deeper") {
            Some(TreeNode::Dir(m)) => m,
            _ => panic!("deeper must be a Dir"),
        };
        assert!(matches!(deeper.get("leaf.txt"), Some(TreeNode::File(_))));
    }

    #[test]
    fn build_tree_alphabetic_via_btreemap() {
        let entries = vec![entry("zebra.txt"), entry("apple.txt"), entry("mango.txt")];
        let tree = build_tree(&entries);
        let names: Vec<&String> = tree.keys().collect();
        assert_eq!(names, vec!["apple.txt", "mango.txt", "zebra.txt"]);
    }

    #[test]
    fn plain_dir_two_entries_canonical_encoding() {
        // Two children with stub CIDv1 raw CIDs (32 bytes of repeated content
        // for the multihash). Verifies: structure builds, CIDv1 dag-pb prefix,
        // cumulative_size accounts for child contributions.
        let child_a = ChildLink {
            name: "a.txt".to_string(),
            cid_bytes: stub_cidv1_raw(0x00),
            cumulative_size: 1,
        };
        let child_b = ChildLink {
            name: "b.txt".to_string(),
            cid_bytes: stub_cidv1_raw(0x01),
            cumulative_size: 1,
        };

        let dag = build_plain_directory(&[child_a.clone(), child_b.clone()], true);

        assert_eq!(dag.cid_bytes[0], 0x01); // CIDv1
        assert_eq!(dag.cid_bytes[1], 0x70); // dag-pb codec

        // Cumulative size = node serialized bytes + sum of child cumulative sizes.
        assert!(dag.cumulative_size > child_a.cumulative_size + child_b.cumulative_size);
    }

    fn stub_cidv1_raw(b: u8) -> Vec<u8> {
        // CIDv1 raw codec wrapping a sha2-256 multihash filled with byte `b`.
        let mut v = vec![0x01u8, 0x55, 0x12, 0x20];
        v.extend(std::iter::repeat_n(b, 32));
        v
    }

    #[test]
    fn estimated_dir_size_short_names() {
        // Per boxo productionLinkSize: len(name) + cid.ByteLen().
        // Two entries with 10-char names and 36-byte CIDv1 each:
        // 2 * (10 + 36) = 92.
        let children = vec![
            ChildLink {
                name: "a".repeat(10),
                cid_bytes: stub_cidv1_raw(0x00),
                cumulative_size: 0,
            },
            ChildLink {
                name: "b".repeat(10),
                cid_bytes: stub_cidv1_raw(0x01),
                cumulative_size: 0,
            },
        ];
        assert_eq!(estimated_dir_size(&children), 92);
    }

    #[test]
    fn shard_threshold_below_and_above() {
        // Threshold is 256 * 1024 = 262144.
        // boxo productionLinkSize = len(name) + cid.ByteLen()
        // For 8-char names with 36-byte CIDv1: 8 + 36 = 44 bytes per entry.
        // 5957 entries * 44 = 262108 -> below
        // 5958 entries * 44 = 262152 -> above (matches kubo v0.30.0)
        //
        // Stub CIDv1 raw = 36 bytes (version=1, codec=0x55, sha256 multihash).
        let stub_cid = stub_cidv1_raw(0x00);
        let small: Vec<ChildLink> = (0..5957)
            .map(|i| ChildLink {
                name: format!("{i:08}"),
                cid_bytes: stub_cid.clone(),
                cumulative_size: 0,
            })
            .collect();
        let big: Vec<ChildLink> = (0..5958)
            .map(|i| ChildLink {
                name: format!("{i:08}"),
                cid_bytes: stub_cid.clone(),
                cumulative_size: 0,
            })
            .collect();
        assert!(!should_shard(&small));
        assert!(should_shard(&big));
    }

    #[test]
    fn bitfield_set_and_test() {
        let mut bf = [0u8; 32];
        set_bit(&mut bf, 0);
        set_bit(&mut bf, 7);
        set_bit(&mut bf, 8);
        set_bit(&mut bf, 255);
        assert!(test_bit(&bf, 0));
        assert!(test_bit(&bf, 7));
        assert!(test_bit(&bf, 8));
        assert!(test_bit(&bf, 255));
        assert!(!test_bit(&bf, 1));
        assert!(!test_bit(&bf, 254));
    }

    #[test]
    fn hash_folder_root_single_file_v1_raw() {
        use crate::ipfs::{CidVersion, UploadFolderOptions};
        use std::io::Write;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("hello.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(b"hello\n").unwrap();
        drop(f);

        let entries = vec![FolderEntry {
            relative_path: "hello.txt".to_string(),
            absolute_path: path,
        }];
        let opts = UploadFolderOptions {
            cid_version: CidVersion::V1,
            ..Default::default()
        };

        let item = hash_folder_root(&entries, &opts).expect("hash should succeed");

        let s = match item {
            aleph_types::item_hash::ItemHash::Ipfs(c) => c.to_string(),
            _ => panic!("expected IPFS"),
        };
        assert!(s.starts_with("bafy"), "expected CIDv1 dag-pb root, got {s}");
    }

    #[test]
    fn hamt_single_level_two_entries() {
        // Two entries with names that go into different root-level slots.
        // The HAMT root should:
        //   - Be a CIDv1 dag-pb (codec 0x70)
        //   - Have non-zero cumulative_size
        //   - Build successfully without errors
        let kids = vec![
            ChildLink {
                name: "alpha".to_string(),
                cid_bytes: stub_cidv1_raw(0xaa),
                cumulative_size: 1,
            },
            ChildLink {
                name: "bravo".to_string(),
                cid_bytes: stub_cidv1_raw(0xbb),
                cumulative_size: 1,
            },
        ];
        let dag = build_hamt_root(&kids, true).expect("HAMT must build");

        assert_eq!(dag.cid_bytes[0], 0x01);
        assert_eq!(dag.cid_bytes[1], 0x70);
        assert!(dag.cumulative_size > 0);
    }
}
