use aleph_types::cid::Cid;
use aleph_types::item_hash::{AlephItemHash, ItemHash};
use cid::Cid as LibCid;
use prost::Message;
use sha2::{Digest, Sha256};

/// Encode a protobuf varint (LEB128).
fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    while value >= 0x80 {
        buf.push((value as u8) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Encode a protobuf field tag (field number + wire type).
fn encode_tag(field_number: u32, wire_type: u8, buf: &mut Vec<u8>) {
    encode_varint(((field_number as u64) << 3) | wire_type as u64, buf);
}

/// Wire type 2: length-delimited.
const WIRE_TYPE_LEN: u8 = 2;

/// Encode a PBNode in canonical dag-pb order (Links before Data).
///
/// Standard prost encoding emits fields in field-number order (Data=1 before
/// Links=2), but the IPFS dag-pb spec mandates Links before Data. Without this
/// ordering the SHA-256 digest — and therefore the CID — will differ from what
/// IPFS computes.
pub(crate) fn encode_pbnode_canonical(node: &merkledag::PbNode) -> Vec<u8> {
    let mut buf = Vec::new();

    // Links (field 2) first
    for link in &node.links {
        let mut link_buf = Vec::new();
        prost::Message::encode(link, &mut link_buf).expect("encoding PBLink");
        encode_tag(2, WIRE_TYPE_LEN, &mut buf);
        encode_varint(link_buf.len() as u64, &mut buf);
        buf.extend_from_slice(&link_buf);
    }

    // Data (field 1) second
    if let Some(data) = &node.data {
        encode_tag(1, WIRE_TYPE_LEN, &mut buf);
        encode_varint(data.len() as u64, &mut buf);
        buf.extend_from_slice(data);
    }

    buf
}

use crate::proto::merkledag;
use crate::proto::unixfs;

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("integrity check failed: expected {expected}, computed {actual}")]
    IntegrityMismatch {
        expected: ItemHash,
        actual: ItemHash,
    },
    #[error("unsupported CID format for verification: {0}")]
    UnsupportedCid(String),
}

/// Raw codec for CIDv1 (identity mapping of bytes to CID).
pub(crate) const RAW_CODEC: u64 = 0x55;

/// dag-pb codec for CIDv1.
pub(crate) const DAG_PB_CODEC: u64 = 0x70;

/// IPFS default chunk size: 256 KiB.
pub(crate) const CHUNK_SIZE: usize = 262144;

/// IPFS default maximum links per node (go-ipfs `helpers.DefaultLinksPerBlock`).
const MAX_LINKS: usize = 174;

/// Encode a SHA-256 digest as a multihash: [0x12, 0x20, ...32 bytes...]
pub(crate) fn encode_multihash(digest: &[u8]) -> Vec<u8> {
    let mut mh = Vec::with_capacity(2 + digest.len());
    mh.push(0x12); // SHA-256 code
    mh.push(0x20); // 32 bytes
    mh.extend_from_slice(digest);
    mh
}

/// A dag-pb node (leaf or internal) used during tree construction.
pub struct DagNode {
    /// The CID bytes stored in PBLink.Hash.
    /// For CIDv0: bare multihash [0x12, 0x20, ...32 bytes SHA-256 digest...]
    /// For CIDv1: full CID binary (varint version + varint codec + multihash)
    pub(crate) cid_bytes: Vec<u8>,
    /// Cumulative size: serialized node bytes + sum of children's cumulative sizes.
    /// For raw leaves this equals the raw chunk size. Used for PBLink.Tsize.
    pub(crate) cumulative_size: u64,
    /// Total file data bytes covered by this subtree.
    pub(crate) data_size: u64,
}

/// A streaming hasher that accumulates data and produces an `ItemHash` on finalization.
pub enum Hasher {
    Native {
        hasher: Sha256,
    },
    CidRaw {
        hasher: Sha256,
    },
    DagPb {
        buffer: Vec<u8>,
        leaves: Vec<DagNode>,
        raw_leaves: bool,
    },
}

impl Hasher {
    /// Creates a hasher for Aleph native storage (SHA-256).
    pub fn for_storage() -> Self {
        Self::Native {
            hasher: Sha256::new(),
        }
    }

    /// Creates a hasher for IPFS CIDv0 dag-pb (wrapped leaves, balanced DAG).
    pub fn for_ipfs() -> Self {
        Self::DagPb {
            buffer: Vec::with_capacity(CHUNK_SIZE),
            leaves: Vec::new(),
            raw_leaves: false,
        }
    }

    /// Creates a hasher for IPFS CIDv1 dag-pb with raw leaves.
    ///
    /// Matches kubo's `ipfs add --cid-version=1 --raw-leaves` defaults: chunks
    /// of 256 KiB are stored as raw blocks (codec 0x55) and the file root (if
    /// the file spans multiple chunks) is a dag-pb node linking those raw leaves.
    /// A single-chunk file collapses to a bare raw block whose CID is `bafkrei…`.
    pub(crate) fn for_ipfs_v1_raw_leaves() -> Self {
        Self::DagPb {
            buffer: Vec::with_capacity(CHUNK_SIZE),
            leaves: Vec::new(),
            raw_leaves: true,
        }
    }

    /// Creates a hasher appropriate for the given expected hash.
    pub(crate) fn from_expected(expected: &ItemHash) -> Result<Self, VerifyError> {
        match expected {
            ItemHash::Native(_) => Ok(Self::Native {
                hasher: Sha256::new(),
            }),
            ItemHash::Ipfs(cid) => {
                if cid.is_v0() {
                    // CIDv0 is always dag-pb codec with wrapped leaves
                    return Ok(Self::DagPb {
                        buffer: Vec::with_capacity(CHUNK_SIZE),
                        leaves: Vec::new(),
                        raw_leaves: false,
                    });
                }

                let parsed = LibCid::try_from(cid.as_str())
                    .map_err(|e| VerifyError::UnsupportedCid(format!("{cid}: {e}")))?;

                match parsed.codec() {
                    RAW_CODEC => Ok(Self::CidRaw {
                        hasher: Sha256::new(),
                    }),
                    DAG_PB_CODEC => Ok(Self::DagPb {
                        buffer: Vec::with_capacity(CHUNK_SIZE),
                        leaves: Vec::new(),
                        raw_leaves: true,
                    }),
                    other => Err(VerifyError::UnsupportedCid(format!(
                        "{cid}: unsupported codec 0x{other:x}"
                    ))),
                }
            }
        }
    }

    /// Feed data into the hasher.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Native { hasher, .. } | Self::CidRaw { hasher, .. } => hasher.update(data),
            Self::DagPb {
                buffer,
                leaves,
                raw_leaves,
            } => {
                let raw = *raw_leaves;
                let mut remaining = data;
                while !remaining.is_empty() {
                    let space = CHUNK_SIZE - buffer.len();
                    let take = remaining.len().min(space);
                    buffer.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];
                    if buffer.len() == CHUNK_SIZE {
                        let leaf = if raw {
                            Self::build_raw_leaf(buffer)
                        } else {
                            Self::build_leaf(buffer)
                        };
                        leaves.push(leaf);
                        buffer.clear();
                    }
                }
            }
        }
    }

    /// Build a dag-pb leaf node from a chunk of data.
    pub(crate) fn build_leaf(chunk: &[u8]) -> DagNode {
        let (node, _bytes) = Self::build_leaf_with_bytes(chunk);
        node
    }

    /// Build a dag-pb leaf node from a chunk of data, also returning the
    /// canonical pbnode bytes used to compute the CID.
    pub(crate) fn build_leaf_with_bytes(chunk: &[u8]) -> (DagNode, Vec<u8>) {
        let unixfs_data = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: if chunk.is_empty() {
                None
            } else {
                Some(chunk.to_vec())
            },
            filesize: Some(chunk.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut unixfs_bytes = Vec::new();
        unixfs_data
            .encode(&mut unixfs_bytes)
            .expect("protobuf encoding cannot fail for in-memory buffers");

        let node = merkledag::PbNode {
            links: vec![],
            data: Some(unixfs_bytes),
        };
        let node_bytes = encode_pbnode_canonical(&node);

        let digest = Sha256::digest(&node_bytes);
        let cid_bytes = encode_multihash(&digest);

        let dag_node = DagNode {
            cid_bytes,
            cumulative_size: node_bytes.len() as u64,
            data_size: chunk.len() as u64,
        };
        (dag_node, node_bytes)
    }

    /// Build a raw leaf node: hash the chunk directly without dag-pb/UnixFS wrapping.
    /// Used for CIDv1 dag-pb which defaults to raw leaves.
    pub(crate) fn build_raw_leaf(chunk: &[u8]) -> DagNode {
        let digest = Sha256::digest(chunk);
        let mh = encode_multihash(&digest);
        // CIDv1 raw binary: varint(1) + varint(RAW_CODEC) + multihash
        let mut cid_bytes = Vec::with_capacity(2 + mh.len());
        cid_bytes.push(0x01); // CID version 1
        cid_bytes.push(RAW_CODEC as u8); // 0x55
        cid_bytes.extend_from_slice(&mh);

        DagNode {
            cid_bytes,
            cumulative_size: chunk.len() as u64,
            data_size: chunk.len() as u64,
        }
    }

    /// Build an internal dag-pb node from a list of children.
    /// When `v1` is true, produces CIDv1 dag-pb binary; otherwise bare multihash (CIDv0).
    pub(crate) fn build_internal_node(children: &[DagNode], v1: bool) -> DagNode {
        let (node, _bytes) = Self::build_internal_node_with_bytes(children, v1);
        node
    }

    /// Build an internal dag-pb node from a list of children, also returning the
    /// canonical pbnode bytes used to compute the CID.
    /// When `v1` is true, produces CIDv1 dag-pb binary; otherwise bare multihash (CIDv0).
    pub(crate) fn build_internal_node_with_bytes(
        children: &[DagNode],
        v1: bool,
    ) -> (DagNode, Vec<u8>) {
        let total_data_size: u64 = children.iter().map(|c| c.data_size).sum();
        let blocksizes: Vec<u64> = children.iter().map(|c| c.data_size).collect();

        let links: Vec<merkledag::PbLink> = children
            .iter()
            .map(|c| merkledag::PbLink {
                hash: Some(c.cid_bytes.clone()),
                name: Some(String::new()),
                tsize: Some(c.cumulative_size),
            })
            .collect();

        let root_unixfs = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: None,
            filesize: Some(total_data_size),
            blocksizes,
            hash_type: None,
            fanout: None,
        };
        let mut root_unixfs_bytes = Vec::new();
        root_unixfs
            .encode(&mut root_unixfs_bytes)
            .expect("protobuf encoding cannot fail");

        let node = merkledag::PbNode {
            links,
            data: Some(root_unixfs_bytes),
        };
        let node_bytes = encode_pbnode_canonical(&node);

        let digest = Sha256::digest(&node_bytes);
        let mh = encode_multihash(&digest);

        let cid_bytes = if v1 {
            let mut cid = Vec::with_capacity(2 + mh.len());
            cid.push(0x01); // CID version 1
            cid.push(DAG_PB_CODEC as u8); // 0x70
            cid.extend_from_slice(&mh);
            cid
        } else {
            mh
        };

        let node_size = node_bytes.len() as u64;
        let children_cumulative: u64 = children.iter().map(|c| c.cumulative_size).sum();

        let dag_node = DagNode {
            cid_bytes,
            cumulative_size: node_size + children_cumulative,
            data_size: total_data_size,
        };
        (dag_node, node_bytes)
    }

    /// Like `update`, but invokes `sink` for each complete leaf (raw or
    /// pbnode-wrapped) as it is finalized.
    #[allow(clippy::type_complexity)]
    pub(crate) fn update_with_sink(
        &mut self,
        data: &[u8],
        sink: &mut dyn FnMut(&[u8], &[u8]) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        match self {
            Self::Native { hasher, .. } | Self::CidRaw { hasher, .. } => {
                hasher.update(data);
                Ok(())
            }
            Self::DagPb {
                buffer,
                leaves,
                raw_leaves,
            } => {
                let raw = *raw_leaves;
                let mut remaining = data;
                while !remaining.is_empty() {
                    let space = CHUNK_SIZE - buffer.len();
                    let take = remaining.len().min(space);
                    buffer.extend_from_slice(&remaining[..take]);
                    remaining = &remaining[take..];
                    if buffer.len() == CHUNK_SIZE {
                        let (leaf, block_bytes) = if raw {
                            let bytes = buffer.clone();
                            (Self::build_raw_leaf(buffer), bytes)
                        } else {
                            Self::build_leaf_with_bytes(buffer)
                        };
                        sink(&leaf.cid_bytes, &block_bytes)?;
                        leaves.push(leaf);
                        buffer.clear();
                    }
                }
                Ok(())
            }
        }
    }

    /// Drain the trailing partial buffer (if any), emit final leaf and all
    /// internal nodes up to the root, invoking `sink` for each block.
    /// Returns the root `DagNode`.
    ///
    /// Panics if called on a `Native` or `CidRaw` hasher.
    #[allow(clippy::type_complexity)]
    pub(crate) fn finalize_with_sink(
        self,
        sink: &mut dyn FnMut(&[u8], &[u8]) -> std::io::Result<()>,
    ) -> std::io::Result<DagNode> {
        match self {
            Self::DagPb {
                buffer,
                mut leaves,
                raw_leaves,
            } => {
                let v1 = raw_leaves;

                // Single-leaf (or empty-file) fast path: file fits in one chunk.
                if leaves.is_empty() {
                    let (leaf, block_bytes) = if raw_leaves {
                        let bytes = buffer.clone();
                        (Self::build_raw_leaf(&buffer), bytes)
                    } else {
                        Self::build_leaf_with_bytes(&buffer)
                    };
                    sink(&leaf.cid_bytes, &block_bytes)?;
                    return Ok(leaf);
                }

                // Flush remaining partial chunk.
                if !buffer.is_empty() {
                    let (leaf, block_bytes) = if raw_leaves {
                        let bytes = buffer.clone();
                        (Self::build_raw_leaf(&buffer), bytes)
                    } else {
                        Self::build_leaf_with_bytes(&buffer)
                    };
                    sink(&leaf.cid_bytes, &block_bytes)?;
                    leaves.push(leaf);
                }

                // Build internal node tree, emitting each level to sink.
                let mut nodes = leaves;
                while nodes.len() > 1 {
                    let mut next_level = Vec::with_capacity(nodes.len().div_ceil(MAX_LINKS));
                    for chunk in nodes.chunks(MAX_LINKS) {
                        let (internal, node_bytes) =
                            Self::build_internal_node_with_bytes(chunk, v1);
                        sink(&internal.cid_bytes, &node_bytes)?;
                        next_level.push(internal);
                    }
                    nodes = next_level;
                }
                Ok(nodes.into_iter().next().unwrap())
            }
            _ => panic!("finalize_with_sink called on non-DagPb hasher"),
        }
    }

    /// Build the root `DagNode` for a `DagPb` hasher, including the correct
    /// cumulative_size for use as `PBLink.Tsize` in a parent directory node.
    ///
    /// Panics if called on a `Native` or `CidRaw` hasher (only `DagPb` produces
    /// a DAG node with meaningful cumulative_size).
    pub(crate) fn finalize_dag_node(self) -> DagNode {
        match self {
            Self::DagPb {
                buffer,
                mut leaves,
                raw_leaves,
            } => {
                let make_leaf = |chunk: &[u8]| {
                    if raw_leaves {
                        Self::build_raw_leaf(chunk)
                    } else {
                        Self::build_leaf(chunk)
                    }
                };

                // Flush any remaining bytes in buffer as the last chunk.
                if !buffer.is_empty() {
                    leaves.push(make_leaf(&buffer));
                }

                let v1 = raw_leaves;

                if leaves.is_empty() {
                    make_leaf(&[])
                } else if leaves.len() == 1 {
                    leaves.into_iter().next().unwrap()
                } else {
                    let mut nodes = leaves;
                    while nodes.len() > 1 {
                        nodes = nodes
                            .chunks(MAX_LINKS)
                            .map(|c| Self::build_internal_node(c, v1))
                            .collect();
                    }
                    nodes.into_iter().next().unwrap()
                }
            }
            _ => panic!("finalize_dag_node called on non-DagPb hasher"),
        }
    }

    /// Finalize the hasher and return the computed `ItemHash`.
    pub fn finalize(self) -> ItemHash {
        match self {
            Self::Native { hasher } => {
                let computed = AlephItemHash::new(hasher.finalize().into());
                ItemHash::Native(computed)
            }
            Self::CidRaw { hasher } => {
                let digest = hasher.finalize();
                // SHA-256 multihash code is 0x12
                let mh = multihash::Multihash::<64>::wrap(0x12, &digest)
                    .expect("SHA-256 digest fits in 64-byte multihash");
                let computed_lib_cid = LibCid::new_v1(RAW_CODEC, mh);
                let computed_cid_str = computed_lib_cid.to_string();
                let cid = Cid::try_from(computed_cid_str.as_str()).expect("valid computed CID");
                ItemHash::Ipfs(cid)
            }
            Self::DagPb { .. } => {
                let root_node = self.finalize_dag_node();
                let root_cid_bytes = root_node.cid_bytes;

                // CIDv1 binary starts with 0x01 (version byte); CIDv0 bare
                // multihash starts with 0x12 (sha2-256 code).
                let computed_cid_str = if root_cid_bytes.first() == Some(&0x01) {
                    let lib_cid =
                        LibCid::try_from(&root_cid_bytes[..]).expect("valid CIDv1 from build");
                    lib_cid.to_string()
                } else {
                    // CIDv0: bare multihash (raw_leaves=false)
                    bs58::encode(&root_cid_bytes).into_string()
                };

                let cid =
                    Cid::try_from(computed_cid_str.as_str()).expect("computed CID is always valid");
                ItemHash::Ipfs(cid)
            }
        }
    }
}

pub struct HashVerifier {
    hasher: Hasher,
    expected: ItemHash,
}

impl HashVerifier {
    pub fn new(expected: &ItemHash) -> Result<Self, VerifyError> {
        Ok(Self {
            hasher: Hasher::from_expected(expected)?,
            expected: expected.clone(),
        })
    }

    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    pub fn finalize(self) -> Result<(), VerifyError> {
        let computed = self.hasher.finalize();
        if computed == self.expected {
            Ok(())
        } else {
            Err(VerifyError::IntegrityMismatch {
                expected: self.expected,
                actual: computed,
            })
        }
    }
}

/// Computes an IPFS CIDv0 (dag-pb) for the given data.
///
/// Uses the same chunking and tree construction as IPFS's default settings
/// (256 KiB chunks, balanced DAG, wrapped leaves).
pub fn compute_cid(data: &[u8]) -> Cid {
    let mut hasher = Hasher::for_ipfs();
    hasher.update(data);
    match hasher.finalize() {
        ItemHash::Ipfs(cid) => cid,
        _ => unreachable!("for_ipfs() always produces ItemHash::Ipfs"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aleph_types::item_hash::{AlephItemHash, ItemHash};

    #[test]
    fn test_verify_native_hash_success() {
        let data = b"hello world";
        let expected = ItemHash::Native(AlephItemHash::from_bytes(data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier.finalize().expect("should verify successfully");
    }

    #[test]
    fn test_verify_native_hash_failure() {
        let expected = ItemHash::Native(AlephItemHash::from_bytes(b"hello world"));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"wrong data");
        let err = verifier.finalize().unwrap_err();
        assert!(matches!(err, VerifyError::IntegrityMismatch { .. }));
    }

    #[test]
    fn test_verify_native_hash_chunked() {
        let data = b"hello world";
        let expected = ItemHash::Native(AlephItemHash::from_bytes(data));
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"hello");
        verifier.update(b" ");
        verifier.update(b"world");
        verifier.finalize().expect("chunked update should verify");
    }

    #[test]
    fn test_verify_cidv1_raw_success() {
        use cid::Cid as LibCid;
        use multihash_codetable::{Code, MultihashDigest};

        let data = b"hello ipfs world";
        // Compute the expected CIDv1 raw
        let mh = Code::Sha2_256.digest(data);
        let expected_cid = LibCid::new_v1(0x55, mh); // 0x55 = raw codec
        // Convert to our Cid type (base32 encoded string)
        let cid_string = expected_cid.to_string();
        let our_cid = aleph_types::cid::Cid::try_from(cid_string.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier.finalize().expect("CIDv1 raw should verify");
    }

    #[test]
    fn test_verify_cidv1_raw_failure() {
        use cid::Cid as LibCid;
        use multihash_codetable::{Code, MultihashDigest};

        let data = b"hello ipfs world";
        let mh = Code::Sha2_256.digest(data);
        let expected_cid = LibCid::new_v1(0x55, mh);
        let cid_string = expected_cid.to_string();
        let our_cid = aleph_types::cid::Cid::try_from(cid_string.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"wrong data");
        let err = verifier.finalize().unwrap_err();
        assert!(matches!(err, VerifyError::IntegrityMismatch { .. }));
    }

    #[test]
    fn test_verify_cidv0_single_chunk() {
        // Small file (< 256KB) -> single leaf node, no intermediate DAG
        let data = b"hello dag-pb world";

        // Build the expected CIDv0 manually using our protobuf types
        let unixfs_data = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: Some(data.to_vec()),
            filesize: Some(data.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut unixfs_bytes = Vec::new();
        unixfs_data.encode(&mut unixfs_bytes).unwrap();

        let node = merkledag::PbNode {
            links: vec![],
            data: Some(unixfs_bytes),
        };
        let node_bytes = encode_pbnode_canonical(&node);

        let digest = sha2::Sha256::digest(&node_bytes);
        let mut multihash_bytes = vec![0x12, 0x20];
        multihash_bytes.extend_from_slice(&digest);
        let expected_cidv0 = bs58::encode(&multihash_bytes).into_string();

        let our_cid = aleph_types::cid::Cid::try_from(expected_cidv0.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier
            .finalize()
            .expect("CIDv0 single chunk should verify");
    }

    #[test]
    fn test_verify_cidv0_single_chunk_failure() {
        let data = b"hello dag-pb world";

        let unixfs_data = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: Some(data.to_vec()),
            filesize: Some(data.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut unixfs_bytes = Vec::new();
        unixfs_data.encode(&mut unixfs_bytes).unwrap();

        let node = merkledag::PbNode {
            links: vec![],
            data: Some(unixfs_bytes),
        };
        let node_bytes = encode_pbnode_canonical(&node);

        let digest = sha2::Sha256::digest(&node_bytes);
        let mut multihash_bytes = vec![0x12, 0x20];
        multihash_bytes.extend_from_slice(&digest);
        let expected_cidv0 = bs58::encode(&multihash_bytes).into_string();

        let our_cid = aleph_types::cid::Cid::try_from(expected_cidv0.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(b"wrong data");
        let err = verifier.finalize().unwrap_err();
        assert!(matches!(err, VerifyError::IntegrityMismatch { .. }));
    }

    #[test]
    fn test_verify_cidv0_multi_chunk() {
        // File larger than 256KB -> multiple leaves + root node
        let chunk_size = 262144;
        let data = vec![0xABu8; chunk_size + 100]; // slightly over one chunk

        // Build leaf 1
        let chunk1 = &data[..chunk_size];
        let leaf1_unixfs = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: Some(chunk1.to_vec()),
            filesize: Some(chunk1.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut leaf1_unixfs_bytes = Vec::new();
        leaf1_unixfs.encode(&mut leaf1_unixfs_bytes).unwrap();
        let leaf1_node = merkledag::PbNode {
            links: vec![],
            data: Some(leaf1_unixfs_bytes),
        };
        let leaf1_bytes = encode_pbnode_canonical(&leaf1_node);
        let leaf1_digest = sha2::Sha256::digest(&leaf1_bytes);
        let mut leaf1_mh = vec![0x12, 0x20];
        leaf1_mh.extend_from_slice(&leaf1_digest);

        // Build leaf 2
        let chunk2 = &data[chunk_size..];
        let leaf2_unixfs = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: Some(chunk2.to_vec()),
            filesize: Some(chunk2.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut leaf2_unixfs_bytes = Vec::new();
        leaf2_unixfs.encode(&mut leaf2_unixfs_bytes).unwrap();
        let leaf2_node = merkledag::PbNode {
            links: vec![],
            data: Some(leaf2_unixfs_bytes),
        };
        let leaf2_bytes = encode_pbnode_canonical(&leaf2_node);
        let leaf2_digest = sha2::Sha256::digest(&leaf2_bytes);
        let mut leaf2_mh = vec![0x12, 0x20];
        leaf2_mh.extend_from_slice(&leaf2_digest);

        // Build root node
        let root_unixfs = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: None,
            filesize: Some(data.len() as u64),
            blocksizes: vec![chunk1.len() as u64, chunk2.len() as u64],
            hash_type: None,
            fanout: None,
        };
        let mut root_unixfs_bytes = Vec::new();
        root_unixfs.encode(&mut root_unixfs_bytes).unwrap();
        let root_node = merkledag::PbNode {
            links: vec![
                merkledag::PbLink {
                    hash: Some(leaf1_mh),
                    name: Some(String::new()),
                    tsize: Some(leaf1_bytes.len() as u64),
                },
                merkledag::PbLink {
                    hash: Some(leaf2_mh),
                    name: Some(String::new()),
                    tsize: Some(leaf2_bytes.len() as u64),
                },
            ],
            data: Some(root_unixfs_bytes),
        };
        let root_bytes = encode_pbnode_canonical(&root_node);
        let root_digest = sha2::Sha256::digest(&root_bytes);
        let mut root_multihash = vec![0x12, 0x20];
        root_multihash.extend_from_slice(&root_digest);
        let expected_cidv0 = bs58::encode(&root_multihash).into_string();

        let our_cid = aleph_types::cid::Cid::try_from(expected_cidv0.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(&data);
        verifier
            .finalize()
            .expect("CIDv0 multi-chunk should verify");
    }

    #[test]
    fn test_verify_cidv0_multi_chunk_streamed() {
        // Same multi-chunk test but data is fed in small increments
        let chunk_size = 262144;
        let data = vec![0xCDu8; chunk_size * 2 + 500]; // 2 full chunks + partial

        // Build leaves
        let chunks: Vec<&[u8]> = data.chunks(chunk_size).collect();
        let mut leaves: Vec<(Vec<u8>, u64, u64)> = Vec::new(); // (mh, node_size, data_size)
        for chunk in &chunks {
            let uf = unixfs::Data {
                r#type: unixfs::DataType::File as i32,
                data: Some(chunk.to_vec()),
                filesize: Some(chunk.len() as u64),
                blocksizes: vec![],
                hash_type: None,
                fanout: None,
            };
            let mut uf_bytes = Vec::new();
            uf.encode(&mut uf_bytes).unwrap();
            let node = merkledag::PbNode {
                links: vec![],
                data: Some(uf_bytes),
            };
            let node_bytes = encode_pbnode_canonical(&node);
            let digest = sha2::Sha256::digest(&node_bytes);
            let mut mh = vec![0x12, 0x20];
            mh.extend_from_slice(&digest);
            leaves.push((mh, node_bytes.len() as u64, chunk.len() as u64));
        }

        // Build root
        let links: Vec<merkledag::PbLink> = leaves
            .iter()
            .map(|(mh, ns, _)| merkledag::PbLink {
                hash: Some(mh.clone()),
                name: Some(String::new()),
                tsize: Some(*ns),
            })
            .collect();
        let blocksizes: Vec<u64> = leaves.iter().map(|(_, _, ds)| *ds).collect();
        let root_uf = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: None,
            filesize: Some(data.len() as u64),
            blocksizes,
            hash_type: None,
            fanout: None,
        };
        let mut root_uf_bytes = Vec::new();
        root_uf.encode(&mut root_uf_bytes).unwrap();
        let root_node = merkledag::PbNode {
            links,
            data: Some(root_uf_bytes),
        };
        let root_bytes = encode_pbnode_canonical(&root_node);
        let digest = sha2::Sha256::digest(&root_bytes);
        let mut root_mh = vec![0x12, 0x20];
        root_mh.extend_from_slice(&digest);
        let expected_cidv0 = bs58::encode(&root_mh).into_string();

        let our_cid = aleph_types::cid::Cid::try_from(expected_cidv0.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        // Feed data in small arbitrary increments (simulating streaming)
        let mut verifier = HashVerifier::new(&expected).unwrap();
        let mut offset = 0;
        let step = 1000;
        while offset < data.len() {
            let end = (offset + step).min(data.len());
            verifier.update(&data[offset..end]);
            offset = end;
        }
        verifier
            .finalize()
            .expect("CIDv0 multi-chunk streamed should verify");
    }

    #[test]
    fn test_verify_cidv0_exact_chunk_boundary() {
        // File is exactly one chunk (262144 bytes)
        let data = vec![0x42u8; CHUNK_SIZE];

        let unixfs_data = unixfs::Data {
            r#type: unixfs::DataType::File as i32,
            data: Some(data.clone()),
            filesize: Some(data.len() as u64),
            blocksizes: vec![],
            hash_type: None,
            fanout: None,
        };
        let mut unixfs_bytes = Vec::new();
        unixfs_data.encode(&mut unixfs_bytes).unwrap();

        let node = merkledag::PbNode {
            links: vec![],
            data: Some(unixfs_bytes),
        };
        let node_bytes = encode_pbnode_canonical(&node);

        let digest = sha2::Sha256::digest(&node_bytes);
        let mut multihash_bytes = vec![0x12, 0x20];
        multihash_bytes.extend_from_slice(&digest);
        let expected_cidv0 = bs58::encode(&multihash_bytes).into_string();

        let our_cid = aleph_types::cid::Cid::try_from(expected_cidv0.as_str()).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(&data);
        verifier
            .finalize()
            .expect("CIDv0 exact chunk boundary should verify");
    }

    #[test]
    fn test_verify_cidv0_empty_file() {
        // Known IPFS empty file CID (produced by `echo -n '' | ipfs add`)
        let expected_cid = "QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH";
        let our_cid = aleph_types::cid::Cid::try_from(expected_cid).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let verifier = HashVerifier::new(&expected).unwrap();
        // No update() calls — empty file
        verifier.finalize().expect("empty file should verify");
    }
    #[test]
    fn test_verify_cidv0_multi_level_dag() {
        // "deadbeef" repeated 16Mi times (128 MiB total) — CID obtained from IPFS directly
        let expected_cid = "QmcYKke22MG2rnu4nPVj8Z3hMPi2wtVMKzqLcJwYRThYif";
        let data = "deadbeef".repeat(16 * 1024 * 1024);

        let our_cid = aleph_types::cid::Cid::try_from(expected_cid).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data.as_bytes());
        verifier
            .finalize()
            .expect("multi-level DAG should verify against known IPFS CID");
    }

    #[test]
    fn test_verify_cidv1_dagpb_multi_level_dag() {
        // Same data as the CIDv0 test, CIDv1 dag-pb CID obtained from IPFS directly
        let expected_cid = "bafybeiawhayvhrtunmsazigmne75kqyyb2z7oqlvky3abpk4tbkqyzv6iu";
        let data = "deadbeef".repeat(16 * 1024 * 1024);

        let our_cid = aleph_types::cid::Cid::try_from(expected_cid).unwrap();
        let expected = ItemHash::Ipfs(our_cid);

        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data.as_bytes());
        verifier
            .finalize()
            .expect("CIDv1 dag-pb multi-level DAG should verify against known IPFS CID");
    }

    #[test]
    fn test_compute_cid_small_file() {
        let data = b"hello dag-pb world";
        let cid = compute_cid(data);

        let expected = ItemHash::Ipfs(cid);
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(data);
        verifier.finalize().expect("computed CID should verify");
    }

    #[test]
    fn test_compute_cid_large_file() {
        let data = vec![0xABu8; 262144 + 100];
        let cid = compute_cid(&data);

        let expected = ItemHash::Ipfs(cid);
        let mut verifier = HashVerifier::new(&expected).unwrap();
        verifier.update(&data);
        verifier.finalize().expect("computed CID should verify");
    }

    #[test]
    fn test_hasher_for_storage() {
        let data = b"hello world";
        let mut hasher = Hasher::for_storage();
        hasher.update(data);
        let hash = hasher.finalize();
        assert_eq!(hash, ItemHash::Native(AlephItemHash::from_bytes(data)));
    }

    #[test]
    fn test_hasher_for_storage_chunked() {
        let data = b"hello world";
        let mut hasher = Hasher::for_storage();
        hasher.update(b"hello ");
        hasher.update(b"world");
        let hash = hasher.finalize();
        assert_eq!(hash, ItemHash::Native(AlephItemHash::from_bytes(data)));
    }

    #[test]
    fn test_hasher_for_ipfs() {
        let data = b"hello dag-pb world";
        let mut hasher = Hasher::for_ipfs();
        hasher.update(data);
        let hash = hasher.finalize();
        let expected_cid = compute_cid(data);
        assert_eq!(hash, ItemHash::Ipfs(expected_cid));
    }

    #[test]
    fn test_hasher_for_ipfs_large() {
        let data = vec![0xABu8; CHUNK_SIZE + 100];
        let mut hasher = Hasher::for_ipfs();
        hasher.update(&data);
        let hash = hasher.finalize();
        let expected_cid = compute_cid(&data);
        assert_eq!(hash, ItemHash::Ipfs(expected_cid));
    }

    #[test]
    fn for_ipfs_v1_raw_leaves_hashes_short_input() {
        let mut h = Hasher::for_ipfs_v1_raw_leaves();
        h.update(b"hello\n");
        let item = h.finalize();
        let cid = match item {
            ItemHash::Ipfs(c) => c,
            _ => panic!("expected ItemHash::Ipfs"),
        };
        // CIDv1 raw for "hello\n" (6 bytes): SHA-256("hello\n") = 5891b5b5...
        // encoded as [0x01, 0x55, 0x12, 0x20, ...digest...] in base32lower.
        // Verified: printf 'hello\n' | sha256sum gives 5891b5b5...
        assert_eq!(
            cid.to_string(),
            "bafkreicysg23kiwv34eg2d7qweipxwosdo2py4ldv42nbauguluen5v6am"
        );
    }
}
