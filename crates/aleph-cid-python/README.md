# aleph-cid (Python)

kubo-compatible IPFS CID computation for [Aleph Cloud](https://aleph.cloud), backed by the
[`aleph-cid`](https://crates.io/crates/aleph-cid) Rust crate. Computes the exact CIDs that
kubo would assign, without running an IPFS node: streaming file hashing (CIDv0 and CIDv1),
UnixFS folder DAGs (plain and HAMT-sharded directories), and CARv1 packing.

All hashing releases the GIL, so it can run on worker threads without blocking the
interpreter.

## Install

```sh
pip install aleph-cid
```

Prebuilt abi3 wheels cover CPython 3.10+ on Linux (x86_64, aarch64), macOS, and Windows.

## Usage

```python
import aleph_cid

# One-shot CIDv0 (kubo `ipfs add` default)
aleph_cid.compute_cid(b"hello\n")

# Streaming, for large files
hasher = aleph_cid.CidHasher.for_ipfs_v1_raw_leaves()
with open("video.mp4", "rb") as f:
    while chunk := f.read(1 << 20):
        hasher.update(chunk)
cid = hasher.finalize()

# Verify data against an existing CID of any supported flavor
hasher = aleph_cid.CidHasher.for_expected(cid)
hasher.update(data)
assert hasher.finalize() == cid

# Folder root CID, matching kubo `/api/v0/add?wrap-with-directory=true`
root = aleph_cid.hash_folder("./site", cid_version=1)

# Pack a folder into a CARv1 file (for `/api/v0/ipfs/add_car` uploads)
root = aleph_cid.write_folder_car("./site", "site.car")
assert aleph_cid.read_carv1_root("site.car") == root
```

## Compatibility

CIDs match kubo defaults: sha2-256, 256 KiB chunks, balanced DAGs with 174 links per node,
dag-pb leaves for CIDv0 and raw leaves for CIDv1, directories HAMT-sharded above 256 KiB
of link data. Golden tests pin the output against real kubo.

## Development

Built with [maturin](https://www.maturin.rs):

```sh
cd crates/aleph-cid-python
python -m venv .venv && . .venv/bin/activate
pip install maturin pytest
maturin develop
pytest
```
