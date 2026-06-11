//! Python bindings for the `aleph-cid` crate.
//!
//! Exposes kubo-compatible IPFS CID computation to Python: one-shot and
//! streaming file hashing, UnixFS folder DAG hashing, and CARv1 writing.
//! The module is published to PyPI as `aleph-cid` (import name `aleph_cid`).

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use aleph_cid::car::{read_carv1_root, write_block_frame, write_carv1_header};
use aleph_cid::cid::Cid;
use aleph_cid::folder_hash::{FolderHashError, build_folder_dag, hash_folder_root};
use aleph_cid::verify::Hasher;
use aleph_cid::{CidVersion, CollectError, FolderEntry, UploadFolderOptions, collect_folder_files};
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::prelude::*;

fn folder_options(cid_version: u8, follow_symlinks: bool) -> PyResult<UploadFolderOptions> {
    let mut opts = UploadFolderOptions::default();
    opts.cid_version = match cid_version {
        0 => CidVersion::V0,
        1 => CidVersion::V1,
        other => {
            return Err(PyValueError::new_err(format!(
                "cid_version must be 0 or 1, got {other}"
            )));
        }
    };
    opts.follow_symlinks = follow_symlinks;
    Ok(opts)
}

fn parse_cid(s: &str) -> PyResult<Cid> {
    Cid::try_from(s.to_owned()).map_err(|e| PyValueError::new_err(e.to_string()))
}

fn collect_error(e: CollectError) -> PyErr {
    match e {
        CollectError::Empty(_) | CollectError::NonUtf8(_) => PyValueError::new_err(e.to_string()),
        _ => PyOSError::new_err(e.to_string()),
    }
}

fn folder_hash_error(e: FolderHashError) -> PyErr {
    PyOSError::new_err(e.to_string())
}

fn collect_entries(path: &Path, follow_symlinks: bool) -> PyResult<Vec<FolderEntry>> {
    collect_folder_files(path, follow_symlinks).map_err(collect_error)
}

/// Compute the kubo-default CIDv0 of `data` in one shot.
///
/// Equivalent to `ipfs add --only-hash` on a file with the same content.
#[pyfunction]
#[pyo3(name = "compute_cid")]
fn compute_cid_py(py: Python<'_>, data: &[u8]) -> String {
    py.detach(|| aleph_cid::verify::compute_cid(data).to_string())
}

/// Streaming CID hasher.
///
/// Construct via one of the static methods, feed data with `update()`, and
/// obtain the final CID string with `finalize()`. `update()` releases the
/// GIL while hashing, so large uploads can hash on a worker thread without
/// blocking the interpreter.
#[pyclass(name = "CidHasher")]
struct PyCidHasher {
    inner: Option<Hasher>,
}

impl PyCidHasher {
    fn take_inner(&mut self) -> PyResult<Hasher> {
        self.inner
            .take()
            .ok_or_else(|| PyValueError::new_err("hasher already finalized"))
    }
}

#[pymethods]
impl PyCidHasher {
    /// Hasher producing the kubo-default CIDv0 (dag-pb wrapped leaves).
    #[staticmethod]
    fn for_ipfs() -> Self {
        Self {
            inner: Some(Hasher::for_ipfs()),
        }
    }

    /// Hasher producing the kubo `--cid-version=1` default CIDv1
    /// (raw leaves, base32).
    #[staticmethod]
    fn for_ipfs_v1_raw_leaves() -> Self {
        Self {
            inner: Some(Hasher::for_ipfs_v1_raw_leaves()),
        }
    }

    /// Hasher matching the flavor of an existing CID string, so that hashing
    /// the same bytes reproduces it. Raises ValueError for CID formats the
    /// hasher cannot reproduce (non-sha2-256, exotic codecs).
    #[staticmethod]
    fn for_expected(cid: &str) -> PyResult<Self> {
        let cid = parse_cid(cid)?;
        let inner = Hasher::for_expected(&cid).map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(Self { inner: Some(inner) })
    }

    /// Absorb a chunk of data. Chunks may be of any size; chunking is
    /// internal and matches kubo (256 KiB).
    fn update(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let inner = self
            .inner
            .as_mut()
            .ok_or_else(|| PyValueError::new_err("hasher already finalized"))?;
        py.detach(|| inner.update(data));
        Ok(())
    }

    /// Consume the hasher and return the CID string. Subsequent calls to
    /// `update()` or `finalize()` raise ValueError.
    fn finalize(&mut self, py: Python<'_>) -> PyResult<String> {
        let inner = self.take_inner()?;
        Ok(py.detach(|| inner.finalize().to_string()))
    }
}

/// Compute the UnixFS root CID of a folder, matching kubo's HTTP
/// `/api/v0/add?wrap-with-directory=true` on the same file set
/// (and `ipfs add -r --no-symlinks` when `follow_symlinks` is true).
///
/// Raises ValueError on an empty folder or non-UTF-8 file names, OSError on
/// I/O failures.
#[pyfunction]
#[pyo3(signature = (path, *, cid_version = 1, follow_symlinks = true))]
fn hash_folder(
    py: Python<'_>,
    path: PathBuf,
    cid_version: u8,
    follow_symlinks: bool,
) -> PyResult<String> {
    let opts = folder_options(cid_version, follow_symlinks)?;
    py.detach(|| {
        let entries = collect_entries(&path, follow_symlinks)?;
        hash_folder_root(&entries, &opts)
            .map(|cid| cid.to_string())
            .map_err(folder_hash_error)
    })
}

/// Pack a folder into a CARv1 file at `car_path` and return the root CID.
///
/// The root CID is identical to `hash_folder()` on the same folder. Blocks
/// are streamed through a temporary file next to `car_path`, so memory usage
/// stays flat regardless of folder size.
#[pyfunction]
#[pyo3(signature = (path, car_path, *, cid_version = 1, follow_symlinks = true))]
fn write_folder_car(
    py: Python<'_>,
    path: PathBuf,
    car_path: PathBuf,
    cid_version: u8,
    follow_symlinks: bool,
) -> PyResult<String> {
    let opts = folder_options(cid_version, follow_symlinks)?;
    py.detach(|| {
        let entries = collect_entries(&path, follow_symlinks)?;

        // CARv1 puts the root CID in the header, but the root is only known
        // after walking the whole DAG: stream block frames to a tempfile,
        // then assemble header + frames into the final file.
        let tmp_dir = car_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let mut body_tmp = tempfile::NamedTempFile::new_in(tmp_dir)?;
        let mut last_cid_bytes: Option<Vec<u8>> = None;
        let root = build_folder_dag(&entries, &opts, &mut |cid_bytes, block| {
            write_block_frame(&mut body_tmp, cid_bytes, block)?;
            last_cid_bytes = Some(cid_bytes.to_vec());
            Ok(())
        })
        .map_err(folder_hash_error)?;
        body_tmp.flush()?;
        let root_cid_bytes =
            last_cid_bytes.expect("build_folder_dag always emits at least the root");

        let mut out = BufWriter::new(File::create(&car_path)?);
        write_carv1_header(&mut out, &root_cid_bytes)?;
        let mut body = body_tmp.reopen()?;
        io::copy(&mut body, &mut out)?;
        out.flush()?;

        Ok(root.to_string())
    })
}

/// Read the single root CID from a CARv1 file header.
///
/// Raises ValueError if the file is not a valid single-root CARv1.
#[pyfunction]
#[pyo3(name = "read_carv1_root")]
fn read_carv1_root_py(py: Python<'_>, path: PathBuf) -> PyResult<String> {
    py.detach(|| read_carv1_root(&path).map_err(|e| PyValueError::new_err(e.to_string())))
}

#[pymodule]
#[pyo3(name = "aleph_cid")]
fn aleph_cid_module(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compute_cid_py, m)?)?;
    m.add_function(wrap_pyfunction!(hash_folder, m)?)?;
    m.add_function(wrap_pyfunction!(write_folder_car, m)?)?;
    m.add_function(wrap_pyfunction!(read_carv1_root_py, m)?)?;
    m.add_class::<PyCidHasher>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
