use aleph_types::message::execution::base::Encoding;
use anyhow::{Context, Result, bail};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Either a borrowed path (for pre-built archives the caller already has on
/// disk) or an owned tempfile holding a freshly-zipped directory.
#[derive(Debug)]
pub enum PreparedArchive {
    Borrowed(PathBuf),
    Owned(tempfile::NamedTempFile),
}

impl PreparedArchive {
    pub fn path(&self) -> &Path {
        match self {
            Self::Borrowed(p) => p,
            Self::Owned(f) => f.path(),
        }
    }
}

/// Detect encoding and prepare an archive at the returned path.
///
/// Mirrors aleph_client/utils.py::create_archive:
/// - directory -> zip (ZIP_DEFLATED, no filtering, mtime/mode preserved) - implemented in Task 6
/// - .squashfs file or squashfs-magic file -> Encoding::Squashfs, returned as-is
/// - other file -> validated as non-empty zip, Encoding::Zip
pub fn prepare_archive(path: &Path) -> Result<(PreparedArchive, Encoding)> {
    if !path.exists() {
        bail!("path not found: {}", path.display());
    }
    if path.is_dir() {
        let tmp = zip_directory(path)
            .with_context(|| format!("failed to zip directory {}", path.display()))?;
        return Ok((PreparedArchive::Owned(tmp), Encoding::Zip));
    }
    if path.is_file() {
        if is_squashfs(path)? {
            return Ok((
                PreparedArchive::Borrowed(path.to_path_buf()),
                Encoding::Squashfs,
            ));
        }
        validate_zip(path).with_context(|| format!("not a valid zip: {}", path.display()))?;
        return Ok((PreparedArchive::Borrowed(path.to_path_buf()), Encoding::Zip));
    }
    bail!("not a regular file or directory: {}", path.display())
}

fn is_squashfs(path: &Path) -> Result<bool> {
    if path.extension().and_then(|s| s.to_str()) == Some("squashfs") {
        return Ok(true);
    }
    let mut buf = [0u8; 4];
    let mut f = fs::File::open(path)?;
    let n = f.read(&mut buf)?;
    Ok(n == 4 && &buf == b"hsqs")
}

fn validate_zip(path: &Path) -> Result<()> {
    let f = fs::File::open(path)?;
    let mut zip = zip::ZipArchive::new(f).context("invalid zip archive")?;
    if zip.is_empty() {
        bail!("zip archive contains no entries");
    }
    let _ = zip
        .by_index(0)
        .context("zip archive central directory is unreadable")?;
    Ok(())
}

fn zip_directory(dir: &Path) -> Result<tempfile::NamedTempFile> {
    let tmp = tempfile::Builder::new()
        .prefix("aleph-program-")
        .suffix(".zip")
        .tempfile()?;
    let f = std::io::BufWriter::new(tmp.reopen()?);
    let mut zip = zip::ZipWriter::new(f);
    let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    for entry in walkdir::WalkDir::new(dir).follow_links(false) {
        let entry = entry?;
        let abs = entry.path();
        let rel = abs.strip_prefix(dir)?;
        if rel.as_os_str().is_empty() {
            continue;
        }
        let name = rel.to_string_lossy().replace('\\', "/");

        let metadata = entry.metadata()?;
        let mut entry_opts = opts;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            entry_opts = entry_opts.unix_permissions(metadata.permissions().mode());
        }
        #[cfg(not(unix))]
        let _ = metadata;

        if entry.file_type().is_dir() {
            zip.add_directory(name, entry_opts)?;
        } else if entry.file_type().is_file() {
            zip.start_file(name, entry_opts)?;
            let mut src = fs::File::open(abs)?;
            std::io::copy(&mut src, &mut zip)?;
        } else if entry.file_type().is_symlink() {
            let target = fs::read_link(abs)?;
            zip.add_symlink(name, target.to_string_lossy(), entry_opts)?;
        }
    }

    zip.finish()?;
    Ok(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn detect_squashfs_by_extension() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("rootfs.squashfs");
        fs::write(&p, b"\0\0\0\0unused").unwrap();
        let (archive, enc) = prepare_archive(&p).unwrap();
        assert_eq!(enc, Encoding::Squashfs);
        assert_eq!(archive.path(), p);
    }

    #[test]
    fn detect_squashfs_by_magic() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("noext");
        fs::write(&p, b"hsqs....rest").unwrap();
        let (_archive, enc) = prepare_archive(&p).unwrap();
        assert_eq!(enc, Encoding::Squashfs);
    }

    #[test]
    fn empty_zip_rejected() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("empty.zip");
        let f = fs::File::create(&p).unwrap();
        let w = zip::ZipWriter::new(f);
        w.finish().unwrap();
        let err = prepare_archive(&p).unwrap_err();
        assert!(
            format!("{err:#}").contains("contains no entries"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn valid_zip_accepted() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("ok.zip");
        let f = fs::File::create(&p).unwrap();
        let mut w = zip::ZipWriter::new(f);
        let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default();
        w.start_file("hello.txt", opts).unwrap();
        w.write_all(b"hi").unwrap();
        w.finish().unwrap();

        let (archive, enc) = prepare_archive(&p).unwrap();
        assert_eq!(enc, Encoding::Zip);
        assert_eq!(archive.path(), p);
    }

    #[test]
    fn missing_path_errors() {
        let err = prepare_archive(Path::new("/nonexistent/__nope__")).unwrap_err();
        assert!(
            format!("{err:#}").to_lowercase().contains("not found"),
            "unexpected error: {err:#}"
        );
    }

    #[test]
    fn directory_zip_includes_hidden_and_dotgit_with_no_filtering() {
        let src = tempdir().unwrap();
        fs::write(src.path().join("main.py"), b"print('hi')\n").unwrap();
        fs::create_dir(src.path().join("sub")).unwrap();
        fs::write(src.path().join("sub/util.py"), b"x = 1\n").unwrap();
        fs::write(src.path().join(".env"), b"SECRET=1\n").unwrap();
        fs::create_dir(src.path().join(".git")).unwrap();
        fs::write(src.path().join(".git/config"), b"[core]\n").unwrap();

        let (archive, enc) = prepare_archive(src.path()).unwrap();
        assert_eq!(enc, Encoding::Zip);

        let f = fs::File::open(archive.path()).unwrap();
        let mut zip = zip::ZipArchive::new(f).unwrap();
        let mut names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();

        assert!(names.iter().any(|n| n == "main.py"));
        assert!(names.iter().any(|n| n == "sub/util.py"));
        assert!(names.iter().any(|n| n == ".env"));
        assert!(names.iter().any(|n| n == ".git/config"));
    }

    #[cfg(unix)]
    #[test]
    fn directory_zip_preserves_executable_bit() {
        use std::os::unix::fs::PermissionsExt;
        let src = tempdir().unwrap();
        let exe = src.path().join("run.sh");
        fs::write(&exe, b"#!/bin/sh\necho hi\n").unwrap();
        fs::set_permissions(&exe, fs::Permissions::from_mode(0o755)).unwrap();

        let (archive, _) = prepare_archive(src.path()).unwrap();
        let f = fs::File::open(archive.path()).unwrap();
        let mut zip = zip::ZipArchive::new(f).unwrap();
        let entry = zip.by_name("run.sh").unwrap();
        let mode = entry.unix_mode().unwrap_or(0);
        assert_eq!(
            mode & 0o111,
            0o111,
            "executable bit lost; got mode {mode:o}"
        );
    }

    #[test]
    fn directory_zip_uses_deflated_compression() {
        let src = tempdir().unwrap();
        // Use a payload that compresses well so DEFLATE shows a clear win.
        fs::write(src.path().join("payload.txt"), "x".repeat(4096).as_bytes()).unwrap();
        let (archive, _) = prepare_archive(src.path()).unwrap();
        let f = fs::File::open(archive.path()).unwrap();
        let mut zip = zip::ZipArchive::new(f).unwrap();
        let entry = zip.by_name("payload.txt").unwrap();
        assert_eq!(entry.compression(), zip::CompressionMethod::Deflated);
    }
}
