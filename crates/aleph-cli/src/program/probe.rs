//! Embedded ASGI probe used by `aleph program check-runtime`.
//!
//! The probe is a tiny ASGI app (entrypoint `main:app`) that returns a JSON map
//! of the software versions the runtime provides. It is zipped at runtime so it
//! travels the same `zip` encoding path as a real program.

/// A minimal ASGI app reporting runtime versions as a JSON object.
pub const PROBE_MAIN_PY: &str = r#"
import json, platform, subprocess, sys


def _v(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
    except Exception:
        return ""


def _distribution():
    try:
        return platform.freedesktop_os_release().get("PRETTY_NAME", "")
    except Exception:
        return ""


def _pip():
    out = _v(["pip", "--version"])
    parts = out.split()
    return parts[1] if len(parts) > 1 else ""


async def app(scope, receive, send):
    versions = {
        "Distribution": _distribution(),
        "Python": sys.version.split()[0],
        "Node.js": _v(["node", "--version"]),
        "pip": _pip(),
    }
    body = json.dumps(versions).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [[b"content-type", b"application/json"]],
        }
    )
    await send({"type": "http.response.body", "body": body})
"#;

/// Build a zip archive containing `main.py` with the probe, returning the bytes.
pub fn probe_zip() -> std::io::Result<Vec<u8>> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::SimpleFileOptions = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file("main.py", opts)?;
        zip.write_all(PROBE_MAIN_PY.as_bytes())?;
        zip.finish()?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_zip_contains_main_py() {
        let bytes = probe_zip().unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        let entry = archive.by_name("main.py").unwrap();
        assert_eq!(entry.name(), "main.py");
    }
}
