//! CARv1 header parser used by `/api/v0/ipfs/add_car`.
//!
//! This intentionally reads only the varint-prefixed DAG-CBOR header. Kubo's
//! `/dag/import` validates the block data during import.

use cid::{Cid, Version};
use std::io::{Cursor, Read as StdRead};
use std::path::Path;
use tokio::io::AsyncReadExt;

use crate::{AlephError, AlephResult};

const MAX_CAR_HEADER_BYTES: usize = 8 * 1024;
const MAX_VARINT_BYTES: usize = 10;
const CBOR_MAJOR_UNSIGNED: u8 = 0;
const CBOR_MAJOR_BYTES: u8 = 2;
const CBOR_MAJOR_TEXT: u8 = 3;
const CBOR_MAJOR_ARRAY: u8 = 4;
const CBOR_MAJOR_MAP: u8 = 5;
const CBOR_MAJOR_TAG: u8 = 6;
const CBOR_TAG_CID: u64 = 42;

pub fn read_carv1_root(bytes: &[u8]) -> AlephResult<String> {
    let mut cursor = Cursor::new(bytes);
    let header_len = read_varint(&mut cursor)?;
    if header_len > MAX_CAR_HEADER_BYTES as u64 {
        return Err(invalid_car(format!(
            "header exceeds maximum size ({header_len} > {MAX_CAR_HEADER_BYTES})"
        )));
    }
    let mut header = vec![0u8; header_len as usize];
    StdRead::read_exact(&mut cursor, &mut header)
        .map_err(|_| invalid_car("truncated header"))?;
    parse_carv1_header(&header)
}

pub async fn read_carv1_root_from_path(path: &Path) -> AlephResult<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let header_len = read_varint_async(&mut file).await?;
    if header_len > MAX_CAR_HEADER_BYTES as u64 {
        return Err(invalid_car(format!(
            "header exceeds maximum size ({header_len} > {MAX_CAR_HEADER_BYTES})"
        )));
    }
    let mut header = vec![0u8; header_len as usize];
    file.read_exact(&mut header)
        .await
        .map_err(|_| invalid_car("truncated header"))?;
    parse_carv1_header(&header)
}

fn parse_carv1_header(header: &[u8]) -> AlephResult<String> {
    let mut cbor = Cursor::new(header);
    let map_len = read_cbor_len(&mut cbor, CBOR_MAJOR_MAP)?;
    let mut version: Option<u64> = None;
    let mut roots: Option<Vec<String>> = None;

    for _ in 0..map_len {
        let key = read_cbor_text(&mut cbor)?;
        match key.as_str() {
            "version" => version = Some(read_cbor_uint(&mut cbor)?),
            "roots" => roots = Some(read_cbor_cid_array(&mut cbor)?),
            _ => skip_cbor_value(&mut cbor)?,
        }
    }

    if version != Some(1) {
        return Err(invalid_car(format!(
            "unsupported CAR version (got {:?}, expected 1)",
            version
        )));
    }
    let roots = roots.ok_or_else(|| invalid_car("malformed DAG-CBOR header: roots is missing"))?;
    if roots.len() != 1 {
        return Err(invalid_car(format!(
            "expected exactly 1 root, got {}",
            roots.len()
        )));
    }
    Ok(roots[0].clone())
}

fn read_varint(reader: &mut Cursor<&[u8]>) -> AlephResult<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    for _ in 0..MAX_VARINT_BYTES {
        let byte = read_u8(reader).map_err(|_| invalid_car("malformed varint: truncated"))?;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(invalid_car("malformed varint: exceeds 10 bytes"))
}

async fn read_varint_async(file: &mut tokio::fs::File) -> AlephResult<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    let mut buf = [0u8; 1];
    for _ in 0..MAX_VARINT_BYTES {
        file.read_exact(&mut buf)
            .await
            .map_err(|_| invalid_car("malformed varint: truncated"))?;
        let byte = buf[0];
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(invalid_car("malformed varint: exceeds 10 bytes"))
}

fn read_cbor_cid_array(reader: &mut Cursor<&[u8]>) -> AlephResult<Vec<String>> {
    let len = read_cbor_len(reader, CBOR_MAJOR_ARRAY)?;
    let mut roots = Vec::with_capacity(len as usize);
    for _ in 0..len {
        roots.push(read_cbor_cid(reader)?);
    }
    Ok(roots)
}

fn read_cbor_cid(reader: &mut Cursor<&[u8]>) -> AlephResult<String> {
    let tag = read_cbor_len(reader, CBOR_MAJOR_TAG)?;
    if tag != CBOR_TAG_CID {
        return Err(invalid_car(format!("malformed root CID: unexpected tag {tag}")));
    }
    let raw = read_cbor_bytes(reader)?;
    let cid_bytes = raw
        .strip_prefix(&[0])
        .ok_or_else(|| invalid_car("malformed root CID: missing identity multibase prefix"))?;
    let cid = Cid::read_bytes(Cursor::new(cid_bytes))
        .map_err(|e| invalid_car(format!("malformed root CID: {e}")))?;
    canonical_cid_string(&cid)
}

fn canonical_cid_string(cid: &Cid) -> AlephResult<String> {
    if cid.version() == Version::V1 {
        Ok(cid.to_string())
    } else {
        Ok(cid.to_string())
    }
}

fn read_cbor_text(reader: &mut Cursor<&[u8]>) -> AlephResult<String> {
    let len = read_cbor_len(reader, CBOR_MAJOR_TEXT)?;
    let bytes = read_exact(reader, len as usize)?;
    String::from_utf8(bytes).map_err(|e| invalid_car(format!("malformed DAG-CBOR header: {e}")))
}

fn read_cbor_bytes(reader: &mut Cursor<&[u8]>) -> AlephResult<Vec<u8>> {
    let len = read_cbor_len(reader, CBOR_MAJOR_BYTES)?;
    read_exact(reader, len as usize)
}

fn read_cbor_uint(reader: &mut Cursor<&[u8]>) -> AlephResult<u64> {
    read_cbor_len(reader, CBOR_MAJOR_UNSIGNED)
}

fn read_cbor_len(reader: &mut Cursor<&[u8]>, expected_major: u8) -> AlephResult<u64> {
    let first = read_u8(reader)?;
    let major = first >> 5;
    if major != expected_major {
        return Err(invalid_car("malformed DAG-CBOR header: unexpected type"));
    }
    let addl = first & 0x1f;
    match addl {
        n @ 0..=23 => Ok(u64::from(n)),
        24 => Ok(u64::from(read_u8(reader)?)),
        25 => Ok(u64::from(u16::from_be_bytes(read_array(reader)?))),
        26 => Ok(u64::from(u32::from_be_bytes(read_array(reader)?))),
        27 => Ok(u64::from_be_bytes(read_array(reader)?)),
        _ => Err(invalid_car(
            "malformed DAG-CBOR header: indefinite lengths are unsupported",
        )),
    }
}

fn skip_cbor_value(reader: &mut Cursor<&[u8]>) -> AlephResult<()> {
    let first = read_u8(reader)?;
    let major = first >> 5;
    let addl = first & 0x1f;
    let len = match addl {
        n @ 0..=23 => u64::from(n),
        24 => u64::from(read_u8(reader)?),
        25 => u64::from(u16::from_be_bytes(read_array(reader)?)),
        26 => u64::from(u32::from_be_bytes(read_array(reader)?)),
        27 => u64::from_be_bytes(read_array(reader)?),
        _ => {
            return Err(invalid_car(
                "malformed DAG-CBOR header: indefinite lengths are unsupported",
            ));
        }
    };
    match major {
        CBOR_MAJOR_UNSIGNED => Ok(()),
        CBOR_MAJOR_BYTES | CBOR_MAJOR_TEXT => {
            read_exact(reader, len as usize)?;
            Ok(())
        }
        CBOR_MAJOR_ARRAY => {
            for _ in 0..len {
                skip_cbor_value(reader)?;
            }
            Ok(())
        }
        CBOR_MAJOR_MAP => {
            for _ in 0..len {
                skip_cbor_value(reader)?;
                skip_cbor_value(reader)?;
            }
            Ok(())
        }
        CBOR_MAJOR_TAG => skip_cbor_value(reader),
        _ => Err(invalid_car("malformed DAG-CBOR header: unsupported type")),
    }
}

fn read_exact(reader: &mut Cursor<&[u8]>, len: usize) -> AlephResult<Vec<u8>> {
    let mut bytes = vec![0u8; len];
    StdRead::read_exact(reader, &mut bytes)
        .map_err(|_| invalid_car("malformed DAG-CBOR header: truncated value"))?;
    Ok(bytes)
}

fn read_array<const N: usize>(reader: &mut Cursor<&[u8]>) -> AlephResult<[u8; N]> {
    let mut bytes = [0u8; N];
    StdRead::read_exact(reader, &mut bytes)
        .map_err(|_| invalid_car("malformed DAG-CBOR header: truncated value"))?;
    Ok(bytes)
}

fn read_u8(reader: &mut Cursor<&[u8]>) -> AlephResult<u8> {
    let mut buf = [0u8; 1];
    StdRead::read_exact(reader, &mut buf)
        .map_err(|_| invalid_car("malformed DAG-CBOR header: truncated value"))?;
    Ok(buf[0])
}

fn invalid_car(message: impl Into<String>) -> AlephError {
    AlephError::InvalidMessage(format!("Invalid CAR file: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid;
    use multihash_codetable::{Code, MultihashDigest};

    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if value == 0 {
                return out;
            }
        }
    }

    fn sample_car_header(root: Cid, version: u8) -> Vec<u8> {
        let cid_bytes = root.to_bytes();
        let mut header = Vec::new();
        header.push(0xa2);
        header.extend_from_slice(b"\x65roots\x81\xd8\x2a");
        header.push(0x58);
        header.push((cid_bytes.len() + 1) as u8);
        header.push(0x00);
        header.extend_from_slice(&cid_bytes);
        header.extend_from_slice(b"\x67version");
        header.push(version);

        let mut car = varint(header.len() as u64);
        car.extend_from_slice(&header);
        car.extend_from_slice(b"blocks-not-read");
        car
    }

    #[test]
    fn read_carv1_root_returns_canonical_cid() {
        let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(b"root"));
        let car = sample_car_header(cid, 1);
        let root = read_carv1_root(&car).unwrap();
        assert_eq!(root, cid.to_string());
    }

    #[test]
    fn read_carv1_root_rejects_wrong_version() {
        let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(b"root"));
        let car = sample_car_header(cid, 2);
        let err = read_carv1_root(&car).unwrap_err();
        assert!(err.to_string().contains("unsupported CAR version"));
    }

    #[tokio::test]
    async fn read_carv1_root_from_path_reads_header_only() {
        let cid = Cid::new_v1(0x55, Code::Sha2_256.digest(b"root"));
        let car = sample_car_header(cid, 1);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sample.car");
        tokio::fs::write(&path, car).await.unwrap();
        let root = read_carv1_root_from_path(&path).await.unwrap();
        assert_eq!(root, cid.to_string());
    }
}
