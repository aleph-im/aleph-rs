//! CARv1 framing for IPFS directory uploads.
//!
//! Reference: https://ipld.io/specs/transport/car/carv1/
//!
//! Provides hand-rolled writers (`write_carv1_header`, `write_block_frame`)
//! and a strict reader (`read_carv1_root`) that heph re-uses to validate
//! uploaded CARs in its stub `add_car` handler.

use std::io::{self, Read, Write};

/// Upper bound on the declared CARv1 header size. A single-root header is
/// ~40 bytes; this cap exists to bound allocations from a malicious varint.
pub(crate) const MAX_CAR_HEADER_BYTES: usize = 8 * 1024;

/// Write an unsigned LEB128 varint.
pub(crate) fn write_uvarint<W: Write>(w: &mut W, mut n: u64) -> io::Result<()> {
    while n >= 0x80 {
        w.write_all(&[(n as u8) | 0x80])?;
        n >>= 7;
    }
    w.write_all(&[n as u8])
}

/// Build the DAG-CBOR header bytes for a single-root CARv1.
///
/// The header is a 2-entry map `{"roots": [<root>], "version": 1}`. DAG-CBOR
/// requires deterministic key order (length-first then lexicographic on the
/// CBOR-encoded keys); "roots" (6 bytes encoded) sorts before "version" (8).
///
/// `root_cid_bytes` is the canonical binary CID: for CIDv1, `[varint(1),
/// varint(codec), multihash]`; for CIDv0, the bare multihash. Either is
/// accepted; DAG-CBOR wraps it with the 0x00 multibase identity prefix.
pub(crate) fn build_dagcbor_header(root_cid_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(48 + root_cid_bytes.len());
    out.push(0xA2); // map, 2 entries
    // key "roots" (text string, length 5)
    out.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
    // value: array of 1 CID
    out.push(0x81); // array, 1 entry
    out.push(0xD8); // tag, 1-byte tag value follows
    out.push(0x2A); // tag 42 (CID)
    // byte string: 0x00 multibase prefix + cid_bytes
    let bytestring_len = 1 + root_cid_bytes.len();
    write_cbor_bytestring_header(&mut out, bytestring_len);
    out.push(0x00);
    out.extend_from_slice(root_cid_bytes);
    // key "version" (text string, length 7)
    out.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
    // value: uint 1 (CBOR short form)
    out.push(0x01);
    out
}

fn write_cbor_bytestring_header(out: &mut Vec<u8>, len: usize) {
    // Major type 2 (byte string). Short form 0x40..=0x57 for len 0..=23.
    if len <= 23 {
        out.push(0x40 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(0x58);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0x59);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else if len <= u32::MAX as usize {
        out.push(0x5A);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    } else {
        out.push(0x5B);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
}

/// Write a complete CARv1 header (leading varint length + DAG-CBOR header)
/// for a single-root file.
pub(crate) fn write_carv1_header<W: Write>(w: &mut W, root_cid_bytes: &[u8]) -> io::Result<()> {
    let header = build_dagcbor_header(root_cid_bytes);
    write_uvarint(w, header.len() as u64)?;
    w.write_all(&header)
}

/// Write one CARv1 block frame: `varint(cid_len + data_len) || cid || data`.
pub(crate) fn write_block_frame<W: Write>(
    w: &mut W,
    cid_bytes: &[u8],
    block_bytes: &[u8],
) -> io::Result<()> {
    let total = cid_bytes.len() + block_bytes.len();
    write_uvarint(w, total as u64)?;
    w.write_all(cid_bytes)?;
    w.write_all(block_bytes)
}

/// Errors produced by [`read_carv1_root`].
#[derive(Debug, thiserror::Error)]
pub enum InvalidCarFile {
    #[error("malformed varint")]
    MalformedVarint,
    #[error("declared header size exceeds maximum ({MAX_CAR_HEADER_BYTES} bytes)")]
    HeaderTooLarge,
    #[error("truncated CAR header")]
    TruncatedHeader,
    #[error("malformed DAG-CBOR header")]
    MalformedHeader,
    #[error("unsupported CAR version (got {got}, expected 1)")]
    UnsupportedVersion { got: u64 },
    #[error("expected exactly 1 root, got {got}")]
    BadRootCount { got: usize },
    #[error("malformed root CID")]
    MalformedRootCid,
    #[error("I/O error reading CAR file: {0}")]
    Io(#[from] std::io::Error),
}

/// Read the CARv1 header from `path` and return its single root CID as a
/// canonical string (base32 CIDv1 or base58btc CIDv0). Does not read or
/// validate any block past the header.
pub fn read_carv1_root(path: &std::path::Path) -> Result<String, InvalidCarFile> {
    let mut f = std::fs::File::open(path)?;
    read_carv1_root_from(&mut f)
}

/// `Read`-based variant of [`read_carv1_root`]; the file entry point is a
/// thin wrapper around this.
pub(crate) fn read_carv1_root_from<R: Read>(r: &mut R) -> Result<String, InvalidCarFile> {
    let header_len = read_uvarint_checked(r)?;
    if header_len > MAX_CAR_HEADER_BYTES as u64 {
        return Err(InvalidCarFile::HeaderTooLarge);
    }
    let mut header = vec![0u8; header_len as usize];
    r.read_exact(&mut header)
        .map_err(|_| InvalidCarFile::TruncatedHeader)?;
    parse_dagcbor_header(&header)
}

fn read_uvarint_checked<R: Read>(r: &mut R) -> Result<u64, InvalidCarFile> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for _ in 0..10 {
        let mut buf = [0u8; 1];
        if r.read_exact(&mut buf).is_err() {
            return Err(InvalidCarFile::MalformedVarint);
        }
        let byte = buf[0];
        let payload = (byte & 0x7F) as u64;
        // Reject over-long encodings: at shift 63, only payload 0 or 1 is valid,
        // and the continuation bit must be clear.
        if shift == 63 && (payload > 1 || byte & 0x80 != 0) {
            return Err(InvalidCarFile::MalformedVarint);
        }
        result |= payload << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(InvalidCarFile::MalformedVarint)
}

/// Parse the fixed `{"roots":[<cid>],"version":1}` shape this module emits.
/// Accept entries in either order (defensive parity with the pyaleph parser).
fn parse_dagcbor_header(bytes: &[u8]) -> Result<String, InvalidCarFile> {
    let mut cur = HeaderCursor::new(bytes);
    let head = cur.next_byte()?;
    if head != 0xA2 {
        return Err(InvalidCarFile::MalformedHeader);
    }
    let mut roots_cid: Option<Vec<u8>> = None;
    let mut roots_count: Option<usize> = None;
    let mut version: Option<u64> = None;
    for _ in 0..2 {
        let key = cur.read_text()?;
        match key.as_str() {
            "roots" => {
                let n = cur.read_array_header()?;
                roots_count = Some(n);
                if n != 1 {
                    // Report early - we cannot safely skip unknown array entries.
                    return Err(InvalidCarFile::BadRootCount { got: n });
                }
                let tag1 = cur.next_byte()?;
                let tag2 = cur.next_byte()?;
                if tag1 != 0xD8 || tag2 != 0x2A {
                    return Err(InvalidCarFile::MalformedRootCid);
                }
                let bs = cur.read_bytestring()?;
                if bs.is_empty() || bs[0] != 0x00 {
                    return Err(InvalidCarFile::MalformedRootCid);
                }
                roots_cid = Some(bs[1..].to_vec());
            }
            "version" => {
                version = Some(cur.read_uint()?);
            }
            _ => return Err(InvalidCarFile::MalformedHeader),
        }
    }
    if roots_count.is_none() {
        return Err(InvalidCarFile::MalformedHeader);
    }
    let cid_bytes = roots_cid.ok_or(InvalidCarFile::MalformedRootCid)?;
    let version = version.ok_or(InvalidCarFile::MalformedHeader)?;
    if version != 1 {
        return Err(InvalidCarFile::UnsupportedVersion { got: version });
    }
    let cid = ::cid::Cid::try_from(&cid_bytes[..]).map_err(|_| InvalidCarFile::MalformedRootCid)?;
    // Trailing bytes inside the declared header length are silently accepted.
    // Producers we control never pad; aligns with pyaleph's parser behaviour.
    Ok(cid.to_string())
}

struct HeaderCursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> HeaderCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    fn next_byte(&mut self) -> Result<u8, InvalidCarFile> {
        let b = *self
            .bytes
            .get(self.pos)
            .ok_or(InvalidCarFile::MalformedHeader)?;
        self.pos += 1;
        Ok(b)
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], InvalidCarFile> {
        if self.pos + n > self.bytes.len() {
            return Err(InvalidCarFile::MalformedHeader);
        }
        let out = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }
    fn read_text(&mut self) -> Result<String, InvalidCarFile> {
        let head = self.next_byte()?;
        let len = match head {
            0x60..=0x77 => (head - 0x60) as usize,
            0x78 => self.next_byte()? as usize,
            0x79 => {
                let hi = self.next_byte()? as u16;
                let lo = self.next_byte()? as u16;
                ((hi << 8) | lo) as usize
            }
            _ => return Err(InvalidCarFile::MalformedHeader),
        };
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(|s| s.to_string())
            .map_err(|_| InvalidCarFile::MalformedHeader)
    }
    fn read_array_header(&mut self) -> Result<usize, InvalidCarFile> {
        let head = self.next_byte()?;
        match head {
            0x80..=0x97 => Ok((head - 0x80) as usize),
            0x98 => Ok(self.next_byte()? as usize),
            0x99 => {
                let hi = self.next_byte()? as u16;
                let lo = self.next_byte()? as u16;
                Ok(((hi << 8) | lo) as usize)
            }
            _ => Err(InvalidCarFile::MalformedHeader),
        }
    }
    fn read_bytestring(&mut self) -> Result<&'a [u8], InvalidCarFile> {
        let head = self.next_byte()?;
        let len = match head {
            0x40..=0x57 => (head - 0x40) as usize,
            0x58 => self.next_byte()? as usize,
            0x59 => {
                let hi = self.next_byte()? as u16;
                let lo = self.next_byte()? as u16;
                ((hi << 8) | lo) as usize
            }
            _ => return Err(InvalidCarFile::MalformedHeader),
        };
        self.take(len)
    }
    fn read_uint(&mut self) -> Result<u64, InvalidCarFile> {
        let head = self.next_byte()?;
        match head {
            0x00..=0x17 => Ok(head as u64),
            0x18 => Ok(self.next_byte()? as u64),
            0x19 => Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()) as u64),
            0x1A => Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()) as u64),
            0x1B => Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap())),
            _ => Err(InvalidCarFile::MalformedHeader),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cid::Cid as RawCid;
    use multihash::Multihash;
    use std::io::Read;

    /// Read an unsigned LEB128 varint. Returns (value, bytes_consumed).
    // NOTE: This helper is only safe for round-tripping bytes from write_uvarint.
    // Task 3's production reader must additionally reject payload > 1 at shift == 63.
    fn read_uvarint(r: &mut impl Read) -> io::Result<(u64, usize)> {
        let mut result: u64 = 0;
        let mut shift = 0u32;
        let mut bytes_read = 0;
        loop {
            let mut buf = [0u8; 1];
            r.read_exact(&mut buf)?;
            bytes_read += 1;
            let byte = buf[0];
            let payload = (byte & 0x7F) as u64;
            if shift >= 64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "varint too long",
                ));
            }
            result |= payload << shift;
            if byte & 0x80 == 0 {
                return Ok((result, bytes_read));
            }
            shift += 7;
        }
    }

    #[test]
    fn varint_round_trip_small_values() {
        for n in [0u64, 1, 0x7F, 0x80, 0xFF, 0x3FFF, 0x4000, 0xFFFFFFFF] {
            let mut buf = Vec::new();
            write_uvarint(&mut buf, n).unwrap();
            let (got, consumed) = read_uvarint(&mut &buf[..]).unwrap();
            assert_eq!(got, n, "round trip failed for {n}");
            assert_eq!(consumed, buf.len(), "byte count off for {n}");
        }
    }

    #[test]
    fn varint_round_trip_u64_max() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, u64::MAX).unwrap();
        let (got, _) = read_uvarint(&mut &buf[..]).unwrap();
        assert_eq!(got, u64::MAX);
        assert_eq!(buf.len(), 10, "u64::MAX should use 10 bytes");
    }

    #[test]
    fn varint_encodes_127_as_one_byte() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 127).unwrap();
        assert_eq!(buf, vec![0x7F]);
    }

    #[test]
    fn varint_encodes_128_as_two_bytes() {
        let mut buf = Vec::new();
        write_uvarint(&mut buf, 128).unwrap();
        assert_eq!(buf, vec![0x80, 0x01]);
    }

    /// Build a CIDv1 dag-pb CID whose sha2-256 digest is `digest`. Used by
    /// header-byte tests to construct an arbitrary deterministic CID.
    fn make_cidv1_dagpb(digest: [u8; 32]) -> Vec<u8> {
        let mh = Multihash::<64>::wrap(0x12, &digest).unwrap();
        let cid = RawCid::new_v1(0x70, mh);
        cid.to_bytes()
    }

    #[test]
    fn dagcbor_header_v1_root_canonical_bytes() {
        // CIDv1 dag-pb, sha256(zeros).
        let cid_bytes = make_cidv1_dagpb([0u8; 32]);
        let hdr = super::build_dagcbor_header(&cid_bytes);

        // Expected byte layout:
        // A2                     map 2
        //   65 r o o t s         text "roots" (len 5)
        //   81                   array 1
        //     D8 2A              tag 42
        //     58 <len> 00 ..cid  byte-string(len) of (0x00 || cid)
        //   67 v e r s i o n     text "version" (len 7)
        //   01                   uint 1

        let mut expected = vec![0xA2];
        expected.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
        expected.push(0x81);
        expected.push(0xD8);
        expected.push(0x2A);

        let bytestring_len = 1 + cid_bytes.len();
        assert!(bytestring_len <= u8::MAX as usize);
        expected.push(0x58);
        expected.push(bytestring_len as u8);
        expected.push(0x00);
        expected.extend_from_slice(&cid_bytes);

        expected.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
        expected.push(0x01);

        assert_eq!(hdr, expected);
    }

    #[test]
    fn dagcbor_header_small_cid_uses_short_bytestring() {
        // 34-byte payload (CIDv0-sized). bytestring_len = 35, uses 0x58 branch.
        let cid_bytes = vec![0u8; 34];
        let hdr = super::build_dagcbor_header(&cid_bytes);
        let tag_idx = hdr.iter().position(|&b| b == 0x2A).unwrap();
        assert_eq!(
            hdr[tag_idx + 1],
            0x58,
            "bytestring should use 0x58 form for len 35"
        );
        assert_eq!(hdr[tag_idx + 2], 35);
    }

    #[test]
    fn read_carv1_root_round_trip() {
        let cid_bytes = make_cidv1_dagpb([7u8; 32]);
        let header = super::build_dagcbor_header(&cid_bytes);
        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, header.len() as u64).unwrap();
        framed.extend_from_slice(&header);

        let got = super::read_carv1_root_from(&mut &framed[..]).unwrap();
        let expected_cid_str = cid::Cid::try_from(&cid_bytes[..]).unwrap().to_string();
        assert_eq!(got, expected_cid_str);
    }

    #[test]
    fn read_carv1_root_truncated_varint() {
        let bytes = [0x80u8]; // continuation bit set, no follow-up
        let err = super::read_carv1_root_from(&mut &bytes[..]).unwrap_err();
        assert!(matches!(err, super::InvalidCarFile::MalformedVarint));
    }

    #[test]
    fn read_carv1_root_oversized_header_declared() {
        let mut bytes = Vec::new();
        super::write_uvarint(&mut bytes, (super::MAX_CAR_HEADER_BYTES + 1) as u64).unwrap();
        let err = super::read_carv1_root_from(&mut &bytes[..]).unwrap_err();
        assert!(matches!(err, super::InvalidCarFile::HeaderTooLarge));
    }

    #[test]
    fn read_carv1_root_truncated_body() {
        let mut bytes = Vec::new();
        super::write_uvarint(&mut bytes, 40).unwrap();
        bytes.extend_from_slice(&[0u8; 20]); // claim 40, give 20
        let err = super::read_carv1_root_from(&mut &bytes[..]).unwrap_err();
        assert!(matches!(err, super::InvalidCarFile::TruncatedHeader));
    }

    #[test]
    fn read_carv1_root_version_2_rejected() {
        let cid_bytes = make_cidv1_dagpb([1u8; 32]);
        let mut hdr = vec![0xA2];
        hdr.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
        hdr.push(0x81);
        hdr.extend_from_slice(&[0xD8, 0x2A]);
        let bs_len = 1 + cid_bytes.len();
        hdr.push(0x58);
        hdr.push(bs_len as u8);
        hdr.push(0x00);
        hdr.extend_from_slice(&cid_bytes);
        hdr.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
        hdr.push(0x02); // version 2

        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, hdr.len() as u64).unwrap();
        framed.extend_from_slice(&hdr);

        let err = super::read_carv1_root_from(&mut &framed[..]).unwrap_err();
        assert!(matches!(
            err,
            super::InvalidCarFile::UnsupportedVersion { got: 2 }
        ));
    }

    #[test]
    fn read_carv1_root_zero_roots_rejected() {
        let mut hdr = vec![0xA2];
        hdr.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
        hdr.push(0x80); // empty array
        hdr.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
        hdr.push(0x01);

        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, hdr.len() as u64).unwrap();
        framed.extend_from_slice(&hdr);

        let err = super::read_carv1_root_from(&mut &framed[..]).unwrap_err();
        assert!(matches!(
            err,
            super::InvalidCarFile::BadRootCount { got: 0 }
        ));
    }

    #[test]
    fn read_carv1_root_two_roots_rejected() {
        let cid_bytes = make_cidv1_dagpb([2u8; 32]);
        let mut hdr = vec![0xA2];
        hdr.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
        hdr.push(0x82); // array of 2
        for _ in 0..2 {
            hdr.extend_from_slice(&[0xD8, 0x2A]);
            let bs_len = 1 + cid_bytes.len();
            hdr.push(0x58);
            hdr.push(bs_len as u8);
            hdr.push(0x00);
            hdr.extend_from_slice(&cid_bytes);
        }
        hdr.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
        hdr.push(0x01);

        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, hdr.len() as u64).unwrap();
        framed.extend_from_slice(&hdr);

        let err = super::read_carv1_root_from(&mut &framed[..]).unwrap_err();
        assert!(matches!(
            err,
            super::InvalidCarFile::BadRootCount { got: 2 }
        ));
    }

    #[test]
    fn read_carv1_root_malformed_cbor_rejected() {
        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, 4).unwrap();
        framed.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);
        let err = super::read_carv1_root_from(&mut &framed[..]).unwrap_err();
        assert!(matches!(err, super::InvalidCarFile::MalformedHeader));
    }

    #[test]
    fn read_carv1_root_overlong_varint_rejected() {
        // 9 continuation bytes + a 10th byte with payload=2 at shift=63 -> overflow.
        // Without the shift==63 guard, this would silently truncate (release) or
        // panic (debug); the guard maps it to MalformedVarint.
        let bytes = [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02];
        let err = super::read_carv1_root_from(&mut &bytes[..]).unwrap_err();
        assert!(matches!(err, super::InvalidCarFile::MalformedVarint));
    }

    #[test]
    fn read_carv1_root_version_first_ordering_accepted() {
        // Craft a header with "version" first, then "roots". DAG-CBOR canonical
        // ordering puts "roots" first (shorter), but our parser is defensively
        // order-insensitive; this test pins that.
        let cid_bytes = make_cidv1_dagpb([9u8; 32]);
        let mut hdr = vec![0xA2];
        // version: 1
        hdr.extend_from_slice(&[0x67, b'v', b'e', b'r', b's', b'i', b'o', b'n']);
        hdr.push(0x01);
        // roots: [<cid>]
        hdr.extend_from_slice(&[0x65, b'r', b'o', b'o', b't', b's']);
        hdr.push(0x81);
        hdr.extend_from_slice(&[0xD8, 0x2A]);
        let bs_len = 1 + cid_bytes.len();
        hdr.push(0x58);
        hdr.push(bs_len as u8);
        hdr.push(0x00);
        hdr.extend_from_slice(&cid_bytes);

        let mut framed = Vec::new();
        super::write_uvarint(&mut framed, hdr.len() as u64).unwrap();
        framed.extend_from_slice(&hdr);

        let got = super::read_carv1_root_from(&mut &framed[..]).unwrap();
        let expected = cid::Cid::try_from(&cid_bytes[..]).unwrap().to_string();
        assert_eq!(got, expected);
    }

    #[test]
    fn write_carv1_header_emits_varint_prefix_and_canonical_header() {
        let cid_bytes = make_cidv1_dagpb([3u8; 32]);
        let mut out = Vec::new();
        super::write_carv1_header(&mut out, &cid_bytes).unwrap();

        // Round-trip: a reader should recover the same CID.
        let cid_str = super::read_carv1_root_from(&mut &out[..]).unwrap();
        let expected = cid::Cid::try_from(&cid_bytes[..]).unwrap().to_string();
        assert_eq!(cid_str, expected);
    }

    #[test]
    fn write_block_frame_layout() {
        let mut out = Vec::new();
        super::write_block_frame(&mut out, b"abc", b"hello").unwrap();
        // len = 3 + 5 = 8 -> varint 0x08
        assert_eq!(out, b"\x08abchello");
    }

    #[test]
    fn write_block_frame_large_payload_uses_multi_byte_varint() {
        let cid = vec![0xAAu8; 36];
        let block = vec![0xBBu8; 130]; // total 166 -> varint 0xA6 0x01
        let mut out = Vec::new();
        super::write_block_frame(&mut out, &cid, &block).unwrap();
        assert_eq!(&out[..2], &[0xA6, 0x01]);
        assert_eq!(&out[2..2 + cid.len()], &cid[..]);
        assert_eq!(&out[2 + cid.len()..], &block[..]);
    }
}
