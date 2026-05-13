//! CARv1 framing for IPFS directory uploads.
//!
//! Reference: https://ipld.io/specs/transport/car/carv1/
//!
//! Writer-only today; a reader is added in a later task so heph can
//! validate uploaded CARs in its stub `add_car` handler.

use std::io::{self, Write};

/// Upper bound on the declared CARv1 header size. A single-root header is
/// ~40 bytes; this cap exists to bound allocations from a malicious varint.
#[allow(dead_code)]
pub(crate) const MAX_CAR_HEADER_BYTES: usize = 8 * 1024;

/// Write an unsigned LEB128 varint.
#[allow(dead_code)]
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
#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[cfg(test)]
mod tests {
    use super::*;
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

    use cid::Cid as RawCid;
    use multihash::Multihash;

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
        // CIDv0 = 34 bytes (bare multihash). bytestring_len = 35.
        // 35 > 23 -> uses 0x58 prefix.
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
}
