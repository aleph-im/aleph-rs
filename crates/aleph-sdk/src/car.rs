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
}
