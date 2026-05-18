//! Channel type alias.
//!
//! Mirrors `src/aleph/types/channel.py`. Python uses `NewType("Channel", str)`;
//! the canonical Rust analogue lives in `aleph_types::channel::Channel`.
//! This module re-exports it so existing import paths keep working.

pub use aleph_types::channel::Channel;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_roundtrip() {
        let c: Channel = "TEST-CHANNEL".to_string().into();
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(json, "\"TEST-CHANNEL\"");
        let back: Channel = serde_json::from_str(&json).unwrap();
        assert_eq!(back, c);
    }
}
