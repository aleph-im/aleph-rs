//! Level of message ``content`` detail returned by the messages API.
//!
//! Mirrors `src/aleph/types/content_format.py`.
//!
//! * `full`    - the complete content (default).
//! * `headers` - a reduced, per-type metadata subset built from
//!   denormalized columns; the content JSONB is not read.
//! * `none`    - content omitted entirely (the behaviour of the
//!   deprecated `excludeContent=true` flag).

use serde::{Deserialize, Serialize};

/// Level of message `content` detail returned by the messages API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentFormat {
    #[default]
    Full,
    Headers,
    None,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_format_roundtrip() {
        assert_eq!(
            serde_json::to_string(&ContentFormat::Full).unwrap(),
            "\"full\""
        );
        assert_eq!(
            serde_json::to_string(&ContentFormat::Headers).unwrap(),
            "\"headers\""
        );
        assert_eq!(
            serde_json::to_string(&ContentFormat::None).unwrap(),
            "\"none\""
        );
        let parsed: ContentFormat = serde_json::from_str("\"headers\"").unwrap();
        assert_eq!(parsed, ContentFormat::Headers);
    }

    #[test]
    fn content_format_invalid_rejected() {
        assert!(serde_json::from_str::<ContentFormat>("\"bogus\"").is_err());
    }

    #[test]
    fn content_format_default_is_full() {
        assert_eq!(ContentFormat::default(), ContentFormat::Full);
    }
}
