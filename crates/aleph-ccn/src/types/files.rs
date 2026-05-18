//! File-related types: file tags and file types.
//!
//! Mirrors `src/aleph/types/files.py`.

use serde::{Deserialize, Serialize};

/// A file tag. Python uses `NewType("FileTag", str)`; we use a thin newtype.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FileTag(pub String);

impl FileTag {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for FileTag {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for FileTag {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl std::fmt::Display for FileTag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Whether a stored object is a single file or a directory tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FileType {
    #[serde(rename = "file")]
    File,
    #[serde(rename = "dir")]
    Directory,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_tag_roundtrip() {
        let tag = FileTag::from("my-tag");
        let json = serde_json::to_string(&tag).unwrap();
        assert_eq!(json, "\"my-tag\"");
        let back: FileTag = serde_json::from_str(&json).unwrap();
        assert_eq!(back, tag);
        assert_eq!(tag.as_str(), "my-tag");
        assert_eq!(tag.to_string(), "my-tag");
    }

    #[test]
    fn file_type_roundtrip() {
        assert_eq!(serde_json::to_string(&FileType::File).unwrap(), "\"file\"");
        assert_eq!(
            serde_json::to_string(&FileType::Directory).unwrap(),
            "\"dir\""
        );
        let parsed: FileType = serde_json::from_str("\"file\"").unwrap();
        assert_eq!(parsed, FileType::File);
        let parsed: FileType = serde_json::from_str("\"dir\"").unwrap();
        assert_eq!(parsed, FileType::Directory);
    }
}
