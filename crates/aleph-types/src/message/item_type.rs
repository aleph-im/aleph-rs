use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ItemType {
    Inline,
    Storage,
    Ipfs,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_type_serialization() {
        assert_eq!(serde_json::to_string(&ItemType::Inline).unwrap(), "\"inline\"");
        assert_eq!(serde_json::to_string(&ItemType::Storage).unwrap(), "\"storage\"");
        assert_eq!(serde_json::to_string(&ItemType::Ipfs).unwrap(), "\"ipfs\"");
    }

    #[test]
    fn test_item_type_deserialization() {
        assert_eq!(serde_json::from_str::<ItemType>("\"inline\"").unwrap(), ItemType::Inline);
        assert_eq!(serde_json::from_str::<ItemType>("\"storage\"").unwrap(), ItemType::Storage);
        assert_eq!(serde_json::from_str::<ItemType>("\"ipfs\"").unwrap(), ItemType::Ipfs);
    }
}
