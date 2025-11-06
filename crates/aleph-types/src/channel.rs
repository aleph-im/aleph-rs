use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Channel(String);

impl From<String> for Channel {
    fn from(value: String) -> Self {
        Self(value)
    }
}
