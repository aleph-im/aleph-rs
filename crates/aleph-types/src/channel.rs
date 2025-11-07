use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Channel(String);

impl From<String> for Channel {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Macro for creating Channel instances from string literals.
///
/// # Example
///
/// ```
/// use aleph_types::channel;
/// let channel = channel!("MY-CHANNEL");
/// ```
#[macro_export]
macro_rules! channel {
    ($channel:expr) => {{
        $crate::channel::Channel::from($channel.to_string())
    }};
}

