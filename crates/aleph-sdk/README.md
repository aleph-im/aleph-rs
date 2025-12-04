# aleph-sdk

A Rust SDK to interact with Aleph Cloud.

## Overview

The Aleph Cloud SDK provides a Rust async API to interact with Aleph Cloud.
This SDK is currently in development and only supports a minimal set of features:

* Message listing and filtering
* Querying individual messages by item hash.

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
aleph-sdk = { git = "https://github.com/aleph-im/aleph-rs" }
aleph-types = { git = "https://github.com/aleph-im/aleph-rs" }
```

## Examples

### Fetch a single message by item hash

```rust
use aleph_sdk::AlephClient;
use aleph_types::message::Post;
use aleph_types::item_hash;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = AlephClient::new("https://api2.aleph.im")?;
    
    // In this example, we fetch a message from the network.
    let item_hash = item_hash!("f3862cf9d3ad73a9e82b1c56fed12627ec51c6d2a1e3189ab3ef289642711b3e");
    let message = client.get_message(item_hash).await?;
    
    Ok(())
}
```

### Filter messages by sender

```rust
#[tokio::main]
use aleph_sdk::client::{AlephClient, MessageFilter};
use aleph_types::item_hash;

async fn main() -> anyhow::Result<()> {
    let client = AlephClient::new("https://api2.aleph.im")?;
    
    let address = address!("0x1234567890123456789012345678901234567890");;
    let message_filter = MessageFilter {address: Some(address), ..Default::default()};
    
    // Fetch all messages sent by the given address.
    let messages = client.get_messages(message_filter).await?;
    
    Ok(())
}
```
```