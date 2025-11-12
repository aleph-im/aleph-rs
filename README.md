# aleph-rs

[![CI](https://github.com/aleph-im/aleph-rs/workflows/CI/badge.svg)](https://github.com/aleph-im/aleph-rs/actions)

Rust tools for the Aleph Cloud protocol.
This repository is meant to be a mono-repo for everything related to Aleph Cloud written in Rust.

## Overview

This repository provides three crates at the moment:

- **[aleph-types](crates/aleph-types)** - Core type definitions
- **[aleph-sdk](crates/aleph-sdk)** - Rust SDK for interacting with Aleph Cloud nodes
- **[aleph-cli](crates/aleph-cli)** - Command-line interface built on top of the SDK.

## Features

- 🦀 **Type-safe** - Strongly typed Rust implementation of the Aleph Cloud protocol
- 🔄 **Async/Await** - Built on Tokio for efficient async operations
- 🧪 **Well-tested** - Comprehensive test suite with CI/CD
- 🌐 **Cross-platform** - Tested on Linux, macOS, and Windows
- 📦 **Modular** - Separate crates for types, SDK, and CLI

## Quick Start

### Using the SDK

Add the following to your `Cargo.toml`:

```toml
[dependencies] 
aleph-sdk = { git = "[https://github.com/aleph-im/aleph-rs](https://github.com/aleph-im/aleph-rs)" } 
aleph-types = { git = "[https://github.com/aleph-im/aleph-rs](https://github.com/aleph-im/aleph-rs)" }
```

Basic usage example:

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