# aleph-rs

[![CI](https://github.com/aleph-im/aleph-rs/workflows/CI/badge.svg)](https://github.com/aleph-im/aleph-rs/actions)

Rust tools for the Aleph Cloud protocol.
This repository is a mono-repo for everything related to Aleph Cloud written in Rust.

## Crates

- **[aleph-types](crates/aleph-types)** - Core type definitions (chains, messages, signature verification)
- **[aleph-sdk](crates/aleph-sdk)** - SDK for interacting with Aleph Cloud nodes
- **[aleph-cli](crates/aleph-cli)** - Command-line interface built on top of the SDK

## Features

- **Type-safe** - Strongly typed Rust implementation of the Aleph Cloud protocol
- **Async** - Built on Tokio for efficient async operations
- **Signature verification** - Verify message signatures for EVM chains (Ethereum, Arbitrum, Base, ...) and SVM chains (Solana, Eclipse)
- **Message integrity** - Verify item hashes and content integrity, with parallel verification and per-client concurrency control
- **File storage** - Download files with optional integrity verification, stream to disk or memory
- **Real-time** - Subscribe to messages via WebSocket with automatic reconnection
- **Resilient** - HTTP retry with exponential backoff, configurable retry policy
- **Cross-platform** - Tested on Linux, macOS, and Windows
- **Modular** - Separate crates for types, SDK, and CLI

## Quick Start

### Using the SDK

Add the following to your `Cargo.toml`:

```toml
[dependencies]
aleph-sdk = { git = "https://github.com/aleph-im/aleph-rs" }
aleph-types = { git = "https://github.com/aleph-im/aleph-rs" }
```

Basic usage example:

```rust
use aleph_sdk::client::{AlephClient, AlephMessageClient};
use aleph_types::item_hash;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = AlephClient::new(Url::parse("https://api2.aleph.im")?);

    // Fetch a message from the network.
    let item_hash = item_hash!("f3862cf9d3ad73a9e82b1c56fed12627ec51c6d2a1e3189ab3ef289642711b3e");
    let message = client.get_message(&item_hash).await?;

    Ok(())
}
```

### Feature Flags

`aleph-types` provides optional signature verification behind feature flags:

| Feature | Description | Default |
|---------|-------------|---------|
| `signature` | Enables both `signature-evm` and `signature-sol` | Yes |
| `signature-evm` | Ethereum/EVM signature verification (secp256k1, EIP-191) | Yes |
| `signature-sol` | Solana/SVM signature verification (Ed25519) | Yes |

To disable signature verification (e.g. to reduce dependencies):

```toml
aleph-types = { git = "https://github.com/aleph-im/aleph-rs", default-features = false }
```

### CLI

The `aleph` CLI supports fetching, listing, and syncing messages between nodes:

```sh
# Fetch a message by item hash
aleph message get <item_hash>

# List messages with filters
aleph message list --message-types post --chains ETH

# Sync messages between two nodes
aleph message sync --source <url> --target <url>
```