# aleph-rs

[![CI](https://github.com/aleph-im/aleph-rs/workflows/CI/badge.svg)](https://github.com/aleph-im/aleph-rs/actions)

Rust tooling for the [Aleph Cloud](https://aleph.cloud) protocol: a CLI for end users and a typed async SDK for developers.

## Crates

- **[aleph-cli](crates/aleph-cli)**: `aleph` command-line interface
- **[aleph-sdk](crates/aleph-sdk)**: async Rust SDK
- **[aleph-types](crates/aleph-types)**: protocol types and signature verification
- **[heph](crates/heph)**: local single-binary CCN for testing (think Anvil for Aleph)

---

## CLI

### Install

```sh
# 1. Pre-built binaries (Linux/macOS/Windows)
#    https://github.com/aleph-im/aleph-rs/releases/latest

# 2. Debian / Ubuntu (APT repo, signed)
curl -fsSL https://apt.aleph.im/install.sh | sudo bash

# 3. Cargo
cargo install aleph-cli

# 4. From source
git clone https://github.com/aleph-im/aleph-rs && cd aleph-rs
cargo install --path crates/aleph-cli
```

Run `aleph completions <bash|zsh|fish|powershell>` to generate shell completions.

### Quick start

```sh
# 1. Create a local signing account (stored in your OS keychain)
aleph account create my-account
aleph account use my-account

# 2. Buy credits with ALEPH, USDC, or ETH (requires funds on Ethereum mainnet)
aleph credit buy --token usdc --amount 10

# 3. Upload a file (paid for in credits)
aleph file upload ./report.pdf
```

`aleph account balance` shows your ALEPH and credit balances. Every command supports `--json` for scripting and `--help` for full documentation, including examples.

### Commands at a glance

| Group | Purpose |
|---|---|
| `aleph account` | Local signing keys (create, import, use, remove, balance, alias) |
| `aleph credit` | Buy and transfer Aleph credits |
| `aleph file` | Upload, download, and pin files |
| `aleph instance` | Create and manage VM instances on CRNs |
| `aleph post` | Create, amend, and list posts |
| `aleph aggregate` | Create aggregate (key/value) entries |
| `aleph message` | Get, list, sync, and forget raw protocol messages |
| `aleph node` | Register, link, stake, and amend network nodes |
| `aleph authorization` | Manage delegated signing authorizations |
| `aleph config` | Networks and CCN endpoints |

Run `aleph <group> --help` for the full subcommand list, and `aleph <group> <subcommand> --help` for flags and examples.

### Configuration

The CLI ships pre-configured for `mainnet` (CCN: `https://api.aleph.im`). Add more networks or CCN endpoints as needed:

```sh
aleph config network list                       # registered networks
aleph config network add testnet …              # register a new network
aleph config network use testnet                # change the default
aleph config ccn list                           # CCN endpoints in current network
aleph config ccn add my-ccn https://…           # register a CCN endpoint
aleph config ccn use my-ccn                     # set as the default CCN
```

Per-command overrides also exist: `--network <name>` switches the active network for one call, `--ccn <name|url>` overrides the CCN endpoint, and `--account <name>` / `--private-key <hex>` override the signing identity.

### Hardware wallets

Ledger devices are supported for EVM accounts:

```sh
aleph account import my-ledger --ledger
```

Signing operations will prompt the device when required.

---

## SDK

Add to `Cargo.toml`:

```toml
[dependencies]
aleph-sdk = "0.9"
aleph-types = "0.9"
tokio = { version = "1", features = ["full"] }
```

Fetch a message:

```rust
use aleph_sdk::client::{AlephClient, AlephMessageClient};
use aleph_types::item_hash;
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = AlephClient::new(Url::parse("https://api.aleph.im")?);
    let hash = item_hash!("f3862cf9d3ad73a9e82b1c56fed12627ec51c6d2a1e3189ab3ef289642711b3e");
    let message = client.get_message(&hash).await?;
    println!("{message:?}");
    Ok(())
}
```

The SDK also covers posting, aggregates, file storage, instance management, credit transfers, and WebSocket subscriptions. See [`crates/aleph-sdk`](crates/aleph-sdk) for examples.

### Feature flags (`aleph-types`)

| Feature | Description | Default |
|---|---|---|
| `signature` | Enables both `signature-evm` and `signature-sol` | yes |
| `signature-evm` | Ethereum/EVM signatures (secp256k1, EIP-191) | yes |
| `signature-sol` | Solana/SVM signatures (Ed25519) | yes |

To trim dependencies (e.g. for a server that only verifies hashes):

```toml
aleph-types = { version = "0.9", default-features = false }
```

---

## Heph (local CCN for testing)

[Heph](crates/heph) is a single-binary local Core Channel Node, the Anvil-equivalent for Aleph: no P2P, no IPFS, no chain sync, just a fast deterministic test server backed by SQLite and a local file store. Useful for integration tests and local development against a real Aleph API surface.

```sh
cargo install heph
heph                       # listens on http://127.0.0.1:4024
```

Pre-seeded accounts and credit balances are deterministic across runs. Point the CLI or SDK at it via `--ccn http://127.0.0.1:4024`. See [`crates/heph`](crates/heph) for flags (custom port, persistent data dir, pre-seeded addresses, etc.).

---

## License

MIT.
