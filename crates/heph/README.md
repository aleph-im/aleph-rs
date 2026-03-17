# Heph

<p align="center">
  <img src="heph.png" width="400" alt="Heph — the god of the forge" />
</p>

A lightweight, single-binary local [Aleph](https://aleph.im) Core Channel Node for testing. Think [Anvil](https://book.getfoundry.sh/reference/anvil/) but for Aleph: no P2P, no IPFS, no chain sync — just a fast, deterministic test server backed by SQLite and a local file store.

## Features

- All 6 message types: POST, AGGREGATE, STORE, PROGRAM, INSTANCE, FORGET
- EVM + Solana signature verification
- Delegated permissions via security aggregates
- Credit-based cost system with pre-seeded balances
- Deterministic auto-generated accounts (same across runs)
- Full `/api/v0` and `/api/v1` endpoint coverage

## Quick Start

```bash
cargo install heph

# or build from source
cargo build --release -p heph
./target/release/heph
```

```
Heph - Local Aleph CCN v0.1.0
==============================

Available Accounts
==================
(0) 0x... (1000000000 credits)
(1) 0x... (1000000000 credits)
...

Private Keys
============
(0) 0x...
(1) 0x...
...

Listening on http://127.0.0.1:4024
```

## Usage

```bash
# Custom port and host
heph --port 8080 --host 0.0.0.0

# Persistent data directory
heph --data-dir ./my-data

# Pre-seed specific accounts
heph --accounts 0xYourAddress1,0xYourAddress2 --balance 5000000000

# Debug logging
heph --log-level debug
```

## Docker

```bash
docker build -f crates/heph/Dockerfile -t heph .
docker run -p 4024:4024 heph
```

## API

Heph implements the Aleph CCN HTTP API. Key endpoints:

| Endpoint | Description |
|---|---|
| `POST /api/v0/messages` | Submit a message |
| `GET /api/v0/messages.json` | List messages (with filtering) |
| `GET /api/v0/messages/{hash}` | Get a message by hash |
| `GET /api/v0/aggregates/{address}.json` | Get aggregates for an address |
| `GET /api/v0/posts.json` | Query posts |
| `POST /api/v0/storage/add_file` | Upload a file |
| `GET /api/v0/storage/raw/{hash}` | Download a file |

See the full [Aleph API documentation](https://docs.aleph.im) for details.
