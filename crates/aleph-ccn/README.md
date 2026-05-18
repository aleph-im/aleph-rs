# aleph-ccn

A faithful Rust port of [pyaleph](https://github.com/aleph-im/pyaleph) — the production [Aleph.im](https://aleph.im) Core Channel Node.

`aleph-ccn` is meant to be a drop-in replacement for the Python CCN: same database schema, same `/api/v0` + `/api/v1` HTTP surface, same WebSocket subscriptions, same message-processing semantics, same multi-chain support. If you operate an Aleph CCN today on pyaleph, `aleph-ccn` is built to run in its place.

For local testing / Anvil-style dev nodes use [Heph](../heph) instead — Heph is single-binary, SQLite-backed, and skips P2P/IPFS/chain-sync.

## Features

- **All 6 message types**: POST, AGGREGATE, STORE, PROGRAM, INSTANCE, FORGET, with full amend / forget / replace semantics.
- **Multi-chain signature verification**: Ethereum/EVM (secp256k1, EIP-191), Solana (Ed25519), Substrate (sr25519/ed25519, multi-byte SS58 prefixes), Tezos, Cosmos, NULS2, Avalanche.
- **PostgreSQL persistence** via [tokio-postgres] + [deadpool-postgres], with [refinery] migrations one-to-one with the Alembic history (59 migrations).
- **Ethereum on-chain fetcher** with batched-log retry (handles `TooManyLogsInRange` / Alchemy `-32005` / Infura range errors), automatic window resizing, and progress checkpointing in `chains_sync_status`.
- **RabbitMQ** messaging ([lapin]) for the pending-message / pending-tx pipelines.
- **Redis** ([redis] + ConnectionManager) for the cross-node node cache (API servers, public multiaddresses, hash-fetch dedup).
- **IPFS gateway client** (pinning, get/stat/add, retry-with-error-classification).
- **P2P pubsub** with Fisher-Yates randomized peer fan-out (`rand::thread_rng`) and HTTP fallback to API-server peers gated on `p2p.clients`.
- **Cost engine** with hold / stream / credit payment types, file-size pricing, instance/program/store cost comparison, and a recalculation endpoint.
- **Balance and credit lifecycle jobs** (mark-for-removal / recovery, FIFO credit consumption, expiration cache invalidation).
- **Garbage collector** for REMOVING → REMOVED transitions with per-row PostgreSQL savepoints so one bad row doesn't void the batch.
- **HTTP API** (axum 0.8) for `/api/v0` and `/api/v1`, including cursor pagination, query-string numeric coercion (Pydantic-compatible), and Python-isoformat datetime serialization (`+00:00`, microsecond precision).
- **WebSocket** subscriptions at `/api/ws0/messages` and `/api/ws0/status`.
- **Auth tokens** via `X-Auth-Token` (ECDSA secp256k1, same scheme as pyaleph).
- **Transactional message processing** — each pending row is committed atomically; a failed handler rolls back partial writes (mirrors pyaleph's `with session.begin():` semantics).

## Build

```sh
cargo build --release -p aleph-ccn
```

Or from source:

```sh
git clone https://github.com/aleph-im/aleph-rs && cd aleph-rs
cargo install --path crates/aleph-ccn
```

## Quick start

You need PostgreSQL, Redis, RabbitMQ, and (optionally) an IPFS daemon reachable. A YAML config file points at them.

```sh
# 1. Generate a node key
aleph-ccn gen-keys --key-dir ./keys

# 2. Apply database migrations
aleph-ccn migrate --config ./config.yaml

# 3. Run the node
aleph-ccn --config ./config.yaml --verbose
```

Minimal `config.yaml`:

```yaml
postgres:
  host: localhost
  port: 5432
  user: aleph
  password: aleph
  database: aleph

redis:
  url: redis://localhost:6379

rabbitmq:
  url: amqp://guest:guest@localhost:5672/

ipfs:
  enabled: true
  host: localhost
  port: 5001

p2p:
  http_port: 4024
  clients:
    - http
```

Run `aleph-ccn print-config` to dump the effective configuration after env-var and CLI overrides.

## CLI

```
aleph-ccn [OPTIONS] [COMMAND]

Commands:
  run            Run the full Core Channel Node (default)
  migrate        Apply database migrations and exit
  gen-keys       Generate a node key and save it under --key-dir
  print-config   Print effective configuration as YAML

Options:
  -c, --config <PATH>     YAML config (env: ALEPH_CONFIG)
  -p, --port <PORT>       HTTP API bind port (overrides config)
  -b, --bind <HOST>       HTTP API bind host (overrides config)
  -v, --verbose           INFO logging
      --very-verbose      DEBUG logging
      --no-commit         Don't persist newly received messages
      --no-jobs           API-only mode (skip background jobs)
      --disable-sentry    Disable Sentry even if DSN configured
```

## Compatibility with pyaleph

`aleph-ccn` reads the same PostgreSQL schema and database — you can point it at an existing pyaleph node's database after running `aleph-ccn migrate` (which applies migrations idempotently on top of Alembic-managed schemas). RabbitMQ exchanges and queues are declared with `durable: false` to match aio_pika defaults so co-deployment with a pyaleph node is safe.

Behavior verified equivalent to pyaleph for: message ingestion + retry, hold/stream/credit cost computation, balance/credit jobs, garbage collection, signature verification, cursor pagination semantics, WebSocket history, aggregate-element ordering, post amend resolution (with deterministic `item_hash` tiebreakers), and storage_add_file auth + balance gating.

## Testing

```sh
cargo test -p aleph-ccn --lib              # 490 unit tests
cargo test -p aleph-ccn --tests            # integration tests (embedded Postgres)
```

Integration tests use [postgresql_embedded] (zonky binaries) so docker is not required.

## API

`aleph-ccn` implements the full Aleph CCN HTTP API. See [docs.aleph.im](https://docs.aleph.im) for endpoint reference. Key paths:

| Endpoint | Description |
|---|---|
| `POST /api/v0/ingestion/pubsub/pub` | Submit a message (pubsub) |
| `POST /api/v0/messages` | Submit + wait for processing |
| `GET  /api/v0/messages.json` | List messages with filters and cursor pagination |
| `GET  /api/v0/messages/{hash}` | Get a message by hash |
| `GET  /api/v0/aggregates/{address}.json` | Aggregates for an address |
| `GET  /api/v0/posts.json` | Query posts (with amend chain) |
| `GET  /api/v0/price/{hash}` | Message price recalculation |
| `POST /api/v0/storage/add_file` | Upload a file (auth + balance gated) |
| `GET  /api/v0/storage/raw/{hash}` | Download a file |
| `WS   /api/ws0/messages` | Real-time message stream |
| `WS   /api/ws0/status` | Status updates |

## License

MIT — same as the rest of the workspace.

[tokio-postgres]: https://crates.io/crates/tokio-postgres
[deadpool-postgres]: https://crates.io/crates/deadpool-postgres
[refinery]: https://crates.io/crates/refinery
[lapin]: https://crates.io/crates/lapin
[redis]: https://crates.io/crates/redis
[postgresql_embedded]: https://crates.io/crates/postgresql_embedded
