# Standing up an aleph-ccn node

This guide walks through running an `aleph-ccn` Core Channel Node from scratch:
backing services, node keys, a complete annotated `config.yaml`, migrations,
starting the node, and health verification.

Every command and config key below is verified against the code in this crate
(`src/config/settings.rs`, `src/main.rs`, `src/lib.rs`). The config schema is a
typed mirror of pyaleph's `aleph/config.py`, so an existing pyaleph
`config.yaml` is compatible.

---

## 1. Prerequisites

`aleph-ccn` needs four backing services reachable over the network:

| Service     | Purpose                                                                 | Default host:port (config) |
|-------------|-------------------------------------------------------------------------|----------------------------|
| PostgreSQL  | Primary persistence (same schema as pyaleph)                            | `postgres:5432`            |
| Redis       | Cross-node cache (API servers, public multiaddrs, hash-fetch dedup)     | `redis:6379`               |
| RabbitMQ    | Pending-message / pending-tx pipelines and p2p pub/sub                  | `rabbitmq:5672`            |
| IPFS (Kubo) | File storage gateway (pin/get/stat/add). Optional if `ipfs.enabled: false` | `ipfs:5001`             |

Tested versions:

- **PostgreSQL 15+** (16 recommended). The schema uses declarative partitioning
  and identity columns.
- **Redis 6+** / 7.
- **RabbitMQ 3.12+** with the management plugin (handy, not required).
- **IPFS Kubo 0.20+** if `ipfs.enabled` is `true`.
- **Rust 1.85+** (edition 2024) to build from source.

The default config struct uses Docker-network hostnames (`postgres`, `redis`,
`rabbitmq`, `ipfs`). When you run the node outside Docker, point these at
`localhost` (see the example config below).

---

## 2. Backing services with docker-compose

Drop this `docker-compose.yml` next to your `config.yaml`. It brings up the four
services on the ports the example config expects. The node itself is run
separately (natively or in its own container) so you can iterate on it.

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: aleph
      POSTGRES_PASSWORD: decentralize-everything
      POSTGRES_DB: aleph
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U aleph"]
      interval: 5s
      timeout: 5s
      retries: 10

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  rabbitmq:
    image: rabbitmq:3.13-management
    environment:
      RABBITMQ_DEFAULT_USER: aleph-p2p
      RABBITMQ_DEFAULT_PASS: change-me!
    ports:
      - "5672:5672"     # AMQP
      - "15672:15672"   # management UI
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10

  ipfs:
    image: ipfs/kubo:latest
    ports:
      - "4001:4001"     # swarm
      - "5001:5001"     # API (used by aleph-ccn)
      - "8080:8080"     # gateway
    volumes:
      - ipfsdata:/data/ipfs

volumes:
  pgdata:
  ipfsdata:
```

```sh
docker compose up -d
```

The RabbitMQ credentials (`aleph-p2p` / `change-me!`) and Postgres credentials
(`aleph` / `decentralize-everything`) above match the crate's built-in defaults,
so they line up with the example `config.yaml` in section 4. Change them in both
places for anything beyond local testing.

---

## 3. Build the binary

```sh
# from the workspace root
cargo build --release -p aleph-ccn
# binary at target/release/aleph-ccn
```

Or install it:

```sh
cargo install --path crates/aleph-ccn
```

The examples below use `aleph-ccn`; substitute `./target/release/aleph-ccn` if
you did not install it.

---

## 4. Generate node keys

The node identity is a 2048-bit RSA keypair (the shape py-libp2p uses). Generate
it with `gen-keys`:

```sh
aleph-ccn gen-keys --key-dir ./keys
```

This writes two files into `./keys`:

- `node-secret.pkcs8.der` — PKCS#8 DER private key (consumed by the Aleph.im p2p
  service). Keep this secret and backed up.
- `node-pub.key` — PEM-encoded public key.

Flags (from `src/main.rs`):

- `-k, --key-dir <DIR>` — output directory (default: `keys`).
- `--print-key` — also print the PKCS#8 PEM private key to stdout (for
  archiving).

The directory is created if missing. If the path exists and is not a directory,
the command fails.

---

## 5. The config file

Save the following as `config.yaml`. Only a handful of keys are strictly
required to start; everything else has a sensible default baked into the typed
config struct (`src/config/settings.rs`). The annotated example sets the
connection details so the node can reach the docker-compose services from the
host.

```yaml
# Logging level uses Python logging numeric levels:
# 10=DEBUG, 20=INFO, 30=WARNING (default), 40=ERROR.
# Note: --verbose / --very-verbose CLI flags drive tracing; this is the
# pyaleph-compatible mirror field.
logging:
  level: 20
  max_log_file_size: 50000000

aleph:
  # Pub/sub topic. Default is "ALEPH-TEST"; mainnet uses "ALEPH".
  queue_topic: ALEPH
  # Upstream multichain indexer (Ethereum/Solana messages are read through it).
  indexer_url: https://multichain.api.aleph.cloud
  jobs:
    pending_messages:
      max_retries: 10
      max_concurrency: 10
      idle_timeout: 3
    pending_txs:
      max_concurrency: 20
    max_unconfirmed_messages: 10000
    cron:
      period: 0.5

# PostgreSQL — REQUIRED. Override host/port for non-Docker setups.
postgres:
  host: localhost
  port: 5432
  database: aleph
  user: aleph
  password: decentralize-everything
  pool_size: 50
  pool_pre_ping: true
  pool_recycle: 3600

# Redis — REQUIRED. Note: host + port (there is NO `url` field).
redis:
  host: localhost
  port: 6379

# RabbitMQ — REQUIRED. Note: host/port/username/password (there is NO `url`).
rabbitmq:
  host: localhost
  port: 5672
  username: aleph-p2p
  password: change-me!
  pub_exchange: p2p-publish
  sub_exchange: p2p-subscribe
  message_exchange: aleph-messages
  pending_message_exchange: aleph-pending-messages
  pending_tx_exchange: aleph-pending-txs
  heartbeat: 600

# IPFS gateway. Set enabled: false to run without an IPFS daemon.
ipfs:
  enabled: true
  host: localhost
  port: 5001
  scheme: http

# Local file storage for STORE messages.
storage:
  folder: /var/lib/aleph
  store_files: true
  garbage_collector_period: 24
  grace_period: 24

# P2P. http_port doubles as the HTTP API bind port.
p2p:
  http_port: 4024
  clients:
    - http

# Chain connectors. Ethereum is disabled by default; enable + set api_url
# (an RPC endpoint) to sync on-chain confirmations.
ethereum:
  enabled: false
  api_url: http://127.0.0.1:8545
```

### Important schema notes (these differ from naive guesses)

- **`redis`** has `host` + `port`, NOT a `url` string.
- **`rabbitmq`** has `host` / `port` / `username` / `password`, NOT a `url`
  string. The connection is assembled from these fields.
- **`ipfs`** uses `host` / `port` / `scheme`, NOT a single URL.
- **`p2p.http_port`** (default `4024`) is the HTTP API bind port. There is no
  separate `api_port`. The bind host defaults to `0.0.0.0` and is overridden
  with `-b/--bind` or the `ALEPH_BIND_HOST` env var.
- **`storage.folder`** defaults to `/var/lib/pyaleph`; make sure the path is
  writable by the node process (the example uses `/var/lib/aleph`).

### Environment-variable overrides

Any key can be overridden via env vars with the `ALEPH` prefix and `__`
(double underscore) as the nesting separator (`src/config/mod.rs`):

```sh
export ALEPH__POSTGRES__HOST=db.internal
export ALEPH__RABBITMQ__PASSWORD=supersecret
export ALEPH__P2P__HTTP_PORT=4024
```

Dump the fully-resolved config (defaults + file + env + CLI overrides) at any
time:

```sh
aleph-ccn --config ./config.yaml print-config
```

---

## 6. Run migrations

Migrations are embedded ([refinery], raw SQL, one-to-one with the pyaleph
Alembic history) and applied idempotently. Run them once before first start and
after each upgrade:

```sh
aleph-ccn --config ./config.yaml migrate
```

This connects with the `postgres` block and applies any pending `VNNNN__*.sql`
migrations from the crate's `migrations/` directory. It is safe to point at an
existing pyaleph database — migrations layer on top of the Alembic-managed
schema.

---

## 7. Start the node

```sh
aleph-ccn --config ./config.yaml --verbose
```

`run` is the default subcommand, so no explicit subcommand is needed. On
startup the node:

1. Connects to PostgreSQL, Redis, RabbitMQ (and IPFS if enabled). **All of these
   must be reachable** or startup fails.
2. Declares the RabbitMQ exchanges/queues.
3. Binds the HTTP API on `0.0.0.0:<p2p.http_port>` (default `4024`).
4. Starts background jobs (pending-message / pending-tx pipelines, cron,
   balance/credit/GC jobs).

Useful flags (from `src/main.rs`):

| Flag                 | Effect                                                       |
|----------------------|--------------------------------------------------------------|
| `-c, --config <PATH>`| Config path (env: `ALEPH_CONFIG`).                           |
| `-p, --port <PORT>`  | Override the HTTP API port (sets `p2p.http_port`).           |
| `-b, --bind <HOST>`  | Override the HTTP API bind host (sets `ALEPH_BIND_HOST`).    |
| `-v, --verbose`      | INFO logging.                                                |
| `--very-verbose`     | DEBUG logging.                                                |
| `--no-commit`        | Don't persist newly received messages.                       |
| `--no-jobs`          | API-only mode: serve HTTP, skip all background jobs and the RabbitMQ pipeline. Useful for a read-only API replica. |
| `--disable-sentry`   | Disable Sentry even if a DSN is configured.                  |

`--no-jobs` is the only mode that does NOT require RabbitMQ at startup.

---

## 8. Verify health

The node does not expose a dedicated `/health` route; use one of these:

```sh
# Liveness: returns 200 with the body "aleph-ccn"
curl -s http://localhost:4024/

# Version (also at /api/v0/version)
curl -s http://localhost:4024/version

# Node metrics as JSON — confirms DB connectivity and message counts
curl -s http://localhost:4024/metrics.json | head

# Prometheus-format metrics
curl -s http://localhost:4024/metrics

# Public node info (libp2p multiaddress)
curl -s http://localhost:4024/api/v0/info/public.json
```

A live status feed is available over WebSocket at `/api/ws0/status`, and the
message stream at `/api/ws0/messages`.

For a container orchestrator, `GET /` is the cheapest liveness probe;
`GET /metrics.json` is a good readiness probe because it touches the database.

---

## 9. Common gotchas

- **Wrong `redis` / `rabbitmq` shape.** These take discrete `host`/`port`
  fields, not a `url`. A `url:` key is silently ignored and the node falls back
  to the default Docker hostname (`redis` / `rabbitmq`), which won't resolve on
  the host — you'll see a connection error at startup.
- **`storage.folder` not writable.** With `store_files: true` the node creates a
  filesystem storage engine at `storage.folder` on startup and errors if it
  can't. Default is `/var/lib/pyaleph`; set it to a writable path.
- **RabbitMQ required unless `--no-jobs`.** Full `run` mode declares exchanges
  and opens channels during startup. If RabbitMQ is down, startup aborts.
- **`queue_topic` mismatch.** The default is `ALEPH-TEST`. To participate in
  mainnet set `aleph.queue_topic: ALEPH` (and review `p2p.topics`).
- **Port confusion.** The HTTP API port is `p2p.http_port` (default `4024`),
  not `p2p.port` (`4025`, the libp2p TCP port).
- **Migrations vs. pyaleph numbering.** `aleph-ccn` migration numbers track the
  Alembic history (`V0057`, `V0058`, …) plus port-specific extras (`V0059`
  `fix_id_identity_columns`, `V0060` `lease_pending_txs`). `migrate` is
  idempotent — just run it after every upgrade.
- **IPFS disabled.** Setting `ipfs.enabled: false` is fine for a node that
  doesn't serve file storage, but STORE message file fetching via IPFS will be
  unavailable.

[refinery]: https://crates.io/crates/refinery
