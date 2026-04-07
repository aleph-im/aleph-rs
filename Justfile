# Extract heph version from Cargo.toml
heph_version := `grep '^version = ' crates/heph/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'`
heph_image := "alephim/heph"

# Regenerate prost code from .proto files (requires protoc)
generate-proto:
    ./scripts/generate-proto.sh

# Run clippy on all targets
check-typing:
    cargo clippy --all-targets --all-features -- -D warnings

# Check documentation builds without warnings
check-doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

# Format all code
format:
    cargo fmt --all

# Build heph and start it locally for integration tests
setup-dev-env:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f /tmp/heph.pid ] && kill -0 "$(cat /tmp/heph.pid)" 2>/dev/null; then
        echo "heph is already running (PID $(cat /tmp/heph.pid))"
        exit 0
    fi
    echo "Building heph..."
    cargo build --release -p heph
    echo "Starting heph..."
    ./target/release/heph > /tmp/heph.log 2>&1 &
    echo $! > /tmp/heph.pid
    for i in $(seq 1 30); do
        if curl -sf http://127.0.0.1:4024/api/v0/version > /dev/null 2>&1; then
            echo "heph is ready (PID $(cat /tmp/heph.pid))"
            exit 0
        fi
        echo "Waiting for heph... ($i/30)"
        sleep 1
    done
    echo "heph did not start in time"
    cat /tmp/heph.log
    just stop-dev-env
    exit 1

# Stop local heph
stop-dev-env:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -f /tmp/heph.pid ]; then
        PID=$(cat /tmp/heph.pid)
        if kill -0 "$PID" 2>/dev/null; then
            kill "$PID"
            echo "Stopped heph (PID $PID)"
        else
            echo "heph process (PID $PID) was not running"
        fi
        rm -f /tmp/heph.pid
    else
        echo "No heph PID file found"
    fi

# Run unit tests
test:
    cargo test --verbose --all-features

# Run integration tests (starts heph automatically)
test-integration: setup-dev-env
    ALEPH_TEST_CCN_URL=http://127.0.0.1:4024 cargo test --verbose --all-features --test heph_integration -p aleph-sdk -- --include-ignored

# Run all tests (unit + integration)
test-all: test test-integration

# Generate code coverage report (installs cargo-tarpaulin if needed)
coverage:
    cargo install cargo-tarpaulin
    cargo tarpaulin --verbose --all-features --workspace --timeout 120 --out xml

# Build Docker image for heph
build-heph-image:
    docker build -t {{heph_image}}:{{heph_version}} -f crates/heph/Dockerfile .

# Publish heph Docker image to Docker Hub
publish-heph-image: build-heph-image
    docker push {{heph_image}}:{{heph_version}}
