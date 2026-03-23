#!/bin/bash
set -euo pipefail

# Test the full APT packaging flow locally using Docker.
#
# Prerequisites: cargo-deb, reprepro, docker, python3
#
# What it does:
#   1. Builds a .deb package with cargo-deb
#   2. Creates a throwaway GPG key (isolated from your real keyring)
#   3. Sets up a local APT repository with reprepro
#   4. Serves it over HTTP on localhost
#   5. Spins up an Ubuntu container that installs aleph-cli from the repo
#   6. Cleans everything up

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WORK_DIR=""
HTTP_PID=""

cleanup() {
    echo "Cleaning up..."
    [ -n "$HTTP_PID" ] && kill "$HTTP_PID" 2>/dev/null || true
    [ -n "$WORK_DIR" ] && rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Check prerequisites
for cmd in cargo-deb reprepro docker python3 gpg; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is not installed."
        [ "$cmd" = "cargo-deb" ] && echo "  Install with: cargo install cargo-deb"
        [ "$cmd" = "reprepro" ] && echo "  Install with: sudo apt-get install reprepro"
        exit 1
    fi
done

if ! docker info &>/dev/null; then
    echo "Error: Docker daemon is not running."
    exit 1
fi

# --- Step 1: Build .deb ---
echo "==> Building .deb package..."
cd "$REPO_ROOT"
cargo deb -p aleph-cli

DEB_FILE=$(find target/debian -name "aleph-cli_*.deb" -printf '%T@ %p\n' | sort -n | tail -1 | cut -d' ' -f2)
echo "    Built: $DEB_FILE"

# --- Step 2: Create temp workspace ---
WORK_DIR=$(mktemp -d)
echo "==> Working in $WORK_DIR"

# --- Step 3: Generate throwaway GPG key ---
echo "==> Generating throwaway GPG key..."
export GNUPGHOME="$WORK_DIR/gnupg"
mkdir -p "$GNUPGHOME"
chmod 700 "$GNUPGHOME"

gpg --batch --gen-key <<EOF
Key-Type: EdDSA
Key-Curve: ed25519
Name-Real: APT Test Key
Name-Email: test@localhost
%no-protection
%commit
EOF

# --- Step 4: Build APT repository with reprepro ---
echo "==> Setting up APT repository..."
APT_REPO="$WORK_DIR/repo"
mkdir -p "$APT_REPO/conf"

cat > "$APT_REPO/conf/distributions" <<EOF
Origin: Aleph.im
Label: Aleph.im
Codename: stable
Architectures: amd64
Components: main
Description: Aleph Cloud CLI packages (test)
SignWith: yes
EOF

cat > "$APT_REPO/conf/options" <<EOF
verbose
basedir .
EOF

cd "$APT_REPO"
reprepro includedeb stable "$REPO_ROOT/$DEB_FILE"

gpg --export --armor > "$APT_REPO/gpg.key"

echo "    Repository contents:"
find "$APT_REPO/dists" -type f | head -10 | sed 's/^/      /'
echo "    Pool:"
find "$APT_REPO/pool" -type f | sed 's/^/      /'

# --- Step 5: Serve over HTTP ---
PORT=18808
echo "==> Starting HTTP server on port $PORT..."
python3 -m http.server "$PORT" --directory "$APT_REPO" &>/dev/null &
HTTP_PID=$!
sleep 1

if ! kill -0 "$HTTP_PID" 2>/dev/null; then
    echo "Error: HTTP server failed to start."
    exit 1
fi

# --- Step 6: Test in Docker ---
echo "==> Testing installation in Docker (ubuntu:24.04)..."
docker run --rm --network host ubuntu:24.04 bash -c "
    set -euo pipefail

    echo '--- Installing prerequisites ---'
    apt-get update -qq
    apt-get install -y -qq curl gnupg > /dev/null

    echo '--- Adding APT repository ---'
    curl -fsSL http://localhost:$PORT/gpg.key | gpg --dearmor -o /usr/share/keyrings/aleph.gpg

    cat > /etc/apt/sources.list.d/aleph.sources <<SOURCES
Types: deb
URIs: http://localhost:$PORT
Suites: stable
Components: main
Signed-By: /usr/share/keyrings/aleph.gpg
SOURCES

    echo '--- apt-get update ---'
    apt-get update -o Dir::Etc::sourcelist=/etc/apt/sources.list.d/aleph.sources -o Dir::Etc::sourceparts=-

    echo '--- apt-get install aleph-cli ---'
    apt-get install -y aleph-cli

    echo '--- Verifying installation ---'
    which aleph
    aleph --help | head -5
    dpkg -s aleph-cli | grep -E '^(Package|Version|Status):'

    echo ''
    echo '=== ALL TESTS PASSED ==='
"

echo ""
echo "Done. The full APT flow works correctly."
