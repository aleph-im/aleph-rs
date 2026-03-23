#!/bin/bash
set -euo pipefail

# Test the Homebrew formula locally on macOS.
#
# Prerequisites: brew, cargo (Rust toolchain)
#
# What it does:
#   1. Builds a release binary with cargo
#   2. Detects host architecture (arm64 or x86_64)
#   3. Generates a formula pointing at the local binary
#   4. Installs via brew, verifies, and uninstalls
#
# Note: Only tests the host architecture. Cross-arch testing
# requires a different machine.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

cleanup() {
    echo "Cleaning up..."
    brew uninstall --formula aleph-cli 2>/dev/null || true
    [ -n "${WORK_DIR:-}" ] && rm -rf "$WORK_DIR"
}
trap cleanup EXIT

# Check prerequisites
if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Error: This script must be run on macOS."
    exit 1
fi

for cmd in brew cargo shasum; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: $cmd is not installed."
        exit 1
    fi
done

# --- Step 1: Build release binary ---
echo "==> Building release binary..."
cd "$REPO_ROOT"
cargo build --release --package aleph-cli

BINARY="$REPO_ROOT/target/release/aleph"
if [ ! -f "$BINARY" ]; then
    echo "Error: Binary not built at $BINARY"
    exit 1
fi
echo "    Built: $BINARY"

# --- Step 2: Detect architecture ---
ARCH="$(uname -m)"
case "$ARCH" in
    arm64)
        ASSET_NAME="aleph-cli-macos-aarch64"
        BLOCK_TYPE="on_arm"
        ;;
    x86_64)
        ASSET_NAME="aleph-cli-macos-x86_64"
        BLOCK_TYPE="on_intel"
        ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        exit 1
        ;;
esac
echo "    Architecture: $ARCH ($BLOCK_TYPE)"

# --- Step 3: Set up temp workspace ---
WORK_DIR=$(mktemp -d)
echo "==> Working in $WORK_DIR"

# Copy and rename binary to match the expected asset name
cp "$BINARY" "$WORK_DIR/$ASSET_NAME"
SHA256=$(shasum -a 256 "$WORK_DIR/$ASSET_NAME" | cut -d' ' -f1)
echo "    SHA256: $SHA256"

# --- Step 4: Generate formula ---
echo "==> Generating formula..."
VERSION=$(grep '^version = ' "$REPO_ROOT/crates/aleph-cli/Cargo.toml" | head -1 | sed 's/version = "\(.*\)"/\1/')
echo "    Version: $VERSION"

cat > "$WORK_DIR/aleph-cli.rb" <<EOF
class AlephCli < Formula
  desc "Minimal CLI for Aleph Cloud"
  homepage "https://github.com/aleph-im/aleph-rs"
  version "$VERSION"
  license "MIT"

  $BLOCK_TYPE do
    url "file://$WORK_DIR/$ASSET_NAME"
    sha256 "$SHA256"
  end

  def install
    bin.install "$ASSET_NAME" => "aleph"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/aleph --version")
  end
end
EOF

echo "    Formula:"
cat "$WORK_DIR/aleph-cli.rb" | sed 's/^/      /'

# --- Step 5: Install and verify ---
echo "==> Installing via brew..."
brew install --formula "$WORK_DIR/aleph-cli.rb"

echo "==> Verifying installation..."
INSTALLED_PATH="$(which aleph)"
echo "    Installed at: $INSTALLED_PATH"

aleph --help | head -5
aleph --version

echo "==> Running brew test..."
brew test aleph-cli

echo ""
echo "=== ALL TESTS PASSED ==="
