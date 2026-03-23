#!/bin/bash
set -euo pipefail

# Aleph CLI — APT repository setup
# Usage: curl -fsSL https://apt.aleph.im/install.sh | sudo bash

KEYRING_PATH="/usr/share/keyrings/aleph.gpg"
SOURCES_PATH="/etc/apt/sources.list.d/aleph.sources"

echo "Adding Aleph Cloud APT repository..."

# Download and install the signing key
curl -fsSL https://apt.aleph.im/gpg.key | gpg --dearmor -o "$KEYRING_PATH"

# Add the repository (deb822 format, GitHub Pages as fallback)
cat > "$SOURCES_PATH" <<EOF
Types: deb
URIs: https://apt.aleph.im https://aleph-im.github.io/apt
Suites: stable
Components: main
Signed-By: $KEYRING_PATH
EOF

# Update and install
apt-get update -o Dir::Etc::sourcelist="$SOURCES_PATH" -o Dir::Etc::sourceparts="-"
apt-get install -y aleph-cli

echo "Aleph CLI installed successfully. Run 'aleph --help' to get started."
