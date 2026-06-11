#!/usr/bin/env bash
# Manual: regenerate golden CAR header bytes for unit tests.
# Requires Docker. Pinned kubo version matches what ipfs.aleph.cloud
# runs as of authoring; bump both together if drift becomes a problem.
#
# Usage:
#   bash crates/aleph-cid/tests/regen-car-goldens.sh

set -euo pipefail

KUBO_TAG="ipfs/kubo:v0.30.0"
CID_VERSION=1

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/fixture"
printf 'hello' > "$work/fixture/a.txt"
printf 'world' > "$work/fixture/b.txt"

docker run --rm -v "$work:/work" "$KUBO_TAG" \
    sh -c "ipfs init >/dev/null 2>&1 && ipfs add -rQ --cid-version=$CID_VERSION /work/fixture > /work/cid.txt"
CID=$(cat "$work/cid.txt")
echo "root CID: $CID"

docker run --rm -v "$work:/work" "$KUBO_TAG" \
    sh -c "ipfs init >/dev/null 2>&1 && ipfs add -rQ --cid-version=$CID_VERSION /work/fixture >/dev/null && ipfs dag export $CID > /work/golden.car"

echo
echo "Header bytes (first 64) for fixture:"
xxd -l 64 "$work/golden.car"
echo
echo "Compare against the header structure encoded by build_carv1_header."
