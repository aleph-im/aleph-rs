#!/usr/bin/env bash
#
# Regenerates the GOLDEN_* constants in tests/folder_hash.rs by running real
# kubo (Docker) over the same fixtures the tests build. Output is printed to
# stdout; copy-paste into the `// === Goldens ===` block of the test file.
#
# Requires: docker.
#
# Usage:
#   bash crates/aleph-cid/tests/regen-folder-hash-goldens.sh

set -euo pipefail

KUBO_IMAGE="${KUBO_IMAGE:-ipfs/kubo:v0.30.0}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

docker pull -q "$KUBO_IMAGE" >&2

run_kubo_add() {
    # $1 = fixture name, $2 = path on host, $3 = cid version (0 or 1)
    local name="$1"
    local hostpath="$2"
    local ver="$3"
    local extra=""
    if [[ "$ver" == "1" ]]; then
        extra="--cid-version=1 --raw-leaves"
    fi
    local cid
    # Mount the fixture at /fixture (read-only) and use /tmp/.ipfs as IPFS_PATH
    # to avoid conflicts with kubo's default IPFS_PATH=/data/.ipfs.
    cid=$(docker run --rm \
        -v "$hostpath":/fixture:ro \
        --entrypoint sh "$KUBO_IMAGE" -c \
        "IPFS_PATH=/tmp/.ipfs ipfs init --profile=test >/dev/null 2>&1; IPFS_PATH=/tmp/.ipfs ipfs add -rQ $extra /fixture")
    local upper
    upper=$(echo "$name" | tr '[:lower:]' '[:upper:]')
    echo "const GOLDEN_${upper}_V${ver}: &str = \"${cid}\";"
}

# === Fixtures (must match the Rust functions in tests/folder_hash.rs) ===

mk_single_file_small() {
    local d="$1"
    printf 'hello\n' > "$d/hello.txt"
}

mk_single_file_multi_chunk() {
    local d="$1"
    # Use `head -c 1048576 /dev/zero | tr '\0' x` — the literal character 'x'
    # (0x78).  Note: `tr '\0' '\xab'` in many shells is parsed as '\x' then
    # 'ab', i.e. the single char 'x', NOT hex 0xAB.  Using 'x' explicitly is
    # unambiguous and matches the Rust fixture (vec![b'x'; ...]).
    head -c 1048576 /dev/zero | tr '\0' x > "$d/big.bin"
}

mk_flat_dir_small() {
    local d="$1"
    for c in a b c d e f g h i j; do
        printf '%s' "$c" > "$d/$c.txt"
    done
}

mk_nested_dir() {
    local d="$1"
    printf 'top\n' > "$d/top.txt"
    mkdir -p "$d/sub/deeper"
    printf 'inner\n' > "$d/sub/inner.txt"
    printf 'leaf\n' > "$d/sub/deeper/leaf.txt"
}

mk_hamt_short_names() {
    local d="$1"
    # 6000 files * (8-char name + 36-byte CIDv1) = 264000 > 262144 (HAMT threshold)
    for i in $(seq 0 5999); do
        printf 'x' > "$(printf '%s/%08d' "$d" "$i")"
    done
}

mk_hamt_long_names() {
    local d="$1"
    local suffix
    # 920 files * (250-char name + 36-byte CIDv1) = 263120 > 262144 (HAMT threshold).
    # 246-char suffix + 4-digit prefix = 250 chars total (within Linux 255-char limit).
    suffix="$(printf 'z%.0s' {1..246})"
    for i in $(seq 0 919); do
        printf 'x' > "$(printf '%s/%04d%s' "$d" "$i" "$suffix")"
    done
}

mk_empty_directory() {
    # Empty directory: leave $1 untouched (it was created by `mkdir -p` above).
    :
}

mk_empty_file() {
    local d="$1"
    : > "$d/empty"
}

mk_utf8_names() {
    local d="$1"
    printf 'a\n' > "$d/café.txt"
    printf 'b\n' > "$d/日本.txt"
    printf 'c\n' > "$d/🚀.txt"
}

mk_threshold_below() {
    local d="$1"
    # 5957 entries with 8-char names: 5957 * (8 + 36) = 262108 < 262144 -> BasicDirectory.
    for i in $(seq 0 5956); do
        printf 'x' > "$(printf '%s/%08d' "$d" "$i")"
    done
}

mk_threshold_above() {
    local d="$1"
    # 5958 entries: 5958 * 44 = 262152 >= 262144 -> HAMTDirectory.
    for i in $(seq 0 5957); do
        printf 'x' > "$(printf '%s/%08d' "$d" "$i")"
    done
}

mk_hamt_multi_level() {
    local d="$1"
    local a b
    a="$(cat crates/aleph-cid/tests/hamt_collision_a.txt)"
    b="$(cat crates/aleph-cid/tests/hamt_collision_b.txt)"
    [[ -n "$a" && -n "$b" ]] || {
        echo "hamt_collision_*.txt are empty — discover collision pair first" >&2
        exit 1
    }
    printf 'a' > "$d/$a"
    printf 'b' > "$d/$b"
    printf 'c' > "$d/Z"
}

# === Run all fixtures ===

for name in single_file_small single_file_multi_chunk flat_dir_small nested_dir \
            hamt_short_names hamt_long_names hamt_multi_level \
            empty_directory empty_file utf8_names threshold_below threshold_above; do
    fdir="$WORK/$name"
    mkdir -p "$fdir"
    "mk_$name" "$fdir"
    run_kubo_add "$name" "$fdir" 1
done

# CIDv0 case for flat_dir_small
run_kubo_add "flat_dir_small" "$WORK/flat_dir_small" 0
