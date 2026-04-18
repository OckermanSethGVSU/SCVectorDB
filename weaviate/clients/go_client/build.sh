#!/bin/bash
# Build Weaviate Go client binaries.
#
# Every *.go in this directory declares its own `package main`, so each binary
# is built from exactly one source file:
#     go build -o <name> <name>.go
#
# Usage:
#   ./build.sh                            # build every binary in this directory
#   ./build.sh insert_streaming           # build a specific binary
#   ./build.sh insert_streaming query     # build several

set -euo pipefail

cd "$(dirname "$0")"

if [[ $# -gt 0 ]]; then
    BINS=("$@")
else
    BINS=()
    for src in *.go; do
        BINS+=("${src%.go}")
    done
fi

for bin in "${BINS[@]}"; do
    src="${bin}.go"
    if [[ ! -f "$src" ]]; then
        echo "Error: source not found: $src" >&2
        exit 1
    fi
    echo "==> go build -o $bin $src"
    go build -o "$bin" "$src"
done
