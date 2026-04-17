#!/bin/bash
# Build selected Weaviate Go client binaries.
#
# Every *.go in this directory declares its own `package main` with a `main()`,
# so each binary is built from exactly one source file:
#     go build -o <name> <name>.go
#
# Usage:
#   ./build.sh                        # builds the defaults below
#   ./build.sh insert_pes2o_streaming # builds one
#   ./build.sh insert_pes2o_streaming query_scaling index_pes2o_ef64

set -euo pipefail

cd "$(dirname "$0")"

DEFAULTS=(insert_pes2o_streaming query_scaling)

if [[ $# -gt 0 ]]; then
    BINS=("$@")
else
    BINS=("${DEFAULTS[@]}")
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
