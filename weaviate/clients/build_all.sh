#!/bin/bash
# Build every Weaviate Go client under weaviate/clients/*/.
# Each subdirectory is expected to expose a build.sh that produces its binary
# in place (e.g. clients/insert_pes2o_streaming/insert_pes2o_streaming).

set -euo pipefail

CLIENTS_DIR="$(cd "$(dirname "$0")" && pwd)"

shopt -s nullglob
for build in "$CLIENTS_DIR"/*/build.sh; do
    dir="$(dirname "$build")"
    echo "==> building $(basename "$dir")"
    (cd "$dir" && ./build.sh)
done
shopt -u nullglob
