#!/bin/bash
# Build every Weaviate Go client binary. Delegates to clients/go_client/build.sh
# which builds one binary per *.go file.

set -euo pipefail

CLIENTS_DIR="$(cd "$(dirname "$0")" && pwd)"

if [[ -x "$CLIENTS_DIR/go_client/build.sh" ]]; then
    "$CLIENTS_DIR/go_client/build.sh" "$@"
else
    echo "Error: $CLIENTS_DIR/go_client/build.sh is missing or not executable." >&2
    echo "Copy the Weaviate go_client source into $CLIENTS_DIR/go_client/ first." >&2
    exit 1
fi
