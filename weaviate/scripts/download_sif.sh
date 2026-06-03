#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WEAVIATE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VERSION="${1:-1.36.0}"

if (( $# > 1 )); then
    echo "Usage: $0 [VERSION]" >&2
    echo "Example: $0 1.36.0" >&2
    exit 1
fi

if ! command -v apptainer >/dev/null 2>&1; then
    echo "Error: apptainer is required to download the Weaviate SIF." >&2
    echo "Load it first, for example: module load apptainer" >&2
    exit 1
fi

SAFE_VERSION="${VERSION//[^A-Za-z0-9_.-]/_}"
SIF_NAME="${WEAVIATE_SIF_NAME:-weaviate_${SAFE_VERSION}.sif}"
IMAGE_URI="${WEAVIATE_IMAGE_URI:-docker://semitechnologies/weaviate:$VERSION}"
SIF_PATH="$SIF_NAME"
TMP_PATH="$SIF_PATH.tmp"


echo "Downloading Weaviate image:"
echo "  source: $IMAGE_URI"
echo "  target: $SIF_PATH"

rm -f "$TMP_PATH"
apptainer pull --force "$TMP_PATH" "$IMAGE_URI"
mv "$TMP_PATH" "$SIF_PATH"

echo "Weaviate SIF is ready at: $SIF_PATH"
