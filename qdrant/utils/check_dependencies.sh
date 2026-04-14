#!/bin/bash

set -u

MISSING_ONLY=false
if [[ "${1:-}" == "--missing-only" ]]; then
    MISSING_ONLY=true
fi

required_paths=(
    "qdrant.sif"
    "runtime_state/perf"
    "runtime/cluster/launchQdrantNode.sh"
    "runtime/cluster/launch.sh"
    "scripts/gen_dirs.py"
    "scripts/mapping.py"
    "scripts/profile.py"
    "scripts/configure_collection.py"
    "scripts/insert_multi_client_summary.py"
    "scripts/build_index.py"
    "main.sh"
    "clients/upload/upload"
    "clients/upload/src/main.rs"
)

present=()
missing=()

for path in "${required_paths[@]}"; do
    if [[ -e "$path" ]]; then
        present+=("$path")
    else
        missing+=("$path")
    fi
done

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "=== Qdrant dependency check ==="
    echo "Present (${#present[@]}):"
    for path in "${present[@]}"; do
        echo "  - $path"
    done
fi

if (( ${#missing[@]} > 0 )); then
    echo "Missing (${#missing[@]}):"
    for path in "${missing[@]}"; do
        echo "  - $path"
    done
    exit 1
fi

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "Missing (0): none"
fi

echo "All required Qdrant dependencies are present."
