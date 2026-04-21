#!/bin/bash

set -u

MISSING_ONLY=false
USE_PERF="${USE_PERF:-false}"   # default false unless exported

if [[ "${1:-}" == "--missing-only" ]]; then
    MISSING_ONLY=true
fi

required_paths=(
    "sifs/milvus.sif"
    "sifs/etcd_v3.5.18.sif"
    "sifs/minio.sif"
    "runtime/cluster/execute.sh"
    "runtime/cluster/launch_etcd.sh"
    "runtime/cluster/launch_minio.sh"
    "runtime/cluster/launch_milvus_part.sh"
    "runtime/cluster/standaloneLaunch.sh"
    "runtime/configs/unified_milvus.yaml"
    "scripts/net_mapping.py"
    "scripts/profile.py"
    "scripts/poll.py"
    "scripts/setup_collection.py"
    "scripts/multi_client_summary.py"
    "scripts/replace_unified.py"
    "main.sh"
    "clients/build.sh"
    "clients/batch_client/batch_client"
    "clients/batch_client/main.go"
    "clients/mixed/mixed"
    "clients/mixed/main.go"
    "utils/status.py"
    "perfDir/"
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

# -------------------------------
# Perf directory validation logic
# -------------------------------

PERF_WARNING=false

if [[ -d "perfDir/" ]]; then
    if [[ ! -x "perfDir/perf" ]]; then
        PERF_WARNING=true
        if [[ "$USE_PERF" == "true" ]]; then
            missing+=("perfDir/perf (required because USE_PERF=true)")
        fi
    fi
fi

# -------------------------------
# Output
# -------------------------------

if [[ "$MISSING_ONLY" == "false" ]]; then
    echo "=== Milvus dependency check ==="
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

if [[ "$PERF_WARNING" == "true" && "$USE_PERF" == "false" ]]; then
    echo ""
    echo "⚠️  Warning: perfDir/ exists but no compatible 'perf' binary found."
    echo "    Profiling will be skipped (USE_PERF=false)."
    echo "    If you intend to profile, place a compatible 'perf' binary in perfDir/."
fi

echo "All required Milvus dependencies are present."
