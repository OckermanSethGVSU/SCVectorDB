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
    "cpuMilvus"
    "milvusSetup/execute.sh"
    "milvusSetup/launch_etcd.sh"
    "milvusSetup/launch_minio.sh"
    "milvusSetup/launch_milvus_part.sh"
    "milvusSetup/standaloneLaunch.sh"
    "generalPython/net_mapping.py"
    "generalPython/replace.py"
    "generalPython/profile.py"
    "generalPython/poll.py"
    "generalPython/status.py"
    "generalPython/setup_collection.py"
    "generalPython/insert_multi_client_summary.py"
    "main.sh"
    "goCode/multiClientInsert/multiClientInsert"
    "goCode/multiClientInsert/main.go"
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