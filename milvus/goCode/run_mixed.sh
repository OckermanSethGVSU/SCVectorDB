#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$SCRIPT_DIR/mixedrunner"
export PROXY_REGISTRY_PATH="$SCRIPT_DIR/multiClientOP/PROXY_registry.txt"

export NUM_PROXIES=1

export INSERT_DATA_FILEPATH=~/Documents/research/SCVectorDB/milvus/utils/random.npy
export INSERT_CORPUS_SIZE=1000
export INSERT_BATCH_SIZE=1
export INSERT_BALANCE_STRATEGY=NONE

export QUERY_DATA_FILEPATH=~/Documents/research/SCVectorDB/milvus/utils/random.npy
export QUERY_CORPUS_SIZE=1000
export QUERY_BATCH_SIZE=1
export QUERY_BALANCE_STRATEGY=NONE

export RESULT_PATH=./mixed_logs

cd "$RUNNER_DIR"
go run .   -insert-clients 1   -query-clients 1   -collection standalone   -vector-field vector   -id-field id   -top-k 10   -mode max   -insert-mode rate   -insert-ops-per-sec 100   -query-mode max

# go run .   -insert-clients 4   -query-clients 2   -collection standalone   -vector-field vector   -id-field id   -top-k 10   -mode max
