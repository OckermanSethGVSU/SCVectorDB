#!/bin/bash
#
# Weaviate schema format:
#   register_weaviate_var "NAME" "REQUIREMENT" "DEFAULT" "CHOICES" "DESCRIPTION" ["REQUIRED_IF"]
#
# REQUIREMENT:
#   required    - caller must provide a value; DEFAULT should normally be empty.
#   default     - DEFAULT is used when the caller does not override the variable.
#   conditional - variable is required only when REQUIRED_IF matches the current config.
#
# CHOICES is a space-separated allowlist. Leave it empty to allow any value.
# REQUIRED_IF currently supports one condition in the form OTHER_VAR=value1|value2.
# Every registered variable may be set to one value or a space-separated sweep list.
# The order in this file controls the order shown in `--help --engine weaviate`.

# Allocation and storage layout
register_weaviate_var "WORKERS_PER_NODE" "default" "4" "" "Worker processes launched per compute node"

# Engine/runtime selection
register_weaviate_var "USEPERF" "default" "false" "true false" "Enable perf collection"
register_weaviate_var "WEAVIATE_CLIENT_BINARY" "default" "insert_streaming" "" "Client binary copied into the run directory for insert/index/query_bs/query_core tasks"
register_weaviate_var "VECTOR_DIM" "default" "2560" "" "Vector dimension"
register_weaviate_var "DISTANCE_METRIC" "default" "COSINE" "COSINE DOT L2" "Distance metric"
register_weaviate_var "GPU_INDEX" "default" "false" "true false" "Whether to use GPU indexing"

# Insert / index workload
register_weaviate_var "INSERT_DATA_FILEPATH" "default" "/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy" "" "Insert corpus file path"
register_weaviate_var "INSERT_CORPUS_SIZE" "default" "10000000" "" "Insert/index corpus size"
register_weaviate_var "INSERT_BATCH_SIZE" "default" "2048" "" "Insert batch size"
register_weaviate_var "INSERT_CLIENTS_PER_WORKER" "default" "16" "" "Insert clients per worker"
register_weaviate_var "INSERT_BALANCE_STRATEGY" "default" "WORKER_BALANCE" "NO_BALANCE WORKER_BALANCE" "Insert balancing policy"

# Query workload
register_weaviate_var "QUERY_FILEPATH" "default" "/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy" "" "Query vector file path"
register_weaviate_var "QUERY_WORKLOAD" "default" "100000" "" "Number of queries to execute"
register_weaviate_var "QUERY_BATCH_SIZE" "default" "256" "" "Query batch size"
register_weaviate_var "QUERY_TOPK" "default" "10" "" "Query top-k"
register_weaviate_var "QUERY_EF" "default" "64" "" "Query ef parameter"
register_weaviate_var "QUERY_CLIENTS_PER_WORKER" "default" "1" "" "Query clients per worker rank"
register_weaviate_var "QUERY_CLIENT_MODE" "default" "per_worker" "fixed per_worker" "How QUERY_CLIENTS_PER_WORKER is interpreted"

# Dataset and per-task client binaries
register_weaviate_var "DATASET_LABEL" "default" "pes2o" "" "Dataset tag used in run-dir names"
register_weaviate_var "CLASS_NAME" "default" "PES2OEF64" "" "Weaviate class/collection name"
register_weaviate_var "INSERT_BIN" "default" "insert_streaming" "" "Client binary for the insert phase of query_scaling"
register_weaviate_var "QUERY_SCALING_BIN" "default" "query" "" "Client binary for the query phase of query_scaling"
