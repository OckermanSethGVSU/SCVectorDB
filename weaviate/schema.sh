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
register_weaviate_var "WORKERS_PER_NODE" "default" "1" "" "Worker processes launched per compute node"

# Engine/runtime selection
register_weaviate_var "USEPERF" "default" "false" "true false" "Enable perf collection"
register_weaviate_var "DEBUG" "default" "false" "true false" "Enable verbose client debug logging"
register_weaviate_var "GPU_INDEX" "default" "false" "true false" "Whether to use GPU indexing"
register_weaviate_var "ASYNC_INDEXING" "default" "true" "true false" "Enable Weaviate async indexing"
register_weaviate_var "DISABLE_LAZY_LOAD_SHARDS" "default" "true" "true false" "Disable Weaviate lazy shard loading"
register_weaviate_var "HNSW_STARTUP_WAIT_FOR_VECTOR_CACHE" "default" "true" "true false" "Wait for HNSW vector cache at startup"
register_weaviate_var "SHARD_COUNT" "default" "1" "" "Explicit shard count for collection creation; 0 lets the workflow derive or use engine defaults"
register_weaviate_var "HNSW_M" "default" "16" "" "HNSW M parameter"
register_weaviate_var "HNSW_EF_CONSTRUCTION" "default" "100" "" "HNSW efConstruction parameter"
register_weaviate_var "HNSW_DYNAMIC_THRESHOLD" "default" "" "" "Dynamic index threshold for flat-to-HNSW conversion; defaults to INSERT_CORPUS_SIZE or the row count of INSERT_DATA_FILEPATH when unset"

# Insert / index workload
register_weaviate_var "INSERT_CLIENTS_PER_WORKER" "default" "1" "" "Insert clients per worker"

# Query workload
register_weaviate_var "QUERY_TOPK" "default" "10" "" "Query top-k"
register_weaviate_var "HNSW_EF_SEARCH" "default" "64" "" "HNSW ef parameter used in collection creation for query-time search breadth"
register_weaviate_var "QUERY_CLIENTS_PER_WORKER" "default" "1" "" "Query clients per worker rank"