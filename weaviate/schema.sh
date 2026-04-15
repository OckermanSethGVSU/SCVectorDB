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

# Core execution mode and platform
register_weaviate_var "TASK" "required" "" "insert index query_bs query_core" "Experiment task"
register_weaviate_var "RUN_MODE" "default" "PBS" "PBS" "Run mode"
register_weaviate_var "PLATFORM" "required" "" "POLARIS AURORA" "Target platform"
register_weaviate_var "ACCOUNT" "required" "" "" "PBS project/account to charge for the run"

# Allocation and storage layout
register_weaviate_var "NODES" "default" "1" "" "Compute-node count to allocate for Weaviate workers"
register_weaviate_var "WORKERS_PER_NODE" "default" "4" "" "Worker processes launched per compute node"
register_weaviate_var "CORES" "default" "112" "" "CPU cores assigned per worker rank"
register_weaviate_var "STORAGE_MEDIUM" "default" "memory" "memory DAOS lustre SSD" "Storage medium for Weaviate data"

# PBS scheduler settings
register_weaviate_var "WALLTIME" "required" "" "" "PBS walltime"
register_weaviate_var "QUEUE" "required" "" "preemptable debug debug-scaling prod capacity" "PBS queue name"

# Engine/runtime selection
register_weaviate_var "USEPERF" "default" "false" "true false" "Enable perf collection"
register_weaviate_var "WEAVIATE_CLIENT_BINARY" "default" "test" "" "Client binary copied into the run directory"
register_weaviate_var "BASE_DIR" "default" "" "" "Optional base directory passed through to the client"
register_weaviate_var "VECTOR_DIM" "default" "2560" "" "Vector dimension"
register_weaviate_var "DISTANCE_METRIC" "default" "COSINE" "COSINE DOT L2" "Distance metric"
register_weaviate_var "GPU_INDEX" "default" "false" "true false" "Whether to use GPU indexing"

# Insert / index workload
register_weaviate_var "DATA_FILEPATH" "default" "/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy" "" "Insert corpus file path"
register_weaviate_var "CORPUS_SIZE" "default" "10000000" "" "Insert/index corpus size"
register_weaviate_var "UPLOAD_BATCH_SIZE" "default" "2048" "" "Upload batch size"
register_weaviate_var "UPLOAD_CLIENTS_PER_WORKER" "default" "16" "" "Upload clients per worker"
register_weaviate_var "UPLOAD_BALANCE_STRATEGY" "default" "WORKER_BALANCE" "NO_BALANCE WORKER_BALANCE" "Upload balancing policy"

# Query workload
register_weaviate_var "QUERY_FILEPATH" "default" "/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy" "" "Query vector file path"
register_weaviate_var "QUERY_WORKLOAD" "default" "100000" "" "Number of queries to execute"
register_weaviate_var "QUERY_BATCH_SIZE" "default" "256" "" "Query batch size"
register_weaviate_var "QUERY_TOPK" "default" "10" "" "Query top-k"
register_weaviate_var "QUERY_EF" "default" "64" "" "Query ef parameter"
