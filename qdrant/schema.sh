#!/bin/bash

# Core execution mode and platform
register_qdrant_var "TASK" "required" "" "INSERT INDEX QUERY MIXED" "Experiment task"
register_qdrant_var "RUN_MODE" "default" "PBS" "PBS local" "Run under PBS or create a local harness"
register_qdrant_var "PLATFORM" "required" "" "POLARIS AURORA" "Target platform"
register_qdrant_var "ACCOUNT" "required" "" "" "PBS project/account to charge for the run"

# Allocation and storage layout
register_qdrant_var "NODES" "default" "1" "" "Compute-node count to allocate for Qdrant workers"
register_qdrant_var "WORKERS_PER_NODE" "default" "1" "" "Worker processes launched per compute node"
register_qdrant_var "CORES" "default" "" "" "CPU cores assigned per worker rank; empty means no core binding"
register_qdrant_var "STORAGE_MEDIUM" "default" "memory" "memory DAOS lustre SSD" "Storage medium for Qdrant data"

# PBS scheduler settings
register_qdrant_var "WALLTIME" "required" "" "" "PBS walltime"
register_qdrant_var "QUEUE" "required" "" "preemptable debug debug-scaling prod capacity" "PBS queue name"

# Engine/runtime selection
register_qdrant_var "QDRANT_EXECUTABLE" "default" "qdrant" "qdrant qdrantInsertTracing qdrantQueryTrace" "Qdrant executable variant"
register_qdrant_var "VECTOR_DIM" "default" "200" "" "Vector dimension"
register_qdrant_var "DISTANCE_METRIC" "default" "IP" "IP COSINE L2" "Distance metric"
register_qdrant_var "GPU_INDEX" "default" "False" "True False" "Whether to use GPU indexing"
register_qdrant_var "REBALANCE_TOPOLOGY" "default" "False" "True False" "Whether configure_collection should actively move shards to the target topology"


# Insert / preload workload
register_qdrant_var "INSERT_FILEPATH" "conditional" "" "" "Insert corpus file path" "TASK=INSERT|INDEX|QUERY|MIXED"
register_qdrant_var "INSERT_CORPUS_SIZE" "default" "" "" "Total vectors available to preload; empty means use all rows in the file"
register_qdrant_var "INSERT_BATCH_SIZE" "default" "512" "" "Insert batch size; single value or sweep list"
register_qdrant_var "INSERT_CLIENTS_PER_WORKER" "default" "1" "" "Insert clients per worker"
register_qdrant_var "INSERT_BALANCE_STRATEGY" "default" "WORKER_BALANCE" "NO_BALANCE WORKER_BALANCE" "Insert load balancing policy"
register_qdrant_var "INSERT_STREAMING" "default" "False" "True False" "Enable streaming insert behavior"

# Query workload
register_qdrant_var "QUERY_FILEPATH" "conditional" "" "" "Query vector file path" "TASK=QUERY|MIXED"
register_qdrant_var "QUERY_CORPUS_SIZE" "default" "" "" "Total queries to execute; empty means use all rows in the file"
register_qdrant_var "QUERY_BATCH_SIZE" "default" "32" "" "Query batch size; single value or sweep list"
register_qdrant_var "QUERY_CLIENTS_PER_WORKER" "conditional" "1" "" "Query clients per worker" "TASK=QUERY|MIXED"
register_qdrant_var "TOTAL_QUERY_CLIENTS" "conditional" "1" "" "Total query clients across the run" "TASK=QUERY|MIXED"
register_qdrant_var "QUERY_BALANCE_STRATEGY" "conditional" "NO_BALANCE" "NO_BALANCE WORKER_BALANCE" "Query balancing policy" "TASK=QUERY|MIXED"
register_qdrant_var "QUERY_STREAMING" "default" "" "True False" "Enable query streaming behavior"

# Mixed workload controls
register_qdrant_var "MIXED_DATA_FILEPATH" "conditional" "" "" "Mixed workload data file" "TASK=MIXED"
register_qdrant_var "MIXED_CORPUS_SIZE" "conditional" "1000" "" "Mixed-workload corpus size" "TASK=MIXED"
register_qdrant_var "INSERT_MODE" "default" "MAX" "MAX RATE" "Mixed insert pacing mode"
register_qdrant_var "INSERT_OPS_PER_SEC" "conditional" "" "" "Required when INSERT_MODE=RATE" "INSERT_MODE=RATE"
register_qdrant_var "QUERY_MODE" "default" "MAX" "MAX RATE" "Mixed query pacing mode"
register_qdrant_var "QUERY_OPS_PER_SEC" "conditional" "" "" "Required when QUERY_MODE=RATE" "QUERY_MODE=RATE"
register_qdrant_var "MIXED_INSERT_CLIENTS_PER_WORKER" "conditional" "1" "" "Mixed insert clients per worker" "TASK=MIXED"
register_qdrant_var "MIXED_QUERY_CLIENTS_PER_WORKER" "conditional" "1" "" "Mixed query clients per worker" "TASK=MIXED"
register_qdrant_var "RESULT_PATH" "default" "mixed_logs" "" "Output subdirectory for mixed workload logs"

register_qdrant_var "INSERT_BATCH_MIN" "default" "" "" "Optional randomized insert batch lower bound"
register_qdrant_var "INSERT_BATCH_MAX" "default" "" "" "Optional randomized insert batch upper bound"
register_qdrant_var "QUERY_BATCH_MIN" "default" "" "" "Optional randomized query batch lower bound"
register_qdrant_var "QUERY_BATCH_MAX" "default" "" "" "Optional randomized query batch upper bound"

register_qdrant_var "COLLECTION_NAME" "default" "" "" "Optional collection override"
register_qdrant_var "TOP_K" "default" "" "" "Optional top-k override"
register_qdrant_var "QUERY_EF_SEARCH" "default" "" "" "Optional ef_search override"
register_qdrant_var "RPC_TIMEOUT" "default" "" "" "Optional RPC timeout override"

# Restore / recovery
register_qdrant_var "RESTORE_DIR" "default" "" "" "Restore an existing Qdrant state from this directory"
register_qdrant_var "EXPECTED_CORPUS_SIZE" "default" "10000000" "" "Expected corpus size when restoring"


# Profiling
register_qdrant_var "PERF" "default" "NONE" "NONE STAT TRACE" "Performance collection mode"
register_qdrant_var "PERF_EVENTS" "default" "topdown-be-bound,topdown-mem-bound,topdown-retiring,topdown-fe-bound,topdown-bad-spec" "" "Comma-separated perf stat events"
register_qdrant_var "INSERT_TRACE" "default" "" "" "Optional insert trace file or mode"
register_qdrant_var "QUERY_TRACE" "default" "" "" "Optional query trace file or mode"
