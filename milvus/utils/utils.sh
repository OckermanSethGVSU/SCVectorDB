#!/bin/bash

apply_override_value() {
    local var_name="$1"
    local override_name="$2"
    local override_value="${!override_name-}"

    if [[ -n "$override_value" ]]; then
        printf -v "$var_name" '%s' "$override_value"
    fi
}

apply_override_array() {
    local var_name="$1"
    local override_name="$2"
    local override_value="${!override_name-}"
    local -n var_ref="$var_name"

    if [[ -n "$override_value" ]]; then
        read -r -a var_ref <<< "$override_value"
    fi
}

apply_overrides() {
    apply_override_value queue QUEUE_OVERRIDE
    apply_override_array NODES NODES_OVERRIDE
    apply_override_array CORES CORES_OVERRIDE

    apply_override_value WALLTIME WALLTIME_OVERRIDE

    apply_override_value ENV_PATH ENV_PATH_OVERRIDE
    apply_override_value MILVUS_BUILD_DIR MILVUS_BUILD_DIR_OVERRIDE
    apply_override_value MILVUS_CONFIG_DIR MILVUS_CONFIG_DIR_OVERRIDE
    apply_override_value PLATFORM PLATFORM_OVERRIDE

    apply_override_value TASK TASK_OVERRIDE
    apply_override_value RUN_MODE RUN_MODE_OVERRIDE
    apply_override_value MODE MODE_OVERRIDE
    apply_override_value STORAGE_MEDIUM STORAGE_MEDIUM_OVERRIDE
    apply_override_value PERF PERF_OVERRIDE
    apply_override_value WAL WAL_OVERRIDE
    apply_override_value GPU_INDEX GPU_INDEX_OVERRIDE
    apply_override_value TRACING TRACING_OVERRIDE
    apply_override_value DEBUG DEBUG_OVERRIDE
    apply_override_value BASE_DIR BASE_DIR_OVERRIDE
    apply_override_value PERF_EVENTS PERF_EVENTS_OVERRIDE

    apply_override_value INSERT_CORPUS_SIZE INSERT_CORPUS_SIZE_OVERRIDE
    apply_override_value INSERT_CLIENTS_PER_PROXY INSERT_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value IMPORT_PROCESSES IMPORT_PROCESSES_OVERRIDE
    apply_override_value INSERT_METHOD INSERT_METHOD_OVERRIDE
    apply_override_value BULK_UPLOAD_TRANSPORT BULK_UPLOAD_TRANSPORT_OVERRIDE
    apply_override_value BULK_UPLOAD_STAGING_MEDIUM BULK_UPLOAD_STAGING_MEDIUM_OVERRIDE
    apply_override_value INSERT_BALANCE_STRATEGY INSERT_BALANCE_STRATEGY_OVERRIDE
    apply_override_value INSERT_STREAMING INSERT_STREAMING_OVERRIDE
    apply_override_value INSERT_DATA_FILEPATH INSERT_DATA_FILEPATH_OVERRIDE
    apply_override_array INSERT_BATCH_SIZE INSERT_BATCH_SIZE_OVERRIDE

    apply_override_value VECTOR_DIM VECTOR_DIM_OVERRIDE
    apply_override_value DISTANCE_METRIC DISTANCE_METRIC_OVERRIDE
    apply_override_value INIT_FLAT_INDEX INIT_FLAT_INDEX_OVERRIDE
    apply_override_value SHARDS SHARDS_OVERRIDE
    apply_override_value FLUSH_BEFORE_INDEX FLUSH_BEFORE_INDEX_OVERRIDE

    apply_override_value QUERY_CORPUS_SIZE QUERY_CORPUS_SIZE_OVERRIDE
    apply_override_value QUERY_CLIENTS_PER_PROXY QUERY_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value QUERY_BALANCE_STRATEGY QUERY_BALANCE_STRATEGY_OVERRIDE
    apply_override_value QUERY_STREAMING QUERY_STREAMING_OVERRIDE
    apply_override_value QUERY_DATA_FILEPATH QUERY_DATA_FILEPATH_OVERRIDE
    apply_override_array QUERY_BATCH_SIZE QUERY_BATCH_SIZE_OVERRIDE

    apply_override_value MIXED_RESULT_PATH MIXED_RESULT_PATH_OVERRIDE
    apply_override_value MIXED_INSERT_BATCH_SIZE MIXED_INSERT_BATCH_SIZE_OVERRIDE
    apply_override_value MIXED_QUERY_BATCH_SIZE MIXED_QUERY_BATCH_SIZE_OVERRIDE
    apply_override_value INSERT_MODE INSERT_MODE_OVERRIDE
    apply_override_value INSERT_OPS_PER_SEC INSERT_OPS_PER_SEC_OVERRIDE
    apply_override_value QUERY_MODE QUERY_MODE_OVERRIDE
    apply_override_value QUERY_OPS_PER_SEC QUERY_OPS_PER_SEC_OVERRIDE
    apply_override_value MIXED_CORPUS_SIZE MIXED_CORPUS_SIZE_OVERRIDE
    apply_override_value MIXED_DATA_FILEPATH MIXED_DATA_FILEPATH_OVERRIDE
    apply_override_value MIXED_QUERY_CLIENTS_PER_PROXY MIXED_QUERY_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value MIXED_INSERT_CLIENTS_PER_PROXY MIXED_INSERT_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value INSERT_START_ID INSERT_START_ID_OVERRIDE
    apply_override_value COLLECTION_NAME COLLECTION_NAME_OVERRIDE
    apply_override_value VECTOR_FIELD VECTOR_FIELD_OVERRIDE
    apply_override_value ID_FIELD ID_FIELD_OVERRIDE
    apply_override_value TOP_K TOP_K_OVERRIDE
    apply_override_value QUERY_EF_SEARCH QUERY_EF_SEARCH_OVERRIDE
    apply_override_value SEARCH_CONSISTENCY SEARCH_CONSISTENCY_OVERRIDE
    apply_override_value RPC_TIMEOUT RPC_TIMEOUT_OVERRIDE
    apply_override_value MIXED_INSERT_BATCH_MIN MIXED_INSERT_BATCH_MIN_OVERRIDE
    apply_override_value MIXED_INSERT_BATCH_MAX MIXED_INSERT_BATCH_MAX_OVERRIDE
    apply_override_value MIXED_QUERY_BATCH_MIN MIXED_QUERY_BATCH_MIN_OVERRIDE
    apply_override_value MIXED_QUERY_BATCH_MAX MIXED_QUERY_BATCH_MAX_OVERRIDE
    apply_override_value INSERT_BATCH_MIN INSERT_BATCH_MIN_OVERRIDE
    apply_override_value INSERT_BATCH_MAX INSERT_BATCH_MAX_OVERRIDE
    apply_override_value QUERY_BATCH_MIN QUERY_BATCH_MIN_OVERRIDE
    apply_override_value QUERY_BATCH_MAX QUERY_BATCH_MAX_OVERRIDE

    apply_override_value RESTORE_DIR RESTORE_DIR_OVERRIDE
    apply_override_value EXPECTED_CORPUS_SIZE EXPECTED_CORPUS_SIZE_OVERRIDE

    apply_override_value MINIO_MODE MINIO_MODE_OVERRIDE
    apply_override_value MINIO_MEDIUM MINIO_MEDIUM_OVERRIDE
    apply_override_value ETCD_MODE ETCD_MODE_OVERRIDE
    apply_override_value COORDINATOR_NODES COORDINATOR_NODES_OVERRIDE
    apply_override_value COORDINATOR_NODES_PER_CN COORDINATOR_NODES_PER_CN_OVERRIDE
    apply_override_value COORDINATOR_NODES CORD_NODES_OVERRIDE
    apply_override_value COORDINATOR_NODES_PER_CN CORD_NODES_PER_CN_OVERRIDE
    apply_override_value STREAMING_NODES STREAMING_NODES_OVERRIDE
    apply_override_value STREAMING_NODES_PER_CN STREAMING_NODES_PER_CN_OVERRIDE
    apply_override_value QUERY_NODES QUERY_NODES_OVERRIDE
    apply_override_value QUERY_NODES_PER_CN QUERY_NODES_PER_CN_OVERRIDE
    apply_override_value DATA_NODES DATA_NODES_OVERRIDE
    apply_override_value DATA_NODES_PER_CN DATA_NODES_PER_CN_OVERRIDE
    apply_override_value NUM_PROXIES NUM_PROXIES_OVERRIDE
    apply_override_value NUM_PROXIES_PER_CN NUM_PROXIES_PER_CN_OVERRIDE
    apply_override_value DML_CHANNELS DML_CHANNELS_OVERRIDE
}

lower_string() {
    printf '%s' "${1,,}"
}

validate_programmatic_submit_config() {
    local num_nodes="$1"
    local num_cores="$2"
    local upload_bs="$3"
    local query_bs="$4"

    local mode_lower task_lower minio_mode_lower etcd_mode_lower insert_method_lower tracing_lower
    mode_lower="$(lower_string "$MODE")"
    task_lower="$(lower_string "$TASK")"
    minio_mode_lower="$(lower_string "$MINIO_MODE")"
    etcd_mode_lower="$(lower_string "$ETCD_MODE")"
    insert_method_lower="$(lower_string "${INSERT_METHOD:-traditional}")"
    tracing_lower="$(lower_string "${TRACING:-false}")"

    local errors=()
    local available_worker_nodes="$num_nodes"

    if (( num_nodes <= 0 )); then
        errors+=("NODES must be > 0; got '$num_nodes'.")
    fi

    if (( num_cores <= 0 )); then
        errors+=("CORES must be > 0; got '$num_cores'.")
    fi

    if [[ "$mode_lower" == "distributed" ]]; then
        if [[ "$minio_mode_lower" != "single" && "$minio_mode_lower" != "stripped" ]]; then
            errors+=("DISTRIBUTED mode requires MINIO_MODE to be 'single' or 'stripped'; got '$MINIO_MODE'.")
        fi

        if [[ "$etcd_mode_lower" != "single" && "$etcd_mode_lower" != "replicated" ]]; then
            errors+=("DISTRIBUTED mode requires ETCD_MODE to be 'single' or 'replicated'; got '$ETCD_MODE'.")
        fi

        if (( COORDINATOR_NODES <= 0 || COORDINATOR_NODES_PER_CN <= 0 )); then
            errors+=("COORDINATOR_NODES and COORDINATOR_NODES_PER_CN must be > 0.")
        elif (( (COORDINATOR_NODES + COORDINATOR_NODES_PER_CN - 1) / COORDINATOR_NODES_PER_CN > available_worker_nodes )); then
            errors+=("Coordinator layout needs more than $available_worker_nodes worker nodes.")
        fi

        if (( STREAMING_NODES <= 0 || STREAMING_NODES_PER_CN <= 0 )); then
            errors+=("STREAMING_NODES and STREAMING_NODES_PER_CN must be > 0.")
        elif (( (STREAMING_NODES + STREAMING_NODES_PER_CN - 1) / STREAMING_NODES_PER_CN > available_worker_nodes )); then
            errors+=("Streaming layout needs more than $available_worker_nodes worker nodes.")
        fi

        if (( QUERY_NODES <= 0 || QUERY_NODES_PER_CN <= 0 )); then
            errors+=("QUERY_NODES and QUERY_NODES_PER_CN must be > 0.")
        elif (( (QUERY_NODES + QUERY_NODES_PER_CN - 1) / QUERY_NODES_PER_CN > available_worker_nodes )); then
            errors+=("Query layout needs more than $available_worker_nodes worker nodes.")
        fi

        if (( DATA_NODES <= 0 || DATA_NODES_PER_CN <= 0 )); then
            errors+=("DATA_NODES and DATA_NODES_PER_CN must be > 0.")
        elif (( (DATA_NODES + DATA_NODES_PER_CN - 1) / DATA_NODES_PER_CN > available_worker_nodes )); then
            errors+=("Data layout needs more than $available_worker_nodes worker nodes.")
        fi

        if (( NUM_PROXIES <= 0 || NUM_PROXIES_PER_CN <= 0 )); then
            errors+=("NUM_PROXIES and NUM_PROXIES_PER_CN must be > 0.")
        elif (( (NUM_PROXIES + NUM_PROXIES_PER_CN - 1) / NUM_PROXIES_PER_CN > available_worker_nodes )); then
            errors+=("Proxy layout needs more than $available_worker_nodes worker nodes.")
        fi
    elif [[ "$mode_lower" == "standalone" ]]; then
        if [[ "$minio_mode_lower" != "off" && "$minio_mode_lower" != "single" ]]; then
            errors+=("STANDALONE mode requires MINIO_MODE to be 'off' or 'single'; got '$MINIO_MODE'.")
        fi
    else
        errors+=("MODE must be 'DISTRIBUTED' or 'STANDALONE'; got '$MODE'.")
    fi

    if [[ "$task_lower" == "import" || "$insert_method_lower" == "bulk" ]]; then
        if [[ "$minio_mode_lower" == "off" ]]; then
            errors+=("Bulk import workflows require remote MinIO; MINIO_MODE cannot be 'off'.")
        fi
    fi

    if [[ "$tracing_lower" != "true" && "$tracing_lower" != "false" ]]; then
        errors+=("TRACING must be 'True' or 'False'; got '$TRACING'.")
    fi

    if (( ${#errors[@]} > 0 )); then
        echo "Programmatic config validation failed before submit:" >&2
        echo "  MODE=$MODE TASK=$TASK NODES=$num_nodes CORES=$num_cores INSERT_BATCH_SIZE=$upload_bs QUERY_BATCH_SIZE=$query_bs MINIO_MODE=$MINIO_MODE ETCD_MODE=$ETCD_MODE" >&2
        local err
        for err in "${errors[@]}"; do
            echo "  - $err" >&2
        done
        return 1
    fi
}

print_config_summary() {
    echo
    echo "                Experiment Configuration"
    echo "======================================================"

    echo "Platform:                 $PLATFORM"
    echo "Mode:                     $MODE"
    echo "Task:                     $TASK"
    echo "Run Mode:                 $RUN_MODE"
    echo "Storage Medium:           $STORAGE_MEDIUM"
    echo "Env Path:                 $ENV_PATH"
    echo "Milvus Build Dir:         $MILVUS_BUILD_DIR"
    echo "Milvus Config Dir:        $MILVUS_CONFIG_DIR"
    echo "Perf:                     $PERF"
    echo "Perf Events:              ${PERF_EVENTS:-<default>}"
    echo "Tracing:                  $TRACING"
    echo "Debug:                    $DEBUG"
    echo "Vector Dim:               $VECTOR_DIM"
    echo "Distance Metric:          $DISTANCE_METRIC"
    echo "Init Flat Index:          $INIT_FLAT_INDEX"
    echo "Shards:                   ${SHARDS:-<auto: streaming nodes>}"
    echo "Flush Before Index:       ${FLUSH_BEFORE_INDEX:-TRUE}"
    echo "GPU Index:                $GPU_INDEX"

    case "$TASK" in
        INSERT)
            echo "Insert Corpus Size:       $INSERT_CORPUS_SIZE"
            echo "Insert Data File:         $INSERT_DATA_FILEPATH"
            echo "Insert Batch Sizes:       ${INSERT_BATCH_SIZE[*]}"
            echo "Insert Clients/Proxy:     $INSERT_CLIENTS_PER_PROXY"
            echo "Insert Method:            ${INSERT_METHOD:-traditional}"
            echo "Insert Balance:           $INSERT_BALANCE_STRATEGY"
            echo "Insert Streaming:         $INSERT_STREAMING"
            ;;
        IMPORT)
            echo "Bulk Upload Corpus Size:  $INSERT_CORPUS_SIZE"
            echo "Bulk Upload Data File:    $INSERT_DATA_FILEPATH"
            echo "Bulk Upload Batch Sizes:  ${INSERT_BATCH_SIZE[*]}"
            echo "Import Processes:         $IMPORT_PROCESSES"
            echo "Bulk Transport:           ${BULK_UPLOAD_TRANSPORT:-writer}"
            echo "Bulk Staging Medium:      ${BULK_UPLOAD_STAGING_MEDIUM:-${STORAGE_MEDIUM:-lustre}}"
            echo "Collection Name:          ${COLLECTION_NAME:-standalone}"
            echo "Vector Field:             ${VECTOR_FIELD:-vector}"
            echo "ID Field:                 ${ID_FIELD:-id}"
            ;;
        INDEX)
            echo "Index Corpus Size:        $INSERT_CORPUS_SIZE"
            echo "Preload Bulk Transport:   ${BULK_UPLOAD_TRANSPORT:-writer}"
            echo "Preload Staging Medium:   ${BULK_UPLOAD_STAGING_MEDIUM:-${STORAGE_MEDIUM:-lustre}}"
            echo "Restore Dir:              ${RESTORE_DIR:-<unset>}"
            echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
            ;;
        QUERY)
            echo "Query Corpus Size:        $QUERY_CORPUS_SIZE"
            echo "Query Data File:          $QUERY_DATA_FILEPATH"
            echo "Query Batch Sizes:        ${QUERY_BATCH_SIZE[*]}"
            echo "Query Clients/Proxy:      $QUERY_CLIENTS_PER_PROXY"
            echo "Preload Insert Method:    ${INSERT_METHOD:-traditional}"
            echo "Preload Bulk Transport:   ${BULK_UPLOAD_TRANSPORT:-writer}"
            echo "Preload Staging Medium:   ${BULK_UPLOAD_STAGING_MEDIUM:-${STORAGE_MEDIUM:-lustre}}"
            echo "Query Balance:            $QUERY_BALANCE_STRATEGY"
            echo "Query Streaming:          $QUERY_STREAMING"
            echo "Restore Dir:              ${RESTORE_DIR:-<unset>}"
            echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
            ;;
        MIXED)
            echo "Insert Corpus Size:       $INSERT_CORPUS_SIZE"
            echo "Insert Data File:         $INSERT_DATA_FILEPATH"
            echo "Insert Batch Sizes:       ${MIXED_INSERT_BATCH_SIZE:-${INSERT_BATCH_SIZE[*]}}"
            echo "Insert Clients/Proxy:     $INSERT_CLIENTS_PER_PROXY"
            echo "Preload Bulk Transport:   ${BULK_UPLOAD_TRANSPORT:-writer}"
            echo "Preload Staging Medium:   ${BULK_UPLOAD_STAGING_MEDIUM:-${STORAGE_MEDIUM:-lustre}}"
            echo "Insert Balance:           $INSERT_BALANCE_STRATEGY"
            echo "Insert Streaming:         $INSERT_STREAMING"
            echo "Query Corpus Size:        $QUERY_CORPUS_SIZE"
            echo "Query Data File:          $QUERY_DATA_FILEPATH"
            echo "Query Batch Sizes:        ${MIXED_QUERY_BATCH_SIZE:-${QUERY_BATCH_SIZE[*]}}"
            echo "Query Clients/Proxy:      $QUERY_CLIENTS_PER_PROXY"
            echo "Query Balance:            $QUERY_BALANCE_STRATEGY"
            echo "Query Streaming:          $QUERY_STREAMING"
            echo "Mixed Corpus Size:        $MIXED_CORPUS_SIZE"
            echo "Mixed Data File:          $MIXED_DATA_FILEPATH"
            echo "Mixed Insert Clients:     $MIXED_INSERT_CLIENTS_PER_PROXY"
            echo "Mixed Query Clients:      $MIXED_QUERY_CLIENTS_PER_PROXY"
            echo "Insert Mode:              $INSERT_MODE"
            echo "Insert Ops/Sec:           ${INSERT_OPS_PER_SEC:-<unset>}"
            echo "Query Mode:               $QUERY_MODE"
            echo "Query Ops/Sec:            ${QUERY_OPS_PER_SEC:-<unset>}"
            echo "Mixed Result Path:        $MIXED_RESULT_PATH"
            echo "Insert Start ID:          $INSERT_START_ID"
            echo "Collection Name:          $COLLECTION_NAME"
            echo "Vector Field:             $VECTOR_FIELD"
            echo "ID Field:                 $ID_FIELD"
            echo "Top K:                    $TOP_K"
            echo "Query EF Search:          ${QUERY_EF_SEARCH:-<unset>}"
            echo "Search Consistency:       $SEARCH_CONSISTENCY"
            echo "RPC Timeout:              $RPC_TIMEOUT"
            echo "Insert Batch Min:         ${MIXED_INSERT_BATCH_MIN:-${INSERT_BATCH_MIN:-<unset>}}"
            echo "Insert Batch Max:         ${MIXED_INSERT_BATCH_MAX:-${INSERT_BATCH_MAX:-<unset>}}"
            echo "Query Batch Min:          ${MIXED_QUERY_BATCH_MIN:-${QUERY_BATCH_MIN:-<unset>}}"
            echo "Query Batch Max:          ${MIXED_QUERY_BATCH_MAX:-${QUERY_BATCH_MAX:-<unset>}}"
            echo "Restore Dir:              ${RESTORE_DIR:-<unset>}"
            echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
            ;;
    esac

    if [[ "$MODE" == "DISTRIBUTED" ]]; then
        echo "MinIO Mode:               $MINIO_MODE"
        echo "MinIO Medium:             $MINIO_MEDIUM"
        echo "ETCD Mode:                $ETCD_MODE"
        echo "Coordinator Nodes:        $COORDINATOR_NODES"
        echo "Coordinator/Compute Node: $COORDINATOR_NODES_PER_CN"
        echo "Streaming Nodes:          $STREAMING_NODES"
        echo "Streaming/Compute Node:   $STREAMING_NODES_PER_CN"
        echo "Query Nodes:              $QUERY_NODES"
        echo "Query/Compute Node:       $QUERY_NODES_PER_CN"
        echo "Data Nodes:               $DATA_NODES"
        echo "Data/Compute Node:        $DATA_NODES_PER_CN"
        echo "Proxies:                  $NUM_PROXIES"
        echo "Proxies/Compute Node:     $NUM_PROXIES_PER_CN"
        echo "DML Channels:             $DML_CHANNELS"
    else
        echo "WAL Mode:                 $WAL"
        echo "MinIO Mode:               $MINIO_MODE"
        echo "MinIO Medium:             $MINIO_MEDIUM"
        case "$TASK" in
            INSERT)
                echo "Standalone Cores:         ${CORES[*]}"
                ;;
            IMPORT)
                echo "Standalone Cores:         ${CORES[*]}"
                ;;
            INDEX)
                echo "Standalone Cores:         ${CORES[*]}"
                ;;
            QUERY)
                echo "Standalone Cores:         ${CORES[*]}"
                ;;
        esac
    fi

    echo "======================================================"
    echo
}
