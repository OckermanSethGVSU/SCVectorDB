#!/bin/bash

source "$ENGINE_DIR/utils/utils.sh"

ENGINE_NAME="milvus"

# Set Milvus defaults used before CLI/env overrides are applied.
#
# Milvus has not yet been converted to schema.sh, so this function still owns
# the default values directly.
engine_set_defaults() {
    NODES=(1)
    CORES=(112)

    ENV_PATH="/lus/flare/projects/radix-io/sockerman/milvusEnv/"
    MILVUS_BUILD_DIR="cpuMilvus"
    MILVUS_CONFIG_DIR="cpuMilvus"

    TASK=""
    RUN_MODE="PBS"
    MODE="DISTRIBUTED"
    STORAGE_MEDIUM="memory"
    PERF="NONE"
    PERF_EVENTS="topdown-be-bound,topdown-mem-bound,topdown-retiring,topdown-fe-bound,topdown-bad-spec"
    WAL="woodpecker"
    GPU_INDEX="False"
    TRACING="False"
    DEBUG="False"
    BASE_DIR="$(pwd)"
    MINIO_MODE="stripped"
    MINIO_MEDIUM="lustre"
    ETCD_MEDIUM="memory"
    LOCAL_SHARED_STORAGE_PATH=""

    INSERT_CORPUS_SIZE=10000000
    INSERT_CLIENTS_PER_PROXY=8
    INSERT_METHOD="traditional"
    BULK_UPLOAD_TRANSPORT="mc"
    BULK_UPLOAD_STAGING_MEDIUM="memory"
    IMPORT_PROCESSES=204
    INSERT_BALANCE_STRATEGY="WORKER"
    INSERT_STREAMING="True"
    INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/mergedData/embeddings_merged.npy"
    INSERT_BATCH_SIZE=(512)

    VECTOR_DIM=2560
    DISTANCE_METRIC="IP"
    INIT_FLAT_INDEX="FALSE"
    SHARDS="16"
    DML_CHANNELS=16
    FLUSH_BEFORE_INDEX="TRUE"

    QUERY_CORPUS_SIZE=100000
    QUERY_CLIENTS_PER_PROXY=1
    QUERY_BALANCE_STRATEGY="NONE"
    QUERY_STREAMING="False"
    QUERY_BATCH_SIZE=(32)
    QUERY_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy"

    INSERT_MODE="max"
    INSERT_OPS_PER_SEC=""
    MIXED_INSERT_BATCH_SIZE=32
    QUERY_MODE="max"
    QUERY_OPS_PER_SEC=""
    MIXED_QUERY_BATCH_SIZE=32
    MIXED_RESULT_PATH="mixed_logs"
    MIXED_CORPUS_SIZE=1000000
    MIXED_QUERY_CLIENTS_PER_PROXY=1
    MIXED_INSERT_CLIENTS_PER_PROXY=1
    MIXED_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/10M_part2.npy"

    COLLECTION_NAME=""
    VECTOR_FIELD=""
    ID_FIELD=""
    TOP_K=""
    QUERY_EF_SEARCH=""
    SEARCH_CONSISTENCY=""
    RPC_TIMEOUT=""

    BULK_IMPORT_PREPARE_ONLY="TRUE"
    BULK_IMPORT_REQUEST_PATH=""
    BULK_IMPORT_LOAD_REQUEST=""
    BULK_IMPORT_SUMMARY_PATH=""

    MIXED_INSERT_BATCH_MIN=""
    MIXED_INSERT_BATCH_MAX=""
    MIXED_QUERY_BATCH_MIN=""
    MIXED_QUERY_BATCH_MAX=""
    INSERT_BATCH_MIN=""
    INSERT_BATCH_MAX=""
    QUERY_BATCH_MIN=""
    QUERY_BATCH_MAX=""

    RESTORE_DIR=""
    EXPECTED_CORPUS_SIZE=10000000

    ETCD_MODE="single"
    STREAMING_NODES=2
    STREAMING_NODES_PER_CN=2
    QUERY_NODES=1
    QUERY_NODES_PER_CN=1
    DATA_NODES=1
    DATA_NODES_PER_CN=1
    COORDINATOR_NODES=1
    COORDINATOR_NODES_PER_CN=1
    NUM_PROXIES=1
    NUM_PROXIES_PER_CN=1

    REQUIRES_DAOS="false"
}

# Apply root-manager overrides and derive dependent Milvus storage defaults.
engine_apply_overrides() {
    apply_overrides

    if [[ -z "$ETCD_MEDIUM" ]]; then
        ETCD_MEDIUM="$STORAGE_MEDIUM"
    fi

    if [[ -z "$MINIO_MODE" ]]; then
        if [[ "$MODE" == "DISTRIBUTED" ]]; then
            MINIO_MODE="stripped"
        else
            MINIO_MODE="off"
        fi
    fi
}

# Validate required Milvus settings and derive INSERT_START_ID.
engine_validate_config() {
    [[ -n "${TASK:-}" ]] || {
        echo "Missing required Milvus setting: TASK" >&2
        exit 1
    }

    if [[ -n "${INSERT_START_ID:-}" ]]; then
        INSERT_START_ID="$INSERT_START_ID"
    elif [[ -n "$RESTORE_DIR" ]]; then
        INSERT_START_ID="$EXPECTED_CORPUS_SIZE"
    else
        INSERT_START_ID="$INSERT_CORPUS_SIZE"
    fi
}

# Print the resolved Milvus configuration using the legacy summary helper.
engine_print_summary() {
    print_config_summary
}

# Emit the legacy Milvus sweep matrix as pipe-delimited combo records.
engine_iterate_matrix() {
    local num_nodes
    local query_bs
    local upload_bs
    local num_cores

    for num_nodes in "${NODES[@]}"; do
        for query_bs in "${QUERY_BATCH_SIZE[@]}"; do
            for upload_bs in "${INSERT_BATCH_SIZE[@]}"; do
                for num_cores in "${CORES[@]}"; do
                    echo "${num_nodes}|${query_bs}|${upload_bs}|${num_cores}"
                done
            done
        done
    done
}

# Load one legacy matrix combo into scalar globals used by naming/env emission.
engine_load_combo() {
    IFS='|' read -r NODES_CURRENT QUERY_BATCH_CURRENT INSERT_BATCH_CURRENT CORES_CURRENT <<< "$1"
    TOTAL_NODES=$((NODES_CURRENT + 1))
    JOB_NAME="${TASK,,}_${MODE,,}_${NODES_CURRENT}n_${CORES_CURRENT}c_q${QUERY_BATCH_CURRENT}"

    if [[ "$STORAGE_MEDIUM" == "DAOS" || "$ETCD_MEDIUM" == "DAOS" || ( "$MODE" == "DISTRIBUTED" && "$MINIO_MEDIUM" == "DAOS" ) ]]; then
        REQUIRES_DAOS="true"
    else
        REQUIRES_DAOS="false"
    fi
}

# Validate a loaded Milvus combo against scheduler/resource constraints.
engine_validate_combo() {
    validate_programmatic_submit_config "$NODES_CURRENT" "$CORES_CURRENT" "$INSERT_BATCH_CURRENT" "$QUERY_BATCH_CURRENT"
}

# Build the Milvus run directory name for the loaded task and combo.
engine_make_run_dir_name() {
    local timestamp
    local corpus_size_for_dir

    timestamp="$(date +"%Y-%m-%d_%H_%M_%S")"
    case "$TASK" in
        INSERT)
            if [[ "$MODE" == "DISTRIBUTED" ]]; then
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_CPP${INSERT_CLIENTS_PER_PROXY}_uploadBS${INSERT_BATCH_CURRENT}_SN${STREAMING_NODES}_SPCN${STREAMING_NODES_PER_CN}_${timestamp}"
            else
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_CPP${INSERT_CLIENTS_PER_PROXY}_uploadBS${INSERT_BATCH_CURRENT}_${timestamp}"
            fi
            ;;
        IMPORT)
            if [[ "$MODE" == "DISTRIBUTED" ]]; then
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_P${IMPORT_PROCESSES}_BS${INSERT_BATCH_CURRENT}_CS${INSERT_CORPUS_SIZE}_${timestamp}"
            else
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${CORES_CURRENT}_N${NODES_CURRENT}_P${IMPORT_PROCESSES}_BS${INSERT_BATCH_CURRENT}_CS${INSERT_CORPUS_SIZE}_${timestamp}"
            fi
            ;;
        INDEX)
            if [[ "$MODE" == "DISTRIBUTED" ]]; then
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_${INSERT_CORPUS_SIZE}_${timestamp}"
            else
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${CORES_CURRENT}_N${NODES_CURRENT}_${INSERT_CORPUS_SIZE}_${timestamp}"
            fi
            ;;
        QUERY)
            if [[ "$MODE" == "DISTRIBUTED" ]]; then
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_${QUERY_BATCH_CURRENT}_${timestamp}"
            else
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${CORES_CURRENT}_N${NODES_CURRENT}_${QUERY_BATCH_CURRENT}_${timestamp}"
            fi
            ;;
        MIXED)
            corpus_size_for_dir="$INSERT_CORPUS_SIZE"
            if [[ -n "$RESTORE_DIR" ]]; then
                corpus_size_for_dir="$EXPECTED_CORPUS_SIZE"
            fi

            if [[ "$MODE" == "DISTRIBUTED" ]]; then
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_IBS${INSERT_BATCH_CURRENT}_QBS${QUERY_BATCH_CURRENT}_CS${corpus_size_for_dir}_${timestamp}"
            else
                echo "${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${CORES_CURRENT}_N${NODES_CURRENT}_IBS${INSERT_BATCH_CURRENT}_QBS${QUERY_BATCH_CURRENT}_CS${corpus_size_for_dir}_${timestamp}"
            fi
            ;;
        *)
            echo "Unknown task: $TASK" >&2
            exit 1
            ;;
    esac
}

# Select the launch script copied into submit.sh for local vs PBS runs.
engine_main_script_path() {
    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
        echo "local_main.sh"
    else
        echo "main.sh"
    fi
}

# Emit run_config.env contents consumed by Milvus launch/client scripts.
engine_emit_runtime_env() {
    cat <<EOF
NODES=${NODES_CURRENT}
PLATFORM=${PLATFORM}
BASE_DIR=${BASE_DIR}
ENV_PATH=${ENV_PATH}
MILVUS_BUILD_DIR=${MILVUS_BUILD_DIR}
MILVUS_CONFIG_DIR=${MILVUS_CONFIG_DIR}
TASK=${TASK}
RUN_MODE=${RUN_MODE}
MODE=${MODE}
STORAGE_MEDIUM=${STORAGE_MEDIUM}
CORES=${CORES_CURRENT}
WAL=${WAL}
INSERT_DATA_FILEPATH=${INSERT_DATA_FILEPATH}
INSERT_METHOD=${INSERT_METHOD}
BULK_UPLOAD_TRANSPORT=${BULK_UPLOAD_TRANSPORT}
BULK_UPLOAD_STAGING_MEDIUM=${BULK_UPLOAD_STAGING_MEDIUM}
INSERT_BALANCE_STRATEGY=${INSERT_BALANCE_STRATEGY}
INSERT_STREAMING=${INSERT_STREAMING}
INSERT_CORPUS_SIZE=${INSERT_CORPUS_SIZE}
INSERT_BATCH_SIZE=${INSERT_BATCH_CURRENT}
INSERT_CLIENTS_PER_PROXY=${INSERT_CLIENTS_PER_PROXY}
IMPORT_PROCESSES=${IMPORT_PROCESSES}
BULK_IMPORT_PREPARE_ONLY=${BULK_IMPORT_PREPARE_ONLY}
BULK_IMPORT_REQUEST_PATH=${BULK_IMPORT_REQUEST_PATH}
BULK_IMPORT_LOAD_REQUEST=${BULK_IMPORT_LOAD_REQUEST}
BULK_IMPORT_SUMMARY_PATH=${BULK_IMPORT_SUMMARY_PATH}
QUERY_DATA_FILEPATH=${QUERY_DATA_FILEPATH}
QUERY_BALANCE_STRATEGY=${QUERY_BALANCE_STRATEGY}
QUERY_STREAMING=${QUERY_STREAMING}
QUERY_CORPUS_SIZE=${QUERY_CORPUS_SIZE}
QUERY_BATCH_SIZE=${QUERY_BATCH_CURRENT}
QUERY_CLIENTS_PER_PROXY=${QUERY_CLIENTS_PER_PROXY}
MIXED_RESULT_PATH=${MIXED_RESULT_PATH}
MIXED_INSERT_BATCH_SIZE=${MIXED_INSERT_BATCH_SIZE:-$INSERT_BATCH_CURRENT}
MIXED_QUERY_BATCH_SIZE=${MIXED_QUERY_BATCH_SIZE:-$QUERY_BATCH_CURRENT}
INSERT_MODE=${INSERT_MODE}
INSERT_OPS_PER_SEC=${INSERT_OPS_PER_SEC}
QUERY_MODE=${QUERY_MODE}
QUERY_OPS_PER_SEC=${QUERY_OPS_PER_SEC}
MIXED_CORPUS_SIZE=${MIXED_CORPUS_SIZE}
MIXED_DATA_FILEPATH=${MIXED_DATA_FILEPATH}
MIXED_QUERY_CLIENTS_PER_PROXY=${MIXED_QUERY_CLIENTS_PER_PROXY}
MIXED_INSERT_CLIENTS_PER_PROXY=${MIXED_INSERT_CLIENTS_PER_PROXY}
INSERT_START_ID=${INSERT_START_ID}
COLLECTION_NAME=${COLLECTION_NAME}
VECTOR_FIELD=${VECTOR_FIELD}
ID_FIELD=${ID_FIELD}
TOP_K=${TOP_K}
QUERY_EF_SEARCH=${QUERY_EF_SEARCH}
SEARCH_CONSISTENCY=${SEARCH_CONSISTENCY}
RPC_TIMEOUT=${RPC_TIMEOUT}
MIXED_INSERT_BATCH_MIN=${MIXED_INSERT_BATCH_MIN}
MIXED_INSERT_BATCH_MAX=${MIXED_INSERT_BATCH_MAX}
MIXED_QUERY_BATCH_MIN=${MIXED_QUERY_BATCH_MIN}
MIXED_QUERY_BATCH_MAX=${MIXED_QUERY_BATCH_MAX}
INSERT_BATCH_MIN=${INSERT_BATCH_MIN}
INSERT_BATCH_MAX=${INSERT_BATCH_MAX}
QUERY_BATCH_MIN=${QUERY_BATCH_MIN}
QUERY_BATCH_MAX=${QUERY_BATCH_MAX}
VECTOR_DIM=${VECTOR_DIM}
DISTANCE_METRIC=${DISTANCE_METRIC}
INIT_FLAT_INDEX=${INIT_FLAT_INDEX}
SHARDS=${SHARDS}
FLUSH_BEFORE_INDEX=${FLUSH_BEFORE_INDEX}
GPU_INDEX=${GPU_INDEX}
TRACING=${TRACING}
PERF=${PERF}
PERF_EVENTS=${PERF_EVENTS}
DEBUG=${DEBUG}
RESTORE_DIR=${RESTORE_DIR}
EXPECTED_CORPUS_SIZE=${EXPECTED_CORPUS_SIZE}
MINIO_MODE=${MINIO_MODE}
MINIO_MEDIUM=${MINIO_MEDIUM}
ETCD_MEDIUM=${ETCD_MEDIUM}
LOCAL_SHARED_STORAGE_PATH=${LOCAL_SHARED_STORAGE_PATH}
EOF

    if [[ "$MODE" == "DISTRIBUTED" ]]; then
        cat <<EOF
ETCD_MODE=${ETCD_MODE}
COORDINATOR_NODES=${COORDINATOR_NODES}
COORDINATOR_NODES_PER_CN=${COORDINATOR_NODES_PER_CN}
STREAMING_NODES=${STREAMING_NODES}
STREAMING_NODES_PER_CN=${STREAMING_NODES_PER_CN}
QUERY_NODES=${QUERY_NODES}
QUERY_NODES_PER_CN=${QUERY_NODES_PER_CN}
DATA_NODES=${DATA_NODES}
DATA_NODES_PER_CN=${DATA_NODES_PER_CN}
NUM_PROXIES=${NUM_PROXIES}
NUM_PROXIES_PER_CN=${NUM_PROXIES_PER_CN}
DML_CHANNELS=${DML_CHANNELS}
EOF
    else
        cat <<EOF
COORDINATOR_NODES=1
COORDINATOR_NODES_PER_CN=1
STREAMING_NODES=1
STREAMING_NODES_PER_CN=1
QUERY_NODES=1
QUERY_NODES_PER_CN=1
DATA_NODES=1
DATA_NODES_PER_CN=1
NUM_PROXIES=1
NUM_PROXIES_PER_CN=1
EOF
    fi
}

# Stage Milvus containers, launch scripts, clients, and task-specific helpers.
engine_copy_payload() {
    local target_dir="$1"

    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
        copy_engine_items "$ENGINE_DIR/goCode/multiClientOP" "$target_dir" "multiClientOP" "main.go"
        if [[ -f "$target_dir/main.go" ]]; then
            mv "$target_dir/main.go" "$target_dir/multiClient.go"
        fi
        copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" \
            "multi_client_summary.py" \
            "bulk_upload_import.py" \
            "bulk_upload_import_mc.py" \
            "replace_unified.py"
        mkdir -p "$target_dir/configs"
        copy_engine_items "$ENGINE_DIR/cpuMilvus/configs" "$target_dir/configs" "unified_milvus.yaml"

        if [[ "$TASK" == "MIXED" ]]; then
            copy_engine_items "$ENGINE_DIR/goCode/mixedRunner" "$target_dir" "mixedRunner" "main.go"
            if [[ -f "$target_dir/main.go" ]]; then
                mv "$target_dir/main.go" "$target_dir/mixed_main.go"
            fi
            copy_engine_items "$ENGINE_DIR/../qdrant/scripts" "$target_dir" "mixed_timeline.py"
        fi

        if [[ -z "$RESTORE_DIR" ]]; then
            copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" "setup_collection.py"
            if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
                copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" "index.py"
            fi
        else
            copy_engine_items "$ENGINE_DIR/utils" "$target_dir" "status.py"
        fi
    else
        if [[ "$MODE" == "STANDALONE" ]]; then
            copy_engine_items "$ENGINE_DIR/sifs" "$target_dir" "milvus.sif"
            copy_engine_items "$ENGINE_DIR/milvusSetup" "$target_dir" "standaloneLaunch.sh" "execute.sh"
        elif [[ "$MODE" == "DISTRIBUTED" ]]; then
            copy_engine_items "$ENGINE_DIR/sifs" "$target_dir" "milvus.sif" "etcd_v3.5.18.sif" "minio.sif"
            copy_engine_items "$ENGINE_DIR/milvusSetup" "$target_dir" \
                "execute.sh" \
                "launch_etcd.sh" \
                "launch_minio.sh" \
                "launch_milvus_part.sh"
        else
            echo "Unknown MODE: $MODE" >&2
            exit 1
        fi

        if [[ "$TRACING" == "True" ]]; then
            copy_engine_items "$ENGINE_DIR/sifs" "$target_dir" "otel-collector.sif"
            copy_engine_items "$ENGINE_DIR/utils" "$target_dir" "launch_otel.sh" "otel_config.yaml" "analyze_traces.py"
        fi

        copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" \
            "net_mapping.py" \
            "replace_unified.py" \
            "profile.py" \
            "poll.py" \
            "multi_client_summary.py" \
            "bulk_upload_import.py" \
            "bulk_upload_import_mc.py"

        copy_engine_items "$ENGINE_DIR/goCode/multiClientOP" "$target_dir" "multiClientOP" "main.go"
        if [[ -f "$target_dir/main.go" ]]; then
            mv "$target_dir/main.go" "$target_dir/multiClient.go"
        fi

        if [[ "$TASK" == "MIXED" ]]; then
            copy_engine_items "$ENGINE_DIR/goCode/mixedRunner" "$target_dir" "mixedRunner" "main.go"
            if [[ -f "$target_dir/main.go" ]]; then
                mv "$target_dir/main.go" "$target_dir/mixed_main.go"
            fi
            copy_engine_items "$ENGINE_DIR/../qdrant/scripts" "$target_dir" "mixed_timeline.py"
        fi

        if [[ -z "$RESTORE_DIR" ]]; then
            copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" "setup_collection.py"
            if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
                copy_engine_items "$ENGINE_DIR/generalPython" "$target_dir" "index.py"
            fi
        else
            copy_engine_items "$ENGINE_DIR/utils" "$target_dir" "status.py"
        fi
    fi
}
