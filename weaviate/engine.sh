#!/bin/bash

ENGINE_NAME="weaviate"

engine_set_defaults() {
    BASE_DIR="$(pwd)"

    WORKERS_PER_NODE=(4)
    QUERY_BATCH_SIZE=(256)
    UPLOAD_CLIENTS_PER_WORKER=(16)
    UPLOAD_BATCH_SIZE=(2048)

    TASK=""
    usePerf="false"
    CORPUS_SIZE=10000000
    UPLOAD_BALANCE_STRATEGY="WORKER_BALANCE"
    GPU_INDEX="false"

    DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy"
    QUERY_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy"
    VECTOR_DIM=2560
    DISTANCE_METRIC="COSINE"

    QUERY_WORKLOAD=100000
    QUERY_TOPK=10
    QUERY_EF=64

    WEAVIATE_CLIENT_BINARY="test"
    REQUIRES_DAOS="false"
}

engine_apply_overrides() {
    apply_array_override WORKERS_PER_NODE
    apply_array_override QUERY_BATCH_SIZE
    apply_array_override UPLOAD_CLIENTS_PER_WORKER
    apply_array_override UPLOAD_BATCH_SIZE

    apply_scalar_override BASE_DIR
    apply_scalar_override TASK
    apply_scalar_override usePerf
    apply_scalar_override CORPUS_SIZE
    apply_scalar_override UPLOAD_BALANCE_STRATEGY
    apply_scalar_override GPU_INDEX
    apply_scalar_override DATA_FILEPATH
    apply_scalar_override QUERY_FILEPATH
    apply_scalar_override VECTOR_DIM
    apply_scalar_override DISTANCE_METRIC
    apply_scalar_override QUERY_WORKLOAD
    apply_scalar_override QUERY_TOPK
    apply_scalar_override QUERY_EF
    apply_scalar_override WEAVIATE_CLIENT_BINARY
}

engine_validate_config() {
    [[ -n "${TASK:-}" ]] || {
        echo "Missing required Weaviate setting: TASK" >&2
        exit 1
    }

    case "$TASK" in
        insert|index|query_bs|query_core)
            ;;
        *)
            echo "Invalid Weaviate TASK='$TASK'." >&2
            exit 1
            ;;
    esac
}

engine_iterate_matrix() {
    local n
    local w
    local ucpw
    local qbs
    local ubs
    local c

    for n in "${NODES[@]}"; do
        for w in "${WORKERS_PER_NODE[@]}"; do
            for ucpw in "${UPLOAD_CLIENTS_PER_WORKER[@]}"; do
                for qbs in "${QUERY_BATCH_SIZE[@]}"; do
                    for ubs in "${UPLOAD_BATCH_SIZE[@]}"; do
                        for c in "${CORES[@]}"; do
                            echo "${n}|${w}|${ucpw}|${qbs}|${ubs}|${c}"
                        done
                    done
                done
            done
        done
    done
}

engine_load_combo() {
    IFS='|' read -r NODES_CURRENT WORKERS_PER_NODE_CURRENT UPLOAD_CLIENTS_CURRENT QUERY_BATCH_CURRENT UPLOAD_BATCH_CURRENT CORES_CURRENT <<< "$1"
    TOTAL_NODES=$((NODES_CURRENT + 1))
    JOB_NAME="${TASK}_${NODES_CURRENT}n_${WORKERS_PER_NODE_CURRENT}w_${CORES_CURRENT}c_q${QUERY_BATCH_CURRENT}"
}

engine_make_run_dir_name() {
    local timestamp
    timestamp="$(date +"%Y-%m-%d_%H_%M_%S")"

    case "$TASK" in
        insert)
            echo "${TASK}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_C${UPLOAD_CLIENTS_CURRENT}_uploadBS${UPLOAD_BATCH_CURRENT}_${timestamp}"
            ;;
        index)
            echo "${TASK}_pes2o_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_CS${CORPUS_SIZE}_${timestamp}"
            ;;
        query_bs|query_core)
            echo "${TASK}_pes2o_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_QBS${QUERY_BATCH_CURRENT}_Q${QUERY_WORKLOAD}_${timestamp}"
            ;;
    esac
}

engine_main_script_path() {
    echo "main.sh"
}

engine_emit_runtime_env() {
    cat <<EOF
NODES=${NODES_CURRENT}
WORKERS_PER_NODE=${WORKERS_PER_NODE_CURRENT}
TASK=${TASK}
VECTOR_DIM=${VECTOR_DIM}
DISTANCE_METRIC=${DISTANCE_METRIC}
STORAGE_MEDIUM=${STORAGE_MEDIUM}
CORPUS_SIZE=${CORPUS_SIZE}
USEPERF=${usePerf}
CORES=${CORES_CURRENT}
QUERY_BATCH_SIZE=${QUERY_BATCH_CURRENT}
UPLOAD_BATCH_SIZE=${UPLOAD_BATCH_CURRENT}
UPLOAD_CLIENTS_PER_WORKER=${UPLOAD_CLIENTS_CURRENT}
DATA_FILEPATH=${DATA_FILEPATH}
QUERY_FILEPATH=${QUERY_FILEPATH}
UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}
PLATFORM=${PLATFORM}
GPU_INDEX=${GPU_INDEX}
BASE_DIR=${BASE_DIR}
QUERY_WORKLOAD=${QUERY_WORKLOAD}
QUERY_TOPK=${QUERY_TOPK}
QUERY_EF=${QUERY_EF}
WEAVIATE_CLIENT_BINARY=${WEAVIATE_CLIENT_BINARY}
EOF
}

engine_copy_payload() {
    local target_dir="$1"
    copy_engine_items "$ENGINE_DIR/weaviateSetup" "$target_dir" \
        "launchWeaviateNode.sh" \
        "mapping.py"

    if [[ -f "$ENGINE_DIR/$WEAVIATE_CLIENT_BINARY" ]]; then
        copy_engine_items "$ENGINE_DIR" "$target_dir" "$WEAVIATE_CLIENT_BINARY"
    elif [[ -f "$ENGINE_DIR/goCode/test/$WEAVIATE_CLIENT_BINARY" ]]; then
        copy_engine_items "$ENGINE_DIR" "$target_dir" "goCode/test/$WEAVIATE_CLIENT_BINARY"
        mv "$target_dir/goCode/test/$WEAVIATE_CLIENT_BINARY" "$target_dir/$WEAVIATE_CLIENT_BINARY"
        rmdir "$target_dir/goCode/test" 2>/dev/null || true
        rmdir "$target_dir/goCode" 2>/dev/null || true
    fi

    if [[ -d "$ENGINE_DIR/perf" ]]; then
        copy_engine_items "$ENGINE_DIR" "$target_dir" "perf"
    fi
}
