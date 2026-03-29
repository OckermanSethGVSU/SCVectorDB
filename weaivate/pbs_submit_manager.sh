#!/bin/bash

set -euo pipefail

apply_scalar_override() {
    local var_name="$1"
    local override_name="${var_name}_OVERRIDE"
    local uppercase_var_name="${var_name^^}"
    local uppercase_override_name="${uppercase_var_name}_OVERRIDE"
    local override_value=""

    if [[ -n "${!override_name:-}" ]]; then
        override_value="${!override_name}"
    elif [[ -n "${!uppercase_override_name:-}" ]]; then
        override_value="${!uppercase_override_name}"
    fi

    if [[ -n "$override_value" ]]; then
        printf -v "$var_name" '%s' "$override_value"
    fi
}

apply_array_override() {
    local var_name="$1"
    local override_name="${var_name}_OVERRIDE"
    local uppercase_var_name="${var_name^^}"
    local uppercase_override_name="${uppercase_var_name}_OVERRIDE"
    local override_value=""

    if [[ -n "${!override_name:-}" ]]; then
        override_value="${!override_name}"
    elif [[ -n "${!uppercase_override_name:-}" ]]; then
        override_value="${!uppercase_override_name}"
    fi

    if [[ -n "$override_value" ]]; then
        read -r -a "$var_name" <<< "$override_value"
    fi
}

apply_overrides() {
    apply_array_override NODES
    apply_array_override WORKERS_PER_NODE
    apply_array_override CORES
    apply_array_override QUERY_BATCH_SIZE
    apply_array_override UPLOAD_CLIENTS_PER_WORKER
    apply_array_override UPLOAD_BATCH_SIZE

    apply_scalar_override WALLTIME
    apply_scalar_override queue
    apply_scalar_override TASK
    apply_scalar_override STORAGE_MEDIUM
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
    apply_scalar_override SNAPSHOT_DIR
    apply_scalar_override PLATFORM
    apply_scalar_override GO_CLIENT_SRC
    apply_scalar_override SAVE_SNAPSHOT
    apply_scalar_override RESTORE_SNAPSHOT
}

BASE_DIR="$(pwd)"

### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(4)

# For query_core sweep: edit CORES manually each run among:
# 1 2 4 8 16 32 64 128 208
CORES=(112)

# For query_bs sweep: edit QUERY_BATCH_SIZE manually each run among:
# 32 64 128 256 512 1024 2048 4096 8192
# For query_core sweep: edit QUERY_BATCH_SIZE manually each run among:
# 32 256 2048
QUERY_BATCH_SIZE=(256)

# PBS vars
WALLTIME="01:00:00"
queue="debug-scaling"   # debug for query sweeps

### Runtime variables ###
# TASK: [insert, index, query_bs, query_core]
TASK="insert"

# Keep these to preserve structure
UPLOAD_CLIENTS_PER_WORKER=(16)
UPLOAD_BATCH_SIZE=(2048)

STORAGE_MEDIUM="memory"   # [memory, DAOS, lustre, SSD]
usePerf="false"           # [true, false]
CORPUS_SIZE=10000000
UPLOAD_BALANCE_STRATEGY="WORKER_BALANCE"
GPU_INDEX="false"

# Dataset / vector settings
DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy"
QUERY_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy"
VECTOR_DIM=2560
DISTANCE_METRIC="COSINE"

# Query workload settings
QUERY_WORKLOAD=100000
QUERY_TOPK=10
QUERY_EF=64

# Build this once with TASK=index and SAVE_SNAPSHOT=true, then reuse it for query runs.
# SNAPSHOT_DIR="/flare/radix-io/songoh/SCVectorDB/weaivate/snapshots/pes2o"

PLATFORM="AURORA"

# Go client source directory
# GO_CLIENT_SRC="/flare/radix-io/songoh/weaviate/multi_node/go_client"

# For one-time snapshot creation only
SAVE_SNAPSHOT="false"   # set true only on the one indexing run used to create the snapshot
RESTORE_SNAPSHOT="true" # for query runs

apply_overrides

for num_nodes in "${NODES[@]}"; do
    for workers in "${WORKERS_PER_NODE[@]}"; do
        for UCPW in "${UPLOAD_CLIENTS_PER_WORKER[@]}"; do
            for query_bs in "${QUERY_BATCH_SIZE[@]}"; do
                for upload_bs in "${UPLOAD_BATCH_SIZE[@]}"; do
                    for numCores in "${CORES[@]}"; do

                        total_nodes=$((num_nodes + 1))   # +1 client node
                        DATE=$(date +"%Y-%m-%d_%H_%M_%S")
                        target_file="submit.sh"

                        if [[ "$TASK" == "insert" ]]; then
                            dir="${TASK}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_C${UCPW}_uploadBS${upload_bs}_${DATE}"
                        elif [[ "$TASK" == "index" ]]; then
                            dir="${TASK}_pes2o_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_CORES${numCores}_CS${CORPUS_SIZE}_${DATE}"
                        elif [[ "$TASK" == "query_bs" ]]; then
                            dir="${TASK}_pes2o_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_CORES${numCores}_QBS${query_bs}_Q${QUERY_WORKLOAD}_${DATE}"
                        elif [[ "$TASK" == "query_core" ]]; then
                            dir="${TASK}_pes2o_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_CORES${numCores}_QBS${query_bs}_Q${QUERY_WORKLOAD}_${DATE}"
                        else
                            echo "Unknown task: $TASK"
                            exit 1
                        fi

                        mkdir -p "$dir"
                        mkdir -p "$dir/"

                        rm -f "$target_file"

                        {
                            echo "#!/bin/bash -l"
                            echo "#PBS -N ${TASK}_${num_nodes}n_${workers}w_${numCores}c_q${query_bs}"
                            echo "#PBS -l select=${total_nodes}"
                            echo "#PBS -l place=scatter"
                            echo "#PBS -l walltime=${WALLTIME}"
                            echo "#PBS -q ${queue}"

                            if [[ "$PLATFORM" == "POLARIS" ]]; then
                                echo "#PBS -l filesystems=home:eagle"
                            elif [[ "$PLATFORM" == "AURORA" ]]; then
                                if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
                                    echo "#PBS -l filesystems=home:flare:daos_user_fs"
                                    echo "#PBS -l daos=daos_user"
                                else
                                    echo "#PBS -l filesystems=home:flare"
                                fi
                            fi

                            echo "#PBS -A radix-io"
                            echo "#PBS -o workflow.out"
                            echo "#PBS -e workflow.err"
                            echo
                            echo "NODES=${num_nodes}"
                            echo "WORKERS_PER_NODE=${workers}"
                            echo "myDIR=${dir}"
                            echo "TASK=${TASK}"
                            echo "VECTOR_DIM=${VECTOR_DIM}"
                            echo "DISTANCE_METRIC=${DISTANCE_METRIC}"
                            echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}"
                            echo "CORPUS_SIZE=${CORPUS_SIZE}"
                            echo "USEPERF=${usePerf}"
                            echo "CORES=${numCores}"
                            echo "QUERY_BATCH_SIZE=${query_bs}"
                            echo "UPLOAD_BATCH_SIZE=${upload_bs}"
                            echo "UPLOAD_CLIENTS_PER_WORKER=${UCPW}"
                            echo "DATA_FILEPATH=${DATA_FILEPATH}"
                            echo "QUERY_FILEPATH=${QUERY_FILEPATH}"
                            echo "UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}"
                            echo "PLATFORM=${PLATFORM}"
                            echo "GPU_INDEX=${GPU_INDEX}"
                            echo "BASE_DIR=${BASE_DIR}"
                            echo "QUERY_WORKLOAD=${QUERY_WORKLOAD}"
                            echo "QUERY_TOPK=${QUERY_TOPK}"
                            echo "QUERY_EF=${QUERY_EF}"
                            # echo "SNAPSHOT_DIR=${SNAPSHOT_DIR}"
                            # echo "SAVE_SNAPSHOT=${SAVE_SNAPSHOT}"
                            # echo "RESTORE_SNAPSHOT=${RESTORE_SNAPSHOT}"
                            echo

                            cat main.sh
                        } > "$target_file"

                        cp weaviateSetup/launchWeaviateNode.sh "$dir/"
                        cp weaviateSetup/mapping.py "$dir/"
                        # cp weaviate_latest.sif "$dir/"
                        cp ./goCode/test/test "$dir/"
                        if [[ -d perf ]]; then
                            cp -r perf "$dir/"
                        fi

                        # cp "${GO_CLIENT_SRC}/index_pes2o.go" "$dir/go_client/"
                        # cp "${GO_CLIENT_SRC}/index_pes2o_ef64.go" "$dir/go_client/"
                        # cp "${GO_CLIENT_SRC}/index_pes2o_ef64" "$dir/go_client/" 2>/dev/null || true
                        
                        # cp "${GO_CLIENT_SRC}/insert_nclients.go" "$dir/go_client/"
                        # cp "${GO_CLIENT_SRC}/query.go" "$dir/go_client/"
                        # cp "${GO_CLIENT_SRC}/go.mod" "$dir/go_client/" 2>/dev/null || true
                        # cp "${GO_CLIENT_SRC}/go.sum" "$dir/go_client/" 2>/dev/null || true

                        # cp "${GO_CLIENT_SRC}/index_pes2o" "$dir/go_client/" 2>/dev/null || true
                        # cp "${GO_CLIENT_SRC}/insert_nclients" "$dir/go_client/" 2>/dev/null || true
                        # cp "${GO_CLIENT_SRC}/query_yandex" "$dir/go_client/" 2>/dev/null || true

                        # chmod +x "$dir/go_client/index_pes2o" "$dir/go_client/insert_nclients" "$dir/go_client/query_yandex" 2>/dev/null || true
                        # chmod +x "$dir/go_client/index_pes2o_ef64" 2>/dev/null || true
                        mv "$target_file" "$dir/"
                        # chmod -R g+w "$dir"
                        # chmod +x "$dir/submit.sh" "$dir/launchWeaviateNode.sh"

                        cd "$dir"
                        echo "[SUBMIT] $(pwd)/submit.sh"
                        # qsub submit.sh
                        sleep 5
                        cd "$BASE_DIR"

                    done
                done
            done
        done
    done
done
