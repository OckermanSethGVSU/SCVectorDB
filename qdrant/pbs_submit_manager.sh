#!/bin/bash

print_config_summary() {
    echo "            Experiment Configuration"
    echo "================================================="
    echo "Platform:                 $PLATFORM"
    echo "Task:                     $TASK"
    echo "Storage Medium:           $STORAGE_MEDIUM"
    echo "Perf:                     $PERF"
    echo "Vector Dim:               $VECTOR_DIM"
    echo "Distance Metric:          $DISTANCE_METRIC"
    echo "GPU Index:                $GPU_INDEX"
    echo "Qdrant Executable:        $QDRANT_EXECUTABLE"

    if [[ -n "$RESTORE_DIR" ]]; then
        echo "Restore Dir:              $RESTORE_DIR"
        echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
    else
        echo "Insert File:              $INSERT_FILEPATH"
        echo "Insert Corpus Size:       $INSERT_CORPUS_SIZE"
        echo "Insert Batch Size:        ${INSERT_BATCH_SIZE[*]}"
        echo "Insert Clients/Worker:    $INSERT_CLIENTS_PER_WORKER"
        echo "Insert Balance:           $INSERT_BALANCE_STRATEGY"
    fi

    if [[ "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
        echo "Query File:               $QUERY_FILEPATH"
        echo "Query Corpus Size:        $QUERY_CORPUS_SIZE"
        echo "Query Batch Size:         ${QUERY_BATCH_SIZE[*]}"
        echo "Query Clients/Worker:     $QUERY_CLIENTS_PER_WORKER"
        echo "Query Balance:            $QUERY_BALANCE_STRATEGY"
    elif [[ "$TASK" == "INDEX" ]]; then
        if [[ -n "$RESTORE_DIR" ]]; then
            echo "Index Corpus Size:        $EXPECTED_CORPUS_SIZE"
        else
            echo "Index Corpus Size:        $INSERT_CORPUS_SIZE"
        fi
    fi
}

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
    apply_array_override INSERT_BATCH_SIZE
    apply_array_override QUERY_BATCH_SIZE

    apply_scalar_override WALLTIME
    apply_scalar_override queue
    apply_scalar_override TASK
    apply_scalar_override RUN_MODE
    apply_scalar_override STORAGE_MEDIUM
    apply_scalar_override PERF
    apply_scalar_override VECTOR_DIM
    apply_scalar_override DISTANCE_METRIC
    apply_scalar_override GPU_INDEX
    apply_scalar_override INSERT_FILEPATH
    apply_scalar_override INSERT_CORPUS_SIZE
    apply_scalar_override INSERT_BALANCE_STRATEGY
    apply_scalar_override INSERT_CLIENTS_PER_WORKER
    apply_scalar_override QUERY_FILEPATH
    apply_scalar_override QUERY_CORPUS_SIZE
    apply_scalar_override QUERY_BALANCE_STRATEGY
    apply_scalar_override QUERY_CLIENTS_PER_WORKER
    apply_scalar_override PLATFORM
    apply_scalar_override QDRANT_EXECUTABLE
    apply_scalar_override RESTORE_DIR
    apply_scalar_override EXPECTED_CORPUS_SIZE
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# if ! "$SCRIPT_DIR/check_dependencies.sh" --missing-only; then
#     exit 1
# fi

### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(1)
CORES=(128)
INSERT_BATCH_SIZE=(256)
QUERY_BATCH_SIZE=(32)


# PBS Vars
WALLTIME="01:00:00"
queue="debug" # [preemptable, debug, debug-scaling, prod, capacity]


### Runtime variables ###
TASK="QUERY" # [INSERT, INDEX, QUERY, MIXED]
RUN_MODE="local" # [PBS, local]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
PERF="NONE" # [NONE, STAT, TRACE]
VECTOR_DIM=200
DISTANCE_METRIC="IP" # [IP, COSINE, L2]
GPU_INDEX=False

# Aurora
    # 10 million 
    #    HPC-Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
    #    Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy
# Polaris 
    # 10 million 
    #     HPC-Pes2o: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy
    #     Yandex: /eagle/projects/argonne_tpc/sockerman/big-ann-benchmarks/benchmark/data/yandex10Mil/Yandex10M.npy

# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/Yandex10M.npy
INSERT_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/Yandex10M.npy"
INSERT_CORPUS_SIZE=1000000 # total data to insert
INSERT_BALANCE_STRATEGY="WORKER_BALANCE" # [NO_BALANCE, WORKER_BALANCE]
# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
INSERT_CLIENTS_PER_WORKER=1


### Query ### 
# Aurora
    # * Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy
    # * Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy
# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy
QUERY_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy"

# 22723, 100000
QUERY_CORPUS_SIZE=100000 # total data to QUERY
QUERY_BALANCE_STRATEGY="NO_BALANCE" # [NO_BALANCE, WORKER_BALANCE]
# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
QUERY_CLIENTS_PER_WORKER=1




### Mixed Insert/Query
RESULT_PATH="mixed_logs"
INSERT_MODE="MAX" # ["MAX", "RATE"]
INSERT_OPS_PER_SEC="" # Only needed if we are using INSERT_MODE="RATE"
QUERY_MODE="MAX" # ["MAX", "RATE"]
QUERY_OPS_PER_SEC="" # Only needed if we are using QUERY_MODE="RATE"



# Optional but useful:
COLLECTION_NAME=""
TOP_K=""
QUERY_EF_SEARCH=""
RPC_TIMEOUT=""
# only relevant if you want random batch sizes 
INSERT_BATCH_MIN=""
INSERT_BATCH_MAX=""
QUERY_BATCH_MIN=""
QUERY_BATCH_MAX=""


PLATFORM="AURORA" # [POLARIS, AURORA]


QDRANT_EXECUTABLE="qdrant" # [qdrant, qdrantInsertTracing]
# RESTORE_DIR="/lus/flare/projects/radix-io/sockerman/temp/qdrant/10Mil/yandex/"
RESTORE_DIR=""
# RESTORE_DIR=""
EXPECTED_CORPUS_SIZE=10000000

apply_overrides

#  Automatically compute much data have you inserted before you run the mixed insert/query experiments; only change if you are certain
if [[ -z "$INSERT_START_ID" ]]; then
    if [[ -n "$RESTORE_DIR" ]]; then
        INSERT_START_ID=$EXPECTED_CORPUS_SIZE
    else
        INSERT_START_ID=$INSERT_CORPUS_SIZE
    fi
fi

print_config_summary

for num_nodes in "${NODES[@]}"
do
    for workers in "${WORKERS_PER_NODE[@]}" 
    do
        for query_bs in "${QUERY_BATCH_SIZE[@]}" 
        do

            for upload_bs in "${INSERT_BATCH_SIZE[@]}"

            do 
                for numCores in "${CORES[@]}"
                do 
                    total_nodes=$((num_nodes + 1))
                    DATE=$(date +"%Y-%m-%d_%T")
                    target_file="submit.sh"
                    
                    
                    echo "#!/bin/bash" >> $target_file
                    echo "#PBS -l select=${total_nodes}" >> $target_file
                    echo "#PBS -l place=scatter" >> $target_file
                    echo "#PBS -l walltime=${WALLTIME}" >> $target_file
                    echo "#PBS -q $queue" >> $target_file
                    
                    if [[ "$PLATFORM" == "POLARIS" ]]; then
                        echo "#PBS -l filesystems=home:eagle" >> $target_file    
                    elif [[ "$PLATFORM" == "AURORA" ]]; then
                        if [[ "$STORAGE_MEDIUM" == "DAOS" ]]; then
                            echo "#PBS -l filesystems=home:flare:daos_user_fs" >> $target_file
                            echo "#PBS -l daos=daos_user" >> $target_file
                        else
                            echo "#PBS -l filesystems=home:flare" >> $target_file
                        fi
                    fi

                    

                    echo "#PBS -A radix-io" >> $target_file
                    echo "#PBS -o workflow.out" >> $target_file
                    echo "#PBS -e workflow.out" >> $target_file


                    echo "" >> $target_file

                    echo "NODES=${num_nodes}" >> $target_file
                    echo "WORKERS_PER_NODE=${workers}" >> $target_file

                    
                    DATE=$(date +"%Y-%m-%d_%H_%M_%S")
                    
                    if [[ "$TASK" == "INSERT" ]]; then
                        dir="${TASK}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_C${INSERT_CLIENTS_PER_WORKER}_uploadBS${upload_bs}_${DATE}"
                    elif [[ "$TASK" == "INDEX" ]]; then
                        dir="${TASK}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_NP${workers}_CS${INSERT_CORPUS_SIZE}_${DATE}"
                    elif [[ "$TASK" == "QUERY" ]]; then    
                        if [[ -n "$RESTORE_DIR" ]]; then
                            corpus_size_for_dir=$EXPECTED_CORPUS_SIZE
                        else
                            corpus_size_for_dir=$INSERT_CORPUS_SIZE
                        fi
                        dir="${TASK}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_NP${workers}_BS${query_bs}_CS${corpus_size_for_dir}_${DATE}"
                    elif [[ "$TASK" == "MIXED" ]]; then
                        
                        if [[ -n "$RESTORE_DIR" ]]; then
                            corpus_size_for_dir=$EXPECTED_CORPUS_SIZE
                        else
                            corpus_size_for_dir=$INSERT_CORPUS_SIZE
                        fi

                        dir="${TASK}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_NP${workers}_IBS${upload_bs}_QBS${query_bs}_CS${corpus_size_for_dir}_${DATE}"
                    else
                            echo "Unknown task: $TASK"
                            exit
                        fi

                    echo "myDIR=${dir}" >> $target_file
                
                    echo "PLATFORM=${PLATFORM}" >> $target_file
                    echo "CORES=${numCores}" >> $target_file
                    echo "TASK=${TASK}" >> $target_file
                    echo "RUN_MODE=${RUN_MODE}" >> $target_file
                    echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                    
                    echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file
                    echo "DISTANCE_METRIC=${DISTANCE_METRIC}" >> $target_file
                    echo "PERF=${PERF}" >> $target_file
                    echo "GPU_INDEX=${GPU_INDEX}" >> $target_file
                    
                    echo "INSERT_BATCH_SIZE=${upload_bs}" >> $target_file
                    echo "INSERT_FILEPATH=${INSERT_FILEPATH}" >> $target_file
                    echo "INSERT_CORPUS_SIZE=${INSERT_CORPUS_SIZE}" >> $target_file
                    echo "INSERT_CLIENTS_PER_WORKER=${INSERT_CLIENTS_PER_WORKER}" >> $target_file
                    echo "INSERT_BALANCE_STRATEGY=${INSERT_BALANCE_STRATEGY}" >> $target_file
                    
                    
                    echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                    echo "QUERY_FILEPATH=${QUERY_FILEPATH}" >> $target_file
                    echo "QUERY_CORPUS_SIZE=${QUERY_CORPUS_SIZE}" >> $target_file
                    echo "QUERY_CLIENTS_PER_WORKER=${QUERY_CLIENTS_PER_WORKER}" >> $target_file
                    echo "QUERY_BALANCE_STRATEGY=${QUERY_BALANCE_STRATEGY}" >> $target_file
                    
        
                    echo "RESULT_PATH=${RESULT_PATH}" >> $target_file
                    echo "INSERT_MODE=${INSERT_MODE}" >> $target_file
                    echo "INSERT_OPS_PER_SEC=${INSERT_OPS_PER_SEC}" >> $target_file
	                    echo "INSERT_START_ID=${INSERT_START_ID}" >> $target_file
                    echo "QUERY_MODE=${QUERY_MODE}" >> $target_file
                    echo "QUERY_OPS_PER_SEC=${QUERY_OPS_PER_SEC}" >> $target_file
                    echo "COLLECTION_NAME=${COLLECTION_NAME}" >> $target_file
                    echo "TOP_K=${TOP_K}" >> $target_file
                    echo "QUERY_EF_SEARCH=${QUERY_EF_SEARCH}" >> $target_file
                    echo "RPC_TIMEOUT=${RPC_TIMEOUT}" >> $target_file
                    echo "INSERT_BATCH_MIN=${INSERT_BATCH_MIN}" >> $target_file
                    echo "INSERT_BATCH_MAX=${INSERT_BATCH_MAX}" >> $target_file
                    echo "QUERY_BATCH_MIN=${QUERY_BATCH_MIN}" >> $target_file
                    echo "QUERY_BATCH_MAX=${QUERY_BATCH_MAX}" >> $target_file
                    
                            
                    echo "QDRANT_EXECUTABLE=${QDRANT_EXECUTABLE}" >> $target_file
                    echo "RESTORE_DIR=${RESTORE_DIR}" >> $target_file
                    echo "EXPECTED_CORPUS_SIZE=${EXPECTED_CORPUS_SIZE}" >> $target_file

                    echo "" >> $target_file
	                    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
	                        cat local_main.sh >> $target_file
	                    else
	                        cat main.sh >> $target_file
	                    fi



	                    mkdir -p $dir

	                    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
	                        mkdir $dir/rustSrc
	                        cp ./rustCode/multiClientOP/multiClientOP $dir/
	                        cp ./rustCode/multiClientOP/src/main.rs $dir/rustSrc/main.rs
	                        cp ./rustCode/mixedRunner/target/release/mixedrunner $dir/
	                        cp ./rustCode/mixedRunner/src/main.rs $dir/rustSrc/mixed_main.rs
	                    else
	                        # copy in Qdrant files
	                        cp qdrant.sif $dir/
	                        cp qdrantSetup/launchQdrantNode.sh $dir/
	                        cp qdrantSetup/launch.sh $dir/

	                        if [[ "$QDRANT_EXECUTABLE" != "" ]]; then
	                            cp qdrantBuilds/${QDRANT_EXECUTABLE} $dir/qdrant
	                        fi
	                        
	                        # copy in general util files
	                        cp generalPython/profile.py $dir/
	                        cp generalPython/gen_dirs.py $dir/
	                        cp generalPython/mapping.py $dir/
	                        cp -r perf/ $dir/
	                
	                        # copy in rust client to use
	                        mkdir $dir/rustSrc
	                        cp ./rustCode/multiClientOP/multiClientOP $dir/
	                        cp ./rustCode/multiClientOP/src/main.rs $dir/rustSrc/main.rs
	                        cp ./rustCode/mixedRunner/target/release/mixedrunner $dir/
	                        cp ./rustCode/mixedRunner/src/main.rs $dir/rustSrc/mixed_main.rs
	                    fi
	                    
                    if [ -n "$RESTORE_DIR" ]; then
                        cp generalPython/fix_peer_id.py $dir/
                        cp generalPython/status.py $dir/
                    fi 

                    cp generalPython/multi_client_summary.py $dir/

                    if [ -z "$RESTORE_DIR" ]; then
                        cp generalPython/configureTopo.py $dir/
                    fi

                    if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" ]]; then
                        cp generalPython/index.py $dir/
                    fi
                    
          

                    mv $target_file $dir

                    chmod -R g+w $dir
                    cd $dir

	                    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
                            echo "Created $dir for testing"
	                    else
	                        qsub_output="$(qsub "$target_file")" || exit $?
	                        echo "$qsub_output"
	                        sleep 5
	                    fi
                    cd .. 
                done
            done
        done
    done
done
