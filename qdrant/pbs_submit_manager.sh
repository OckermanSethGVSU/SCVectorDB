#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/utils/utils.sh"

# if ! "$SCRIPT_DIR/check_dependencies.sh" --missing-only; then
#     exit 1
# fi

### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(1)
CORES=(112)
INSERT_BATCH_SIZE=(512)
QUERY_BATCH_SIZE=(32)


# PBS Vars
WALLTIME="03:00:00"
queue="debug-scaling" # [preemptable, debug, debug-scaling, prod, capacity]


### Runtime variables ###
TASK="QUERY" # [INSERT, INDEX, QUERY, MIXED]
RUN_MODE="PBS" # [PBS, local]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
PERF="NONE" # [NONE, STAT, TRACE]
# topdown-be-bound,topdown-mem-bound,topdown-retiring,topdown-fe-bound,topdown-bad-spec
# cycles,instructions,cache-references,cache-misses,LLC-load-misses
PERF_EVENTS="topdown-be-bound,topdown-mem-bound,topdown-retiring,topdown-fe-bound,topdown-bad-spec" # comma-separated perf stat event list override; only used when PERF=STAT
VECTOR_DIM=200
DISTANCE_METRIC="IP" # [IP, COSINE, L2]
GPU_INDEX=False

# Aurora
    # 10 million 
    #    HPC-Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
    #    Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy
    # All Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/mergedData/embeddings_merged.npy
    # All Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/yandex1B.npy

# Polaris 
    # 10 million 
    #     HPC-Pes2o: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy
    #     Yandex: /eagle/projects/argonne_tpc/sockerman/big-ann-benchmarks/benchmark/data/yandex10Mil/Yandex10M.npy

# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/Yandex10M.npy
INSERT_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"

# 88453763, 1000000000
INSERT_CORPUS_SIZE=5970464 # total data to insert
INSERT_BALANCE_STRATEGY="WORKER_BALANCE" # [NO_BALANCE, WORKER_BALANCE]
# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
INSERT_CLIENTS_PER_WORKER=1
INSERT_STREAMING="False"


### Query ### 
# Aurora
    # * Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy
    # * Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy
# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy
QUERY_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy"

# 22723, 100000
QUERY_CORPUS_SIZE=100000 # total data to QUERY
QUERY_BALANCE_STRATEGY="NO_BALANCE" # [NO_BALANCE, WORKER_BALANCE]
# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
TOTAL_QUERY_CLIENTS=1
QUERY_CLIENTS_PER_WORKER=1
QUERY_STREAMING=""


### Mixed Insert/Query
RESULT_PATH="mixed_logs"
INSERT_MODE="MAX" # ["MAX", "RATE"]
INSERT_OPS_PER_SEC="" # Only needed if we are using INSERT_MODE="RATE"
QUERY_MODE="MAX" # ["MAX", "RATE"]
QUERY_OPS_PER_SEC="" # Only needed if we are using QUERY_MODE="RATE"
MIXED_CORPUS_SIZE=1000
MIXED_DATA_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy"
MIXED_QUERY_CLIENTS_PER_WORKER=1
MIXED_INSERT_CLIENTS_PER_WORKER=1

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


QDRANT_EXECUTABLE="qdrant" # [qdrant, qdrantInsertTracing,qdrantQueryTrace]
INSERT_TRACE=""
QUERY_TRACE=""
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
                    echo "QDRANT_EXECUTABLE=${QDRANT_EXECUTABLE}" >> $target_file
                    echo "INSERT_TRACE=${INSERT_TRACE}" >> $target_file
                    echo "QUERY_TRACE=${QUERY_TRACE}" >> $target_file
                    echo "CORES=${numCores}" >> $target_file
                    echo "TASK=${TASK}" >> $target_file
                    echo "RUN_MODE=${RUN_MODE}" >> $target_file
                    echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                    
                    echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file
                    echo "DISTANCE_METRIC=${DISTANCE_METRIC}" >> $target_file
                    echo "PERF=${PERF}" >> $target_file
                    echo "PERF_EVENTS=${PERF_EVENTS}" >> $target_file
                    echo "GPU_INDEX=${GPU_INDEX}" >> $target_file
                    
                    echo "INSERT_BATCH_SIZE=${upload_bs}" >> $target_file
                    echo "INSERT_FILEPATH=${INSERT_FILEPATH}" >> $target_file
                    echo "INSERT_CORPUS_SIZE=${INSERT_CORPUS_SIZE}" >> $target_file
                    echo "INSERT_CLIENTS_PER_WORKER=${INSERT_CLIENTS_PER_WORKER}" >> $target_file
                    echo "INSERT_BALANCE_STRATEGY=${INSERT_BALANCE_STRATEGY}" >> $target_file
                    echo "INSERT_STREAMING=${INSERT_STREAMING}" >> $target_file
                    
                    
                    echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                    echo "QUERY_FILEPATH=${QUERY_FILEPATH}" >> $target_file
                    echo "QUERY_CORPUS_SIZE=${QUERY_CORPUS_SIZE}" >> $target_file
                    echo "QUERY_CLIENTS_PER_WORKER=${QUERY_CLIENTS_PER_WORKER}" >> $target_file
                    echo "TOTAL_QUERY_CLIENTS=${TOTAL_QUERY_CLIENTS}" >> $target_file
                    echo "QUERY_BALANCE_STRATEGY=${QUERY_BALANCE_STRATEGY}" >> $target_file
                    echo "QUERY_STREAMING=${QUERY_STREAMING}" >> $target_file
                    
                    
                    ## Mixed vars
                    echo "INSERT_MODE=${INSERT_MODE}" >> $target_file
                    echo "INSERT_OPS_PER_SEC=${INSERT_OPS_PER_SEC}" >> $target_file
                    echo "MIXED_INSERT_CLIENTS_PER_WORKER=${MIXED_INSERT_CLIENTS_PER_WORKER}" >> $target_file
                    echo "INSERT_START_ID=${INSERT_START_ID}" >> $target_file
                    echo "INSERT_BATCH_MIN=${INSERT_BATCH_MIN}" >> $target_file
                    echo "INSERT_BATCH_MAX=${INSERT_BATCH_MAX}" >> $target_file
                    
                    echo "QUERY_MODE=${QUERY_MODE}" >> $target_file
                    echo "QUERY_OPS_PER_SEC=${QUERY_OPS_PER_SEC}" >> $target_file
                    echo "MIXED_QUERY_CLIENTS_PER_WORKER=${MIXED_QUERY_CLIENTS_PER_WORKER}" >> $target_file
                    echo "QUERY_EF_SEARCH=${QUERY_EF_SEARCH}" >> $target_file
                    echo "QUERY_BATCH_MIN=${QUERY_BATCH_MIN}" >> $target_file
                    echo "QUERY_BATCH_MAX=${QUERY_BATCH_MAX}" >> $target_file
                    
                    echo "MIXED_CORPUS_SIZE=${MIXED_CORPUS_SIZE}" >> $target_file
                    echo "MIXED_DATA_FILEPATH=${MIXED_DATA_FILEPATH}" >> $target_file
                    echo "RESULT_PATH=${RESULT_PATH}" >> $target_file
                    echo "COLLECTION_NAME=${COLLECTION_NAME}" >> $target_file
                    echo "TOP_K=${TOP_K}" >> $target_file
                    echo "RPC_TIMEOUT=${RPC_TIMEOUT}" >> $target_file
                                                
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

                    if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
                        cp generalPython/index.py $dir/
                    fi

                    if [[ "$TASK" == "MIXED" ]]; then
                        cp generalPython/mixed_timeline.py $dir/
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
