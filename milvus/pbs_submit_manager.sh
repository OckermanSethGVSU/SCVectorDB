#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/utils/utils.sh"

### Allocation Variables ###
NODES=(1)
CORES=(112)


# PBS Vars
WALLTIME="01:00:00"
queue=debug-scaling # [preemptable, debug, debug-scaling, prod,capacity]

### Platform/DIR Specific Variables ###
# Aurora: /lus/flare/projects/radix-io/sockerman/milvusEnv/
# Polaris: /eagle/projects/radix-io/sockerman/vectorEval/milvus/multiNode/env/
ENV_PATH=/lus/flare/projects/radix-io/sockerman/milvusEnv/
MILVUS_BUILD_DIR="cpuMilvus" # Name of the directory with your build: traceMilvus, cpuMilvus
MILVUS_CONFIG_DIR="cpuMilvus" # If you have a specfic config, the path to the dir containing the yaml file
PLATFORM="AURORA" # [POLARIS, AURORA]

### General runtime variables ###
TASK="IMPORT" # [INSERT, IMPORT, INDEX, QUERY, MIXED]
RUN_MODE="local" # [PBS, local]
MODE="STANDALONE" # [DISTRIBUTED, STANDALONE]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
PERF="NONE" # [NONE, STAT, RECORD]
WAL="woodpecker" # [woodpecker, default]
GPU_INDEX="False" # [True, False]
TRACING="False" 
DEBUG="False" # [True, False]
BASE_DIR="$(pwd)"
MINIO_MODE="single" # standalone: [off, single], distributed: [single, stripped]
MINIO_MEDIUM="lustre" # [lustre] (can be memory if running single) - DAOS is broken


# 
### Insertion Variables ### 
INSERT_CORPUS_SIZE=100000 # total data to insert
INSERT_CLIENTS_PER_PROXY=32
INSERT_BALANCE_STRATEGY="WORKER" # [NONE, WORKER]
INSERT_STREAMING="True" # [True, False]
# Aurora
    # 10 million 
    #    HPC-Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
    #    Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy
                # /lus/flare/projects/AuroraGPT/sockerman/text2image1B/10M_part<1/2>.npy
                # /lus/flare/projects/AuroraGPT/sockerman/text2image1B/yandex1B.npy 
# Polaris 
    # 10 million 
    #     HPC-Pes2o: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy
    #     Yandex: /eagle/projects/argonne_tpc/sockerman/big-ann-benchmarks/benchmark/data/yandex10Mil/Yandex10M.npy
# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/Yandex10M.npy
# INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"
# INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/10M_part1.npy"
INSERT_DATA_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy"
# INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
# best batch for 32 clients: 128
INSERT_BATCH_SIZE=(512)
IMPORT_PROCESSES=2 # If you are doing a bulk import


# Index Variables
VECTOR_DIM=200
# VECTOR_DIM=2560
DISTANCE_METRIC="IP" # [IP, COSINE, L2]
INIT_FLAT_INDEX="FALSE" # [TRUE, FALSE]


### QUERY Variables ###
# QUERY_CORPUS_SIZE=22723  # queries
QUERY_CORPUS_SIZE=22723  # queries
QUERY_CLIENTS_PER_PROXY=1
QUERY_BALANCE_STRATEGY="NONE" # [NONE, WORKER]
QUERY_STREAMING="False" # [True, False]
QUERY_BATCH_SIZE=(32)

# Aurora
    # * Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy
    # * Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy
# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy
QUERY_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy"
# QUERY_DATA_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy"
# QUERY_DATA_FILEPATH="/home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy"


# Mixed Insert/Query Variables
INSERT_MODE="max" # [max, rate]
INSERT_OPS_PER_SEC=""
MIXED_INSERT_BATCH_SIZE=32

QUERY_MODE="max" # [max, rate]
QUERY_OPS_PER_SEC=""
MIXED_QUERY_BATCH_SIZE=32

MIXED_RESULT_PATH="mixed_logs"
MIXED_CORPUS_SIZE=1000000
MIXED_QUERY_CLIENTS_PER_PROXY=1
MIXED_INSERT_CLIENTS_PER_PROXY=1
# MIXED_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/10M_part2.npy"
MIXED_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/10M_part2.npy"

# Optional but useful
COLLECTION_NAME=""
VECTOR_FIELD=""
ID_FIELD=""
TOP_K=""
QUERY_EF_SEARCH=""
SEARCH_CONSISTENCY=""
RPC_TIMEOUT=""

# only relevant if you want random batch sizes 
MIXED_INSERT_BATCH_MIN=""
MIXED_INSERT_BATCH_MAX=""
MIXED_QUERY_BATCH_MIN=""
MIXED_QUERY_BATCH_MAX=""

# Polaris: TODO
# Aurora: 
#   Yandex: /lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/yandex
#   pes2o: /lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/pes2o
# RESTORE_DIR="/lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/yandex"
# RESTORE_DIR="/lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/pes2o"
RESTORE_DIR=""
EXPECTED_CORPUS_SIZE=10000000




### Distributed Variables ###
ETCD_MODE="replicated" # [single, replicated]
STREAMING_NODES=1
STREAMING_NODES_PER_CN=1

QUERY_NODES=1
QUERY_NODES_PER_CN=1

DATA_NODES=1
DATA_NODES_PER_CN=1

COORDINATOR_NODES=1
COORDINATOR_NODES_PER_CN=1

NUM_PROXIES=1
NUM_PROXIES_PER_CN=1
DML_CHANNELS=16 # controls DML channels on startup -> defaults to 16 if not set

apply_overrides

if [[ -z "$MINIO_MODE" ]]; then
    if [[ "$MODE" == "DISTRIBUTED" ]]; then
        MINIO_MODE="stripped"
    else
        MINIO_MODE="off"
    fi
fi

compute_insert_start_id() {
    if [[ -n "$INSERT_START_ID" ]]; then
        printf '%s\n' "$INSERT_START_ID"
    elif [[ -n "$RESTORE_DIR" ]]; then
        printf '%s\n' "$EXPECTED_CORPUS_SIZE"
    else
        printf '%s\n' "$INSERT_CORPUS_SIZE"
    fi
}

INSERT_START_ID="$(compute_insert_start_id)"


print_config_summary


for num_nodes in "${NODES[@]}"
do
    for query_bs in "${QUERY_BATCH_SIZE[@]}" 
    do

        for upload_bs in "${INSERT_BATCH_SIZE[@]}"

        do 
            for numCores in "${CORES[@]}"
            do 

                # add one to run client on
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
                    if [[ "$STORAGE_MEDIUM" == "DAOS" || ( "$MODE" == "DISTRIBUTED" && "$MINIO_MEDIUM" == "DAOS" ) ]]; then
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
                
                
                DATE=$(date +"%Y-%m-%d_%H_%M_%S")
                if [[ "$TASK" == "INSERT" ]]; then

                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_CPP${INSERT_CLIENTS_PER_PROXY}_uploadBS${INSERT_bs}_SN${STREAMING_NODES}_SPCN${STREAMING_NODES_PER_CN}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_CPP${INSERT_CLIENTS_PER_PROXY}_uploadBS${INSERT_bs}_${DATE}"
                    fi
                elif [[ "$TASK" == "IMPORT" ]]; then
                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_P${IMPORT_PROCESSES}_BS${upload_bs}_CS${INSERT_CORPUS_SIZE}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_P${IMPORT_PROCESSES}_BS${upload_bs}_CS${INSERT_CORPUS_SIZE}_${DATE}"
                    fi
                elif [[ "$TASK" == "INDEX" ]]; then
                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_${INSERT_CORPUS_SIZE}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_${INSERT_CORPUS_SIZE}_${DATE}"
                    fi
                elif [[ "$TASK" == "QUERY" ]]; then
                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_${query_bs}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_${query_bs}_${DATE}"
                    fi
                elif [[ "$TASK" == "MIXED" ]]; then
                    if [[ -n "$RESTORE_DIR" ]]; then
                        corpus_size_for_dir=$EXPECTED_CORPUS_SIZE
                    else
                        corpus_size_for_dir=$INSERT_CORPUS_SIZE
                    fi

                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_IBS${upload_bs}_QBS${query_bs}_CS${corpus_size_for_dir}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_CORES${numCores}_N${num_nodes}_IBS${upload_bs}_QBS${query_bs}_CS${corpus_size_for_dir}_${DATE}"
                    fi
                else
                    echo "Unknown task: $TASK: valid options include INSERT, IMPORT, INDEX, QUERY, MIXED"
                    exit
                fi

                echo "myDIR=${dir}" >> $target_file
                echo "PLATFORM=${PLATFORM}" >> $target_file
                echo "BASE_DIR=${BASE_DIR}" >> $target_file
                echo "ENV_PATH=${ENV_PATH}" >> $target_file
                echo "MILVUS_BUILD_DIR=${MILVUS_BUILD_DIR}" >> $target_file
                echo "MILVUS_CONFIG_DIR=${MILVUS_CONFIG_DIR}" >> $target_file
                echo "TASK=${TASK}" >> $target_file
                echo "RUN_MODE=${RUN_MODE}" >> $target_file
                echo "MODE=${MODE}" >> $target_file
                echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                echo "CORES=${numCores}" >> $target_file
                echo "WAL=${WAL}" >> $target_file

                echo "INSERT_DATA_FILEPATH=${INSERT_DATA_FILEPATH}" >> $target_file
                echo "INSERT_BALANCE_STRATEGY=${INSERT_BALANCE_STRATEGY}" >> $target_file
                echo "INSERT_STREAMING=${INSERT_STREAMING}" >> $target_file
                echo "INSERT_CORPUS_SIZE=${INSERT_CORPUS_SIZE}" >> $target_file
                echo "INSERT_BATCH_SIZE=${upload_bs}" >> $target_file
                echo "INSERT_CLIENTS_PER_PROXY=${INSERT_CLIENTS_PER_PROXY}" >> $target_file
                echo "IMPORT_PROCESSES=${IMPORT_PROCESSES}" >> $target_file
                
                echo "QUERY_DATA_FILEPATH=${QUERY_DATA_FILEPATH}" >> $target_file
                echo "QUERY_BALANCE_STRATEGY=${QUERY_BALANCE_STRATEGY}" >> $target_file
                echo "QUERY_STREAMING=${QUERY_STREAMING}" >> $target_file
                echo "QUERY_CORPUS_SIZE=${QUERY_CORPUS_SIZE}" >> $target_file
                echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                echo "QUERY_CLIENTS_PER_PROXY=${QUERY_CLIENTS_PER_PROXY}" >> $target_file

                echo "MIXED_RESULT_PATH=${MIXED_RESULT_PATH}" >> $target_file
                echo "MIXED_INSERT_BATCH_SIZE=${MIXED_INSERT_BATCH_SIZE:-$upload_bs}" >> $target_file
                echo "MIXED_QUERY_BATCH_SIZE=${MIXED_QUERY_BATCH_SIZE:-$query_bs}" >> $target_file
                echo "INSERT_MODE=${INSERT_MODE}" >> $target_file
                echo "INSERT_OPS_PER_SEC=${INSERT_OPS_PER_SEC}" >> $target_file
                echo "QUERY_MODE=${QUERY_MODE}" >> $target_file
                echo "QUERY_OPS_PER_SEC=${QUERY_OPS_PER_SEC}" >> $target_file
                echo "MIXED_CORPUS_SIZE=${MIXED_CORPUS_SIZE}" >> $target_file
                echo "MIXED_DATA_FILEPATH=${MIXED_DATA_FILEPATH}" >> $target_file
                echo "MIXED_QUERY_CLIENTS_PER_PROXY=${MIXED_QUERY_CLIENTS_PER_PROXY}" >> $target_file
                echo "MIXED_INSERT_CLIENTS_PER_PROXY=${MIXED_INSERT_CLIENTS_PER_PROXY}" >> $target_file
                echo "INSERT_START_ID=${INSERT_START_ID}" >> $target_file
                echo "COLLECTION_NAME=${COLLECTION_NAME}" >> $target_file
                echo "VECTOR_FIELD=${VECTOR_FIELD}" >> $target_file
                echo "ID_FIELD=${ID_FIELD}" >> $target_file
                echo "TOP_K=${TOP_K}" >> $target_file
                echo "QUERY_EF_SEARCH=${QUERY_EF_SEARCH}" >> $target_file
                echo "SEARCH_CONSISTENCY=${SEARCH_CONSISTENCY}" >> $target_file
                echo "RPC_TIMEOUT=${RPC_TIMEOUT}" >> $target_file
                echo "MIXED_INSERT_BATCH_MIN=${MIXED_INSERT_BATCH_MIN}" >> $target_file
                echo "MIXED_INSERT_BATCH_MAX=${MIXED_INSERT_BATCH_MAX}" >> $target_file
                echo "MIXED_QUERY_BATCH_MIN=${MIXED_QUERY_BATCH_MIN}" >> $target_file
                echo "MIXED_QUERY_BATCH_MAX=${MIXED_QUERY_BATCH_MAX}" >> $target_file
                echo "INSERT_BATCH_MIN=${INSERT_BATCH_MIN}" >> $target_file
                echo "INSERT_BATCH_MAX=${INSERT_BATCH_MAX}" >> $target_file
                echo "QUERY_BATCH_MIN=${QUERY_BATCH_MIN}" >> $target_file
                echo "QUERY_BATCH_MAX=${QUERY_BATCH_MAX}" >> $target_file
                
                echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file
                echo "DISTANCE_METRIC=${DISTANCE_METRIC}" >> $target_file
                echo "INIT_FLAT_INDEX=${INIT_FLAT_INDEX}" >> $target_file
                echo "GPU_INDEX=${GPU_INDEX}" >> $target_file
                
                echo "TRACING=${TRACING}" >> $target_file
                echo "PERF=${PERF}" >> $target_file
                echo "DEBUG=${DEBUG}" >> $target_file

                echo "RESTORE_DIR=${RESTORE_DIR}" >> $target_file
                echo "EXPECTED_CORPUS_SIZE=${EXPECTED_CORPUS_SIZE}" >> $target_file
                
                

                echo "MINIO_MODE=${MINIO_MODE}" >> $target_file
                echo "MINIO_MEDIUM=${MINIO_MEDIUM}" >> $target_file

                if [[ "$MODE" == "DISTRIBUTED" ]]; then
                    echo "ETCD_MODE=${ETCD_MODE}" >> $target_file
                    echo "COORDINATOR_NODES=${COORDINATOR_NODES}" >> $target_file
                    echo "COORDINATOR_NODES_PER_CN=${COORDINATOR_NODES_PER_CN}" >> $target_file
                    echo "STREAMING_NODES=${STREAMING_NODES}" >> $target_file
                    echo "STREAMING_NODES_PER_CN=${STREAMING_NODES_PER_CN}" >> $target_file
                    echo "QUERY_NODES=${QUERY_NODES}" >> $target_file
                    echo "QUERY_NODES_PER_CN=${QUERY_NODES_PER_CN}" >> $target_file
                    echo "DATA_NODES=${DATA_NODES}" >> $target_file
                    echo "DATA_NODES_PER_CN=${DATA_NODES_PER_CN}" >> $target_file
                    echo "NUM_PROXIES=${NUM_PROXIES}" >> $target_file
                    echo "NUM_PROXIES_PER_CN=${NUM_PROXIES_PER_CN}" >> $target_file
                    echo "DML_CHANNELS=${DML_CHANNELS}" >> $target_file
                else
                    echo "COORDINATOR_NODES=1" >> $target_file
                    echo "COORDINATOR_NODES_PER_CN=1" >> $target_file
                    echo "STREAMING_NODES=1" >> $target_file
                    echo "STREAMING_NODES_PER_CN=1" >> $target_file
                    echo "QUERY_NODES=1" >> $target_file
                    echo "QUERY_NODES_PER_CN=1" >> $target_file
                    echo "DATA_NODES=1" >> $target_file
                    echo "DATA_NODES_PER_CN=1" >> $target_file
                    echo "NUM_PROXIES=1" >> $target_file
                    echo "NUM_PROXIES_PER_CN=1" >> $target_file
                fi

                echo "" >> $target_file
                if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
                    cat local_main.sh >> $target_file
                else
                    cat main.sh >> $target_file
                fi
                mkdir -p $dir

                if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
                    cp ./goCode/multiClientOP/multiClientOP $dir/
                    cp ./goCode/multiClientOP/main.go $dir/multiClient.go
                    cp ./generalPython/multi_client_summary.py "$dir/"
                    cp ./generalPython/bulk_upload_import.py "$dir/"
                    if [[ "$TASK" == "MIXED" ]]; then
                        cp ./goCode/mixedRunner/mixedRunner "$dir/"
                        cp ./goCode/mixedRunner/main.go "$dir/mixed_main.go"
                        cp ../qdrant/generalPython/mixed_timeline.py "$dir/"
                    fi

                    if [[ -z "$RESTORE_DIR" ]]; then
                        cp ./generalPython/setup_collection.py "$dir/"

                        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
                            cp ./generalPython/index.py "$dir/"
                        fi
                    else
                        cp ./utils/status.py "$dir/"
                    fi
                else
                    # Copy in mode specific files
                    if [[ "$MODE" == "STANDALONE" ]]; then
                        # cp sifs/milvus.sif $dir/
                        cp sifs/milvus-2.6.6.sif $dir/milvus.sif
                        cp milvusSetup/standaloneLaunch.sh $dir/
                        cp milvusSetup/execute.sh $dir/

                    elif [[ "$MODE" == "DISTRIBUTED" ]]; then
                        cp sifs/milvus.sif $dir/
                        cp sifs/etcd_v3.5.18.sif $dir/
                        cp sifs/minio.sif $dir/
                        cp milvusSetup/execute.sh $dir/
                        cp milvusSetup/launch_etcd.sh $dir/
                        cp milvusSetup/launch_minio.sh $dir/
                        cp milvusSetup/launch_milvus_part.sh $dir/
                    else
                        echo "Unknown MODE: $SYSTEM"
                        exit
                    fi

                    if [[ "$TRACING" == "True" ]]; then
                        cp sifs/otel-collector.sif $dir/
                        cp utils/launch_otel.sh $dir/
                        cp utils/otel_config.yaml $dir/
                        cp utils/analyze_traces.py $dir/
                    fi

                    # Copy in basic python utils
                    cp generalPython/net_mapping.py $dir/
                    cp generalPython/replace_unified.py $dir/
                    cp generalPython/profile.py $dir/
                    cp generalPython/poll.py $dir/
                    cp ./generalPython/multi_client_summary.py "$dir/"
                    cp ./generalPython/bulk_upload_import.py "$dir/"
                                    
                    # all tasks need go code
                    cp ./goCode/multiClientOP/multiClientOP $dir/
                    cp ./goCode/multiClientOP/main.go $dir/multiClient.go
                    if [[ "$TASK" == "MIXED" ]]; then
                        cp ./goCode/mixedRunner/mixedRunner "$dir/"
                        cp ./goCode/mixedRunner/main.go "$dir/mixed_main.go"
                        cp ../qdrant/generalPython/mixed_timeline.py "$dir/"
                    fi

                    if [[ -z "$RESTORE_DIR" ]]; then
                        cp ./generalPython/setup_collection.py "$dir/"

                        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
                            cp ./generalPython/index.py "$dir/"
                        fi
                    else
                            cp ./utils/status.py "$dir/"
                    fi
                fi

                

                mv $target_file $dir
                
                chmod -R g+w $dir

                if [[ "${RUN_MODE^^}" != "LOCAL" ]]; then
                    cd $dir
                    # qsub $target_file
                    sleep 1
                    cd .. 
                else
                    echo "Created local dir ${dir} for testing"
                fi
            done
        done
    done
done
