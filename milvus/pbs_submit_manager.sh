#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# if ! "$SCRIPT_DIR/check_dependencies.sh" --missing-only; then
#     exit 1
# fi

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

    apply_override_value INSERT_CORPUS_SIZE INSERT_CORPUS_SIZE_OVERRIDE
    apply_override_value INSERT_CLIENTS_PER_PROXY INSERT_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value INSERT_BALANCE_STRATEGY INSERT_BALANCE_STRATEGY_OVERRIDE
    apply_override_value INSERT_DATA_FILEPATH INSERT_DATA_FILEPATH_OVERRIDE
    apply_override_array INSERT_BATCH_SIZE INSERT_BATCH_SIZE_OVERRIDE

    apply_override_value VECTOR_DIM VECTOR_DIM_OVERRIDE
    apply_override_value DISTANCE_METRIC DISTANCE_METRIC_OVERRIDE
    apply_override_value INIT_FLAT_INDEX INIT_FLAT_INDEX_OVERRIDE

    apply_override_value QUERY_CORPUS_SIZE QUERY_CORPUS_SIZE_OVERRIDE
    apply_override_value QUERY_CLIENTS_PER_PROXY QUERY_CLIENTS_PER_PROXY_OVERRIDE
    apply_override_value QUERY_BALANCE_STRATEGY QUERY_BALANCE_STRATEGY_OVERRIDE
    apply_override_value QUERY_DATA_FILEPATH QUERY_DATA_FILEPATH_OVERRIDE
    apply_override_array QUERY_BATCH_SIZE QUERY_BATCH_SIZE_OVERRIDE

    apply_override_value RESTORE_DIR RESTORE_DIR_OVERRIDE
    apply_override_value EXPECTED_CORPUS_SIZE EXPECTED_CORPUS_SIZE_OVERRIDE

    apply_override_value MINIO_MODE MINIO_MODE_OVERRIDE
    apply_override_value MINIO_MEDIUM MINIO_MEDIUM_OVERRIDE
    apply_override_value ETCD_MODE ETCD_MODE_OVERRIDE
    apply_override_value STREAMING_NODES STREAMING_NODES_OVERRIDE
    apply_override_value STREAMING_NODES_PER_CN STREAMING_NODES_PER_CN_OVERRIDE
    apply_override_value NUM_PROXIES NUM_PROXIES_OVERRIDE
    apply_override_value NUM_PROXIES_PER_CN NUM_PROXIES_PER_CN_OVERRIDE
    apply_override_value DML_CHANNELS DML_CHANNELS_OVERRIDE
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
    echo "Tracing:                  $TRACING"
    echo "Debug:                    $DEBUG"
    echo "Vector Dim:               $VECTOR_DIM"
    echo "Distance Metric:          $DISTANCE_METRIC"
    echo "Init Flat Index:          $INIT_FLAT_INDEX"
    echo "GPU Index:                $GPU_INDEX"

    case "$TASK" in
        INSERT)
            echo "Insert Corpus Size:       $INSERT_CORPUS_SIZE"
            echo "Insert Data File:         $INSERT_DATA_FILEPATH"
            echo "Insert Batch Sizes:       ${INSERT_BATCH_SIZE[*]}"
            echo "Insert Clients/Proxy:     $INSERT_CLIENTS_PER_PROXY"
            echo "Insert Balance:           $INSERT_BALANCE_STRATEGY"
            ;;
        INDEX)
            echo "Index Corpus Size:        $INSERT_CORPUS_SIZE"
            echo "Restore Dir:              ${RESTORE_DIR:-<unset>}"
            echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
            ;;
        QUERY)
            echo "Query Corpus Size:        $QUERY_CORPUS_SIZE"
            echo "Query Data File:          $QUERY_DATA_FILEPATH"
            echo "Query Batch Sizes:        ${QUERY_BATCH_SIZE[*]}"
            echo "Query Clients/Proxy:      $QUERY_CLIENTS_PER_PROXY"
            echo "Query Balance:            $QUERY_BALANCE_STRATEGY"
            echo "Restore Dir:              ${RESTORE_DIR:-<unset>}"
            echo "Expected Corpus Size:     $EXPECTED_CORPUS_SIZE"
            ;;
    esac

    if [[ "$MODE" == "DISTRIBUTED" ]]; then
        echo "MinIO Mode:               $MINIO_MODE"
        echo "MinIO Medium:             $MINIO_MEDIUM"
        echo "ETCD Mode:                $ETCD_MODE"
        echo "Streaming Nodes:          $STREAMING_NODES"
        echo "Streaming/Compute Node:   $STREAMING_NODES_PER_CN"
        echo "Proxies:                  $NUM_PROXIES"
        echo "Proxies/Compute Node:     $NUM_PROXIES_PER_CN"
        echo "DML Channels:             $DML_CHANNELS"
    else
        echo "WAL Mode:                 $WAL"
        case "$TASK" in
            INSERT)
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
MILVUS_BUILD_DIR="" # Name of the directory with your build: traceMilvus, cpuMilvus
MILVUS_CONFIG_DIR="cpuMilvus" # If you have a specfic config, the path to the dir containing the yaml file
PLATFORM="AURORA" # [POLARIS, AURORA]

### General runtime variables ###
TASK="QUERY" # [INSERT, INDEX, QUERY]
RUN_MODE="PBS" # [PBS, local]
MODE="STANDALONE" # [DISTRIBUTED, STANDALONE]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
PERF="NONE" # [NONE, STAT, RECORD]
WAL="woodpecker" # [woodpecker, default]
GPU_INDEX="False" # [True, False]
TRACING="False" 
DEBUG="False" # [True, False]
BASE_DIR="$(pwd)"

### Insertion Variables ### 
INSERT_CORPUS_SIZE=10000000 # total data to insert
INSERT_CLIENTS_PER_PROXY=4
INSERT_BALANCE_STRATEGY="WORKER" # [NONE, WORKER]
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
INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"
# INSERT_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
# best batch for 32 clients: 128
INSERT_BATCH_SIZE=(512)
# VECTOR_DIM=200
VECTOR_DIM=200
DISTANCE_METRIC="IP" # [IP, COSINE, L2]
INIT_FLAT_INDEX="FALSE" # [TRUE, FALSE]


### QUERY Variables ###
# QUERY_CORPUS_SIZE=22723  # queries
QUERY_CORPUS_SIZE=100000  # queries
QUERY_CLIENTS_PER_PROXY=1
QUERY_BALANCE_STRATEGY="NONE" # [NONE, WORKER]
QUERY_BATCH_SIZE=(512)

# Aurora
    # * Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy
    # * Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy
# Local (docker based)
    # Yandex: /home/seth/Documents/research/SCVectorDB/yandexTest/YandexQuery100k.npy
# QUERY_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy"
# QUERY_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/queries.npy"
QUERY_DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/YandexQuery100k.npy"


# Polaris: TODO
# Aurora: 
#   Yandex: /lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/yandex
#   pes2o: /lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/pes2o
# RESTORE_DIR="/lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/yandex"
# RESTORE_DIR="/lus/flare/projects/radix-io/sockerman/temp/milvus/10MillDirs/pes2o"
RESTORE_DIR=""
EXPECTED_CORPUS_SIZE=10000000




### Distributed Variables ###
MINIO_MODE="stripped" # [single, stripped]
MINIO_MEDIUM="lustre" # [lustre] (can be memory if running single) - DAOS is broken
ETCD_MODE="replicated" # [single, replicated]
STREAMING_NODES=8
STREAMING_NODES_PER_CN=4
NUM_PROXIES=8
NUM_PROXIES_PER_CN=4
DML_CHANNELS=16 # controls DML channels on startup -> defaults to 16 if not set

apply_overrides


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
                else
                    echo "Unknown task: $TASK: valid options include INSERT, INDEX, QUERY"
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
                echo "INSERT_CORPUS_SIZE=${INSERT_CORPUS_SIZE}" >> $target_file
                echo "INSERT_BATCH_SIZE=${upload_bs}" >> $target_file
                echo "INSERT_CLIENTS_PER_PROXY=${INSERT_CLIENTS_PER_PROXY}" >> $target_file
                
                echo "QUERY_DATA_FILEPATH=${QUERY_DATA_FILEPATH}" >> $target_file
                echo "QUERY_BALANCE_STRATEGY=${QUERY_BALANCE_STRATEGY}" >> $target_file
                echo "QUERY_CORPUS_SIZE=${QUERY_CORPUS_SIZE}" >> $target_file
                echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                echo "QUERY_CLIENTS_PER_PROXY=${QUERY_CLIENTS_PER_PROXY}" >> $target_file
                
                echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file
                echo "DISTANCE_METRIC=${DISTANCE_METRIC}" >> $target_file
                echo "INIT_FLAT_INDEX=${INIT_FLAT_INDEX}" >> $target_file
                echo "GPU_INDEX=${GPU_INDEX}" >> $target_file
                
                echo "TRACING=${TRACING}" >> $target_file
                echo "PERF=${PERF}" >> $target_file
                echo "DEBUG=${DEBUG}" >> $target_file

                echo "RESTORE_DIR=${RESTORE_DIR}" >> $target_file
                echo "EXPECTED_CORPUS_SIZE=${EXPECTED_CORPUS_SIZE}" >> $target_file
                
                


                if [[ "$MODE" == "DISTRIBUTED" ]]; then
                    echo "MINIO_MODE=${MINIO_MODE}" >> $target_file
                    echo "ETCD_MODE=${ETCD_MODE}" >> $target_file
                    echo "STREAMING_NODES=${STREAMING_NODES}" >> $target_file
                    echo "STREAMING_NODES_PER_CN=${STREAMING_NODES_PER_CN}" >> $target_file
                    echo "NUM_PROXIES=${NUM_PROXIES}" >> $target_file
                    echo "NUM_PROXIES_PER_CN=${NUM_PROXIES_PER_CN}" >> $target_file
                    echo "DML_CHANNELS=${DML_CHANNELS}" >> $target_file
                    echo "MINIO_MEDIUM=${MINIO_MEDIUM}" >> $target_file
                else
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

                    if [[ -z "$RESTORE_DIR" ]]; then
                        cp ./generalPython/query_setup_collection.py "$dir/"

                        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" ]]; then
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
                                    
                    # all tasks need go code
                    cp ./goCode/multiClientOP/multiClientOP $dir/
                    cp ./goCode/multiClientOP/main.go $dir/multiClient.go

                    if [[ -z "$RESTORE_DIR" ]]; then
                        cp ./generalPython/setup_collection.py "$dir/"

                        if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" ]]; then
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
