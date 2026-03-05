#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if ! "$SCRIPT_DIR/check_dependencies.sh" --missing-only; then
    exit 1
fi



### Loop variables ###
NODES=(1)
CORES=(112)

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768

# Lustre todo: 8192, 256 (queued?)
# best batch for 32 clients: 128
UPLOAD_BATCH_SIZE=(128) 

# 2 8
QUERY_BATCH_SIZE=(2048)

# PBS Vars
WALLTIME="01:00:00"
queue=debug # [preemptable, debug, debug-scaling, prod,capacity]


### General runtime variables ###
TASK="index" # [insert,index]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
usePerf="false" # [true, false]
CORPUS_SIZE=10000000 # total data to insert
UPLOAD_CLIENTS_PER_PROXY=32
BASE_DIR="$(pwd)"
WAL="woodpecker" # [woodpecker, default]
UPLOAD_BALANCE_STRATEGY="WORKER" # [NONE, WORKER]

### Path to embeddings
# Aurora
    # 10 million 
    #    HPC-Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
    #    Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy
# Polaris 
    # 10 million 
    #     HPC-Pes2o: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy

# DATA_FILEPATH="/eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy"
DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"
VECTOR_DIM=200

# Path to Python env
# Aurora: /lus/flare/projects/radix-io/sockerman/milvusEnv/
# Polaris: /eagle/projects/radix-io/sockerman/vectorEval/milvus/multiNode/env/
ENV_PATH=/lus/flare/projects/radix-io/sockerman/milvusEnv/

PLATFORM="AURORA" # [POLARIS, AURORA]

MODE="STANDALONE" # [DISTRIBUTED, STANDALONE]

### Distributed Variables
MINIO_MODE="stripped" # [single, stripped]
MINIO_MEDIUM="DAOS" # [DAOS, lustre] (can be memory if running single)
ETCD_MODE="replicated" # [single, replicated]
STREAMING_NODES=32
STREAMING_NODES_PER_CN=4
NUM_PROXIES=32
NUM_PROXIES_PER_CN=4
DML_CHANNELS=128 # controls DML channels on startup -> defaults to 16 if not set


for num_nodes in "${NODES[@]}"
do
    for query_bs in "${QUERY_BATCH_SIZE[@]}" 
    do

        for upload_bs in "${UPLOAD_BATCH_SIZE[@]}"

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
                    if [[ "$STORAGE_MEDIUM" == "DAOS" || "$MINIO_MEDIUM" == "DAOS" ]]; then
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
                if [[ "$TASK" == "insert" ]]; then

                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_CPP${UPLOAD_CLIENTS_PER_PROXY}_uploadBS${upload_bs}_SN${STREAMING_NODES}_SPCN${STREAMING_NODES_PER_CN}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_CPP${UPLOAD_CLIENTS_PER_PROXY}_uploadBS${upload_bs}_${DATE}"
                    fi
                elif [[ "$TASK" == "index" ]]; then
                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_${DATE}"
                    else
                        dir="${TASK}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_${DATE}"
                    fi
                else
                    echo "Unknown task: $TASK: valid options include insert, index"
                    exit
                fi

                echo "myDIR=${dir}" >> $target_file
                echo "BASE_DIR=${BASE_DIR}" >> $target_file
                echo "ENV_PATH=${ENV_PATH}" >> $target_file
                echo "TASK=${TASK}" >> $target_file

                echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                echo "CORPUS_SIZE=${CORPUS_SIZE}" >> $target_file
                echo "USEPERF=${usePerf}" >> $target_file
                echo "CORES=${numCores}" >> $target_file
                echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                echo "UPLOAD_BATCH_SIZE=${upload_bs}" >> $target_file
                echo "UPLOAD_CLIENTS_PER_PROXY=${UPLOAD_CLIENTS_PER_PROXY}" >> $target_file
                echo "DATA_FILEPATH=${DATA_FILEPATH}" >> $target_file
                echo "PLATFORM=${PLATFORM}" >> $target_file
                echo "MODE=${MODE}" >> $target_file
                echo "WAL=${WAL}" >> $target_file
                echo "UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}" >> $target_file
                echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file

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
                cat main.sh >> $target_file
                mkdir -p $dir

                # Copy in mode specific files
                if [[ "$MODE" == "STANDALONE" ]]; then
                    cp milvus.sif $dir/
                    cp milvusSetup/standaloneLaunch.sh $dir/
                    cp milvusSetup/execute.sh $dir/

                elif [[ "$MODE" == "DISTRIBUTED" ]]; then
                    cp milvus.sif $dir/
                    cp etcd_v3.5.18.sif $dir/
                    cp minio.sif $dir/
                    cp milvusSetup/execute.sh $dir/
                    cp milvusSetup/launch_etcd.sh $dir/
                    cp milvusSetup/launch_minio.sh $dir/
                    cp milvusSetup/launch_milvus_part.sh $dir/
                else
                    echo "Unknown MODE: $SYSTEM"
                    exit
                fi


                # Copy in basic python utils
                cp generalPython/net_mapping.py $dir/
                cp generalPython/replace.py $dir/
                cp generalPython/profile.py $dir/
                cp generalPython/poll.py $dir/
                cp generalPython/status.py $dir/
                
                
                # all tasks need insert code
                cp ./generalPython/setup_collection.py $dir/
                cp ./generalPython/insert_multi_client_summary.py $dir/
                cp ./goCode/multiClientInsert/multiClientInsert $dir/
                cp ./goCode/multiClientInsert/main.go $dir/multiClientInsert.go

                if [[ "$TASK" == "index" ]]; then
                    cp generalPython/convert_to_hnsw.py $dir/
                fi

                mv $target_file $dir
                
                chmod -R g+w $dir
                cd $dir
                qsub $target_file
                sleep 1
                cd .. 
            done
        done
    done
done