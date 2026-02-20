#!/bin/bash

### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(1)
CORES=(112)

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
UPLOAD_BATCH_SIZE=(512) 

# 2 8
QUERY_BATCH_SIZE=(2048)

# PBS Vars
WALLTIME="01:00:00"
queue=debug-scaling # [preemptable, debug, debug-scaling, prod,capacity]


### Runtime variables ###
task="insert" # [insert]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
usePerf="false" # [true, false]
CORPUS_SIZE=10000000 # total data to insert
UPLOAD_CLIENTS_PER_WORKER=32
BASE_DIR="$(pwd)" # directory you are running this script is the base for what the run dir will need to cd into
WAL="woodpecker" # [woodpecker, default]
MINIO_MODE="stripped" # [single, stripped]

### Path to embeddings
# Aurora
    # 10 million subset: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
# Polaris 
    # 10 million subset: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy

# DATA_FILEPATH="/eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy"
DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy"

# Path to Python env
# Aurora: /lus/flare/projects/radix-io/sockerman/milvusEnv/
# Polaris: /eagle/projects/radix-io/sockerman/vectorEval/milvus/multiNode/env/
ENV_PATH=/lus/flare/projects/radix-io/sockerman/milvusEnv/

PLATFORM="AURORA" # [POLARIS, AURORA]

MODE="DISTRIBUTED" # [DISTRIBUTED, STANDALONE]



for num_nodes in "${NODES[@]}"
do

    for workers in "${WORKERS_PER_NODE[@]}" 
    do
        for query_bs in "${QUERY_BATCH_SIZE[@]}" 
        do

            for upload_bs in "${UPLOAD_BATCH_SIZE[@]}"

            do 
                for numCores in "${CORES[@]}"
                do 


                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        total_nodes=$((num_nodes + 4))
                    else
                        total_nodes=$((num_nodes + 1))

                    fi
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
                    if [[ "$task" == "insert" ]]; then
                        dir="${task}_${MODE}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_C${UPLOAD_CLIENTS_PER_WORKER}_uploadBS${upload_bs}_${DATE}"
                    elif [[ "$task" == "aurora" ]]; then
                        echo "Running on Aurora"
                    else
                        echo "Unknown task: $SYSTEM"
                        exit
                    fi

                    echo "myDIR=${dir}" >> $target_file
                    echo "BASE_DIR=${BASE_DIR}" >> $target_file
                    echo "ENV_PATH=${ENV_PATH}" >> $target_file

                    echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                    echo "CORPUS_SIZE=${CORPUS_SIZE}" >> $target_file
                    echo "USEPERF=${usePerf}" >> $target_file
                    echo "CORES=${numCores}" >> $target_file
                    echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                    echo "UPLOAD_BATCH_SIZE=${upload_bs}" >> $target_file
                    echo "UPLOAD_CLIENTS_PER_WORKER=${UPLOAD_CLIENTS_PER_WORKER}" >> $target_file
                    echo "DATA_FILEPATH=${DATA_FILEPATH}" >> $target_file
                    echo "PLATFORM=${PLATFORM}" >> $target_file
                    echo "MODE=${MODE}" >> $target_file

                    if [[ "$MODE" == "DISTRIBUTED" ]]; then
                        echo "WAL=${WAL}" >> $target_file
                        echo "MINIO_MODE=${MINIO_MODE}" >> $target_file
                    fi

                    # base="${target%%.*}"
                    # dir="${task}_${base}_bs_${BATCH_SIZE}_c_${NClients}_${num_nodes}_${workers}_${DATE}"
                    # echo "myDIR=${dir}" >> $target_file
                    # echo "NClients=${NClients}" >> $target_file
                    # echo "CORPUS_SIZE=${CORPUS_SIZE}" >> $target_file
                    # # echo "SEARCH_THREADS=${SEARCH_THREADS}" >> $target_file
                    # # echo "NUMBER_SEGEMENTS=${NUMBER_SEGEMENTS}" >> $target_file
                    # # echo "backupDir=''" >> $target_file
                    # echo "script=${target}" >> $target_file
                    # # echo "" >> $target_file

                    echo "" >> $target_file
                    cat $task/main.sh >> $target_file
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
                        echo "Unknown task: $SYSTEM"
                        exit
                    fi


                    # Copy in basic python utils
                    cp generalPython/net_mapping.py $dir/
                    cp generalPython/replace.py $dir/
                    cp generalPython/profile.py $dir/
                    cp generalPython/poll.py $dir/
                    cp generalPython/status.py $dir/
                    
                    if [[ "$task" == "insert" ]]; then
                        cp ./insert/setup_collection.py $dir/
                        cp ./insert/multi_client_summary.py $dir/
                        cp ./goCode/multiClientInsert/multiClientInsert $dir/
                        cp ./goCode/multiClientInsert/main.go $dir/multiClientInsert.go
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
done