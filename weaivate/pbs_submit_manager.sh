#!/bin/bash

# SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# if ! "$SCRIPT_DIR/check_dependencies.sh" --missing-only; then
#     exit 1
# fi


### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(1)
CORES=(112)

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
UPLOAD_BATCH_SIZE=(128) 

# 2 8
QUERY_BATCH_SIZE=(2048)

UPLOAD_CLIENTS_PER_WORKER=(1)
# PBS Vars
WALLTIME="01:00:00"
queue=debug # [preemptable, debug, debug-scaling, prod, capacity]


### Runtime variables ###
TASK="index" # [insert, index]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
usePerf="false" # [true, false]
CORPUS_SIZE=10000000 # total data to insert
UPLOAD_CLIENTS_PER_WORKER=32
UPLOAD_BALANCE_STRATEGY="WORKER_BALANCE" # [NO_BALANCE, WORKER_BALANCE]
GPU_INDEX=True

# Aurora
    # 10 million 
    #    HPC-Pes2o: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
    #    Yandex: /lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy
# Polaris 
    # 10 million 
    #     HPC-Pes2o: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy
DATA_FILEPATH="/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy"
VECTOR_DIM=200
DISTANCE_METRIC="IP" # [IP, COSINE, L2]

PLATFORM="AURORA" # [POLARIS, AURORA]

for num_nodes in "${NODES[@]}"
do
    for workers in "${WORKERS_PER_NODE[@]}" 
    do
        for UCPW in "${UPLOAD_CLIENTS_PER_WORKER[@]}" 
        do
            for query_bs in "${QUERY_BATCH_SIZE[@]}" 
            do

                for upload_bs in "${UPLOAD_BATCH_SIZE[@]}"

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
                        if [[ "$TASK" == "insert" ]]; then
                            dir="${TASK}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_C${UCPW}_uploadBS${upload_bs}_${DATE}"
                        elif [[ "$TASK" == "index" ]]; then
                            dir="${TASK}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_CS${CORPUS_SIZE}_${DATE}"
                        else
                            echo "Unknown task: $SYSTEM"
                            exit
                        fi
                        
                        echo "myDIR=${dir}" >> $target_file
                        echo "TASK=${TASK}" >> $target_file
                        echo "VECTOR_DIM=${VECTOR_DIM}" >> $target_file
                        echo "DISTANCE_METRIC=${DISTANCE_METRIC}" >> $target_file
                        echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}" >> $target_file
                        echo "CORPUS_SIZE=${CORPUS_SIZE}" >> $target_file
                        echo "USEPERF=${usePerf}" >> $target_file
                        echo "CORES=${numCores}" >> $target_file
                        echo "QUERY_BATCH_SIZE=${query_bs}" >> $target_file
                        echo "UPLOAD_BATCH_SIZE=${upload_bs}" >> $target_file
                        echo "UPLOAD_CLIENTS_PER_WORKER=${UCPW}" >> $target_file
                        echo "DATA_FILEPATH=${DATA_FILEPATH}" >> $target_file
                        echo "UPLOAD_BALANCE_STRATEGY=${UPLOAD_BALANCE_STRATEGY}" >> $target_file
                        echo "PLATFORM=${PLATFORM}" >> $target_file
                        echo "GPU_INDEX=${GPU_INDEX}" >> $target_file

                        echo "" >> $target_file
                        cat main.sh >> $target_file



                        mkdir -p $dir

                      
                        cp weaviateSetup/launchWeaviateNode.sh $dir/
                        # cp qdrantSetup/launch.sh $dir/

                        # copy in general Python files
                       
                        cp -r perf/ $dir/
                

                       
                        # if [[ "$TASK" == "index" ]]; then
                        #     cp generalPython/index.py $dir/
                        # fi

                        
                        
                        mv $target_file $dir

                        chmod -R g+w $dir
                        cd $dir

                        # qsub $target_file
                        sleep 5
                        cd .. 
                    done
                done
            done
        done
    done
done