#!/bin/bash




### Loop variables ###
NODES=(1)
WORKERS_PER_NODE=(1)
CORES=(112)

# Batch: 1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768
UPLOAD_BATCH_SIZE=(512) 

# 2 8
QUERY_BATCH_SIZE=(2048)

UPLOAD_CLIENTS_PER_WORKER=(1)
# PBS Vars
WALLTIME="01:00:00"
queue=debug # [preemptable, debug, debug-scaling, prod]


### Runtime variables ###
task="insert" # [insert]
STORAGE_MEDIUM="memory" # [memory, DAOS, lustre, SSD]
usePerf="false" # [true, false]
CORPUS_SIZE=5000000 # total data to insert
UPLOAD_CLIENTS_PER_WORKER=1
UPLOAD_BALANCE_STRATEGY="NO_BALANCE" # [NO_BALANCE, WORKER_BALANCE]

# 
# Aurora 10 million subset: /lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy
# Polaris 10 million subset: /eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy
# 
DATA_FILEPATH="/eagle/projects/argonne_tpc/sockerman/pes2oEmbeddings/embeddings.npy"

PLATFORM="POLARIS" # [POLARIS, AURORA]

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

                        if [[ "$PLATFORM" == "POLARIS" ]]; then
                            echo "exec > >(tee output.log) 2>&1" >> $target_file    
                        
                        fi
                        echo "NODES=${num_nodes}" >> $target_file
                        echo "WORKERS_PER_NODE=${workers}" >> $target_file

                        
                        DATE=$(date +"%Y-%m-%d_%H_%M_%S")
                        if [[ "$task" == "insert" ]]; then
                            dir="${task}_${STORAGE_MEDIUM}_N${num_nodes}_NP${workers}_C${UCPW}_uploadBS${upload_bs}_${DATE}"
                        elif [[ "$task" == "aurora" ]]; then
                            echo "Running on Aurora"
                        else
                            echo "Unknown task: $SYSTEM"
                            exit
                        fi
                        
                        echo "myDIR=${dir}" >> $target_file


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
                        
                        # # echo "NUM_SEGEMENTS=${NUM_SEGEMENTS}" >> $target_file
                        # # echo "TARGET_SEGEMENTS=${TARGET_SEGEMENTS}" >> $target_file
                        # # echo "HNSW_M=${HNSW_M}" >> $target_file
                        # # echo "EF_CONSTRUCT=${EF_CONSTRUCT}" >> $target_file
                
                        # # # echo "NUMBER_SEGEMENTS=${NUMBER_SEGEMENTS}" >> $target_file
                        # # echo "backupDir=''" >> $target_file
                        # # echo "script=${target}" >> $target_file
                        # # echo "NClients=${NClients}" >> $target_file
                        echo "" >> $target_file

                        cat $task/main.sh >> $target_file


                        mkdir -p $dir

                        # copy in Qdrant files
                        cp qdrant.sif $dir/
                        cp qdrant $dir/
                        cp qdrantSetup/launchQdrantNode.sh $dir/
                        cp qdrantSetup/launch.sh $dir/

                        # copy in general Python files
                        cp generalPython/gen_dirs.py $dir/
                        cp generalPython/mapping.py $dir/
                        cp generalPython/profile.py $dir/
                        cp generalPython/configureTopo.py $dir/
                        cp -r perf/ $dir/
                

                        if [[ "$task" == "insert" ]]; then
                            mkdir $dir/rustSrc
                            cp ./rustCode/multiClientUpload/multiClientUpload $dir/
                            cp ./rustCode/multiClientUpload/src/main.rs $dir/rustSrc/multiClientUpload.rs
                            cp insert/multi_client_summary.py $dir/

                        elif [[ "$task" == "aurora" ]]; then
                            echo "Running on Aurora"
                        else
                            echo "Unknown task: $SYSTEM"
                            exit
                        fi

                        
                        # cp ${task}/${target} $dir/
                        # cp ${task}/index.py $dir/
                        # # cp ${task}/query.py $dir/
                        # # cp ${task}/snap_threads.py $dir/
                        # # cp ${task}/QueryRustClient $dir/
                        # cp ${task}/RecallQueryRustClient $dir/
                        # cp ${task}/EvenUploadRustClient $dir/EvenUploadRustClient
                        # # cp ${task}/rust/src/main.rs $dir/query_main.rs
                        # cp ${task}/evenUpload/src/main.rs $dir/evenUpload_main.rs
                        # cp ${task}/recallQuery/src/main.rs $dir/recall_query.rs
                        # # cp -r ./network/rust/src/main.rs $dir/upload_main.rs

                        # # cp -r ${task}/rust/src/main.rs $dir/
                        # # cp ${task}/min_collection_setup.py $dir/
                        # # cp utils/configureTopo.py $dir/
                        # # cp ${task}/* $dir/
                        # cp ${task}/customKeyConfigureTopo.py $dir/
                        # cp ${task}/restore_from_snapshot.py $dir/
                        # cp ${task}/segmentConfigureTopo.py $dir/
                        # cp ${task}/recall_calc.py $dir/
                        # # cp utils/count_shard_points.py $dir/
                        # # cp utils/snapshot_cluster.py $dir/
                        # # cp utils/snapshot_cluster.py $dir/
                        # cp utils/profile.py $dir/
                        # cp utils/mapping.py $dir/
                        # cp qdrantSetup/* $dir/
                        # # cp /eagle/projects/radix-io/sockerman/vectorEval/qdrantEval/temp/perfData/qdrant/target/release/qdrant $dir
                        # cp qdrant $dir
                        mv $target_file $dir

                        chmod -R g+w $dir
                        cd $dir

                        qsub $target_file
                        sleep 5
                        cd .. 
                    done
                done
            done
        done
    done
done