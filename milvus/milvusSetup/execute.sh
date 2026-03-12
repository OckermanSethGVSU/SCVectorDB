#!/bin/bash

USEPERF=$1
RANK=$2

# LD_PRELOADS
export LD_PRELOAD=""
LIBDIR=/milvus/internal/core/output/lib && \
export LD_LIBRARY_PATH="$LIBDIR:${LD_LIBRARY_PATH}" && \
export LD_PRELOAD="/milvus/internal/core/output/lib/libjemalloc.so"

if [[ "$PLATFORM" == "POLARIS" ]]; then
  export CUDAToolkit_ROOT=/usr/local/cuda
  export CUDA_TOOLKIT_ROOT_DIR=/usr/local/cuda
  export PATH="/usr/local/cuda/bin:$PATH"
  export LD_LIBRARY_PATH="/usr/local/cuda/lib64:$LD_LIBRARY_PATH"
  export PATH="/usr/local/cuda/bin:$PATH"
fi

apt-get update && apt-get install -y libatomic1 libelf-dev libdw-dev libslang2-dev libperl-dev python3-dev libnuma-dev libtraceevent-dev
echo "Install Complete"
sleep 3

cd /milvus

if [[ "$TYPE" == "STANDALONE" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
    MILVUS_PID=$!
    SIGNAL_FILE="milvus_running.txt"

elif [[ "$TYPE" == "COORDINATOR" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run mixcoord &
    MILVUS_PID=$!
    SIGNAL_FILE="cord${RANK}_running.txt"

elif [[ "$TYPE" == "PROXY" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run proxy &
    MILVUS_PID=$!
    SIGNAL_FILE="proxy${RANK}_running.txt"

elif [[ "$TYPE" == "DATA" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run datanode &
    MILVUS_PID=$!
    SIGNAL_FILE="data${RANK}_running.txt"

elif [[ "$TYPE" == "QUERY" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run querynode &
    MILVUS_PID=$!
    SIGNAL_FILE="query${RANK}_running.txt"

elif [[ "$TYPE" == "STREAMING" ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./bin/milvus run streamingnode &
    MILVUS_PID=$!
    SIGNAL_FILE="streaming${RANK}_running.txt"
fi

sleep 10

touch /workerOut/$SIGNAL_FILE

# wait until the start file exists
TARGET="/workerOut/workflow_start.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

if [[ "$PERF" == "RECORD" ]]; then
    echo "Rank ${RANK} Launching perf record"
    /perfDir/perf record -F 99 --call-graph fp -g --proc-map-timeout 5000 -o /workerOut/perf${RANK}.data  -p "$MILVUS_PID" &
    PERF_PID=$!

elif [[ "$PERF" == "STAT" ]]; then
    echo "Rank ${RANK} Launching perf stat"
    /perfDir/perf stat  -e cycles,instructions,branches,branch-misses,cache-misses -o /workerOut/perf${RANK}.data  -p "$MILVUS_PID" &
    PERF_PID=$!
fi



# wait until the stop file exists
TARGET="/workerOut/workflow_end.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

# stop perf cleanly (same as Ctrl-C)
if [[ "$PERF" == "RECORD" || "$PERF" == "STAT" ]]; then
    echo "Rank ${RANK} stopping perf"
    kill -INT "$PERF_PID"
    wait "$PERF_PID"
fi

echo "Rank ${RANK} done"
sleep 15 

# apt-get update && apt-get install -y libatomic1 libelf-dev libdw-dev libslang2-dev libperl-dev python3-dev libnuma-dev libtraceevent-dev
# apt-get install -y \
    # build-essential ca-certificates pkg-config \
    # libssl-dev zlib1g-dev libcurl4-openssl-dev gnupg


# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" milvus run standalone > /workerOut/standalone.txt 2>&1

# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
#  /perfDir/perf record  -F 99 -g --call-graph fp --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# # NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
# # /perfDir/perf record  -F 99 -g --call-graph fp --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &

# NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
#  /perfDir/perf record  -F 99 -g --no-buildid-cache --call-graph dwarf --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone > /workerOut/standalone.txt 2>&1 &
# # /perfDir/perf probe -x milvus -F > probe.txt

# while true; do
#   if [ -f /workerOut/stop.txt ]; then
#     echo "stop.txt detected, sending SIGTERM."
#     # QDRANT_PIDS=$(pgrep -fa './qdrant' | grep -v 'perf'  | awk '{print $1}')
#     PIDS=$(pgrep -fa './milvus' | grep -v 'perf'  | awk '{print $1}')
#     echo "Target PIDs: ${PIDS}"
#     kill -2 $PIDS

#     sleep 60
#     exit
  
#   fi

# done

# /perfDir/perf record -F 99 --call-graph dwarf -g --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone &
# & > /workerOut/standalone.txt 2>&1 &
# /perfDir/perf record -F 99 --call-graph dwarf -g --proc-map-timeout 5000 -o /workerOut/perf.data -- ./bin/milvus run standalone  > /workerOut/standalone.txt 2>&1 