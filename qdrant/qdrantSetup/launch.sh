#!/bin/bash

# apt-get update && apt install -y \
#     build-essential \
#     flex bison \
#     libelf-dev \
#     libdw-dev \
#     libunwind-dev \
#     libzstd-dev \
#     libnuma-dev \
#     libssl-dev \
#     libperl-dev \
#     python3-dev \
#     libiberty-dev \
#     zlib1g-dev \
#     wget \
#     libatomic1 \
#     libelf-dev \
#     libdw-dev \
#     libslang2-dev \
#     libperl-dev \
#     python3-dev \
#     libnuma-dev \
#     libtraceevent-dev
apt-get update && apt-get install -y libatomic1 libelf-dev libdw-dev libslang2-dev libperl-dev python3-dev libnuma-dev libtraceevent-dev curl
export PATH="/perf/:$PATH"

IP_ADDR=$1
P2P_PORT=$2
RANK=$3
USEPERF=$4

# echo "${IP_ADDR},${P2P_PORT},${RANK},${USEPERF}"

if [[ $RANK -eq 0 ]]; then
  NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
  ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --config-path /qdrant/config/config.yaml & 
  QDRANT_PID=$!
  echo "Rank ${RANK} qdrant is healthy"
else
  # wait until the first rank is online
  TARGET="/perf/qdrant_running0.txt"
  while [ ! -e "$TARGET" ]; do
    sleep 0.1
  done
  sleep 10
  sleep $((3 * RANK))

  bootstrapIP=$(sed -n '1p' /ip_registry.txt | cut -d',' -f2) 

  while true; do
    NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" \
    ./qdrant --uri "http://${IP_ADDR}:${P2P_PORT}" --bootstrap http://$bootstrapIP:6335 --config-path /qdrant/config/config.yaml &
    QDRANT_PID=$!
    HTTP_PORT=$((P2P_PORT - 2))
    
    # Wait for health
    healthy=false
    for i in {1..30}; do
        if NO_PROXY="" no_proxy="" http_proxy="" https_proxy="" HTTP_PROXY="" HTTPS_PROXY="" curl -fsS "http://${IP_ADDR}:${HTTP_PORT}/healthz" >/dev/null; then
            echo "Rank ${RANK} qdrant ${IP_ADDR}:{P2P_PORT} is healthy"
            sleep 3
            healthy=true
            touch /perf/qdrant_running${RANK}.txt
            break
        fi
        sleep 1
    done

     if [[ "$healthy" == true ]]; then
      echo "Rank ${RANK} qdrant ${IP_ADDR}:{P2P_PORT} is healthy"
        break   # 🚀 breaks out of the OUTER while loop
    fi

    echo "Rank ${RANK} qdrant ${IP_ADDR}:{P2P_PORT} failed to become healthy, restarting..."
    kill "$QDRANT_PID" 2>/dev/null || true
    wait "$QDRANT_PID" 2>/dev/null || true
    sleep 5
  done 
 

fi
sleep 5
touch /perf/qdrant_running${RANK}.txt

# wait until the file exists
TARGET="/perf/workflow_start.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done


if [[ "$PERF" == "RECORD" ]]; then
    echo "Rank ${RANK} Launching perf record"
    /perf/perf record -F 99 --call-graph fp -g --proc-map-timeout 5000 -o /perf/perf${RANK}.data  -p "$QDRANT_PID" &
    PERF_PID=$!

elif [[ "$PERF" == "STAT" ]]; then
    echo "Rank ${RANK} Launching perf stat"
    /perf/perf stat  -e cycles,instructions,branches,branch-misses,cache-misses -o /perf/perf${RANK}.data  -p "$QDRANT_PID" &
    PERF_PID=$!
fi


# wait until the file exists
TARGET="/perf/workflow_stop.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

# stop perf cleanly (same as Ctrl-C)
if [[ "$PERF" == "RECORD" || "$PERF" == "STAT" ]]; then
    echo "Rank ${RANK} stopping perf"
    kill -INT "$PERF_PID"
    wait "$PERF_PID"
fi


# wait until our main script signals it is safe to close
TARGET="/perf/flag.txt"
while [ ! -e "$TARGET" ]; do
  sleep 0.1
done

echo "Rank ${RANK} closing"

