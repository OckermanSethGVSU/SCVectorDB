#!/bin/bash



mkdir -p configs/
mkdir -p /dev/shm/oltp
mv otel_config.yaml ./configs

apptainer exec --no-home --fakeroot --writable-tmpfs \
  --env http_proxy= --env https_proxy= --env HTTP_PROXY= --env HTTPS_PROXY= --env NO_PROXY= --env no_proxy=  \
  -B ./configs/otel_config.yaml:/otel_config.yaml \
  -B /dev/shm/oltp:/var/lib/oltp \
  otel-collector.sif /otelcol-contrib --config /otel_config.yaml > otel.out  2>&1 &

OTEL_PID=$!


SENTINEL_FILE="./flag.txt"

while [[ ! -f "$SENTINEL_FILE" ]]; do
    sleep 1
done
kill -INT "$OTEL_PID"
wait "$OTEL_PID" || true

echo "Copying trace"
cp /dev/shm/oltp/traces.jsonl . || echo "No traces.jsonl found"