#!/usr/bin/env bash

SRC_DIR=$1
DST_ROOT=$2
THRESHOLD_MB=${3:-100}
MAX_PARALLEL=${4:-8}

THRESHOLD_BYTES=$((THRESHOLD_MB * 1024 * 1024))

# only keep the final directory name
BASE_NAME=$(basename "$SRC_DIR")
DST_DIR="$DST_ROOT/$BASE_NAME"

mkdir -p "$DST_DIR"

batch_size=0
batch_files=()
pids=()

run_batch() {
    local files=("$@")

    (
        cp -r "${files[@]}" "$DST_DIR"
    ) &

    pids+=($!)

    # limit parallelism
    while [ "${#pids[@]}" -ge "$MAX_PARALLEL" ]; do
        wait -n
        new_pids=()
        for pid in "${pids[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                new_pids+=("$pid")
            fi
        done
        pids=("${new_pids[@]}")
    done
}

while IFS= read -r -d '' file; do
    size=$(du -sb "$file" | cut -f1)

    batch_files+=("$file")
    batch_size=$((batch_size + size))

    if (( batch_size >= THRESHOLD_BYTES )); then
        run_batch "${batch_files[@]}"
        batch_files=()
        batch_size=0
    fi

done < <(find "$SRC_DIR" -mindepth 1 -maxdepth 1 -print0)

# flush remaining files
if (( ${#batch_files[@]} > 0 )); then
    run_batch "${batch_files[@]}"
fi

wait
echo "Copy complete → $DST_DIR"