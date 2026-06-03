#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANAGER_SCRIPT="$SCRIPT_DIR/pbs_submit_manager.sh"

CORES=(128 64 32 16 8 4 2 1)
QUERY_BATCH_SIZES=(2048)
QUEUES=(capacity)
declare -A QUEUE_CAPACITY=(
    [debug]=1
    [debug-scaling]=1
    [capacity]=5
)
declare -A QUEUE_Q_CAPACITY=(
    [debug]=1
    [debug-scaling]=1
    [capacity]=5
)
GROUP_LIMIT_NAMES=()
declare -A GROUP_LIMIT_QUEUES=()
declare -A GROUP_LIMIT_CAPACITY=()
declare -A GROUP_LIMIT_STATE_PATTERN=()

# Example grouped queued-job limit:
# GROUP_LIMIT_NAMES=(generic_q)
# GROUP_LIMIT_QUEUES[generic_q]="capacity prod preemptable"
# GROUP_LIMIT_CAPACITY[generic_q]=5
# GROUP_LIMIT_STATE_PATTERN[generic_q]="^Q$"

POLL_SECONDS="${POLL_SECONDS:-30}"

if [[ ! -x "$MANAGER_SCRIPT" ]]; then
    echo "Missing executable manager script: $MANAGER_SCRIPT" >&2
    exit 1
fi

if ! command -v qstat >/dev/null 2>&1; then
    echo "qstat is required but not available in PATH" >&2
    exit 1
fi

active_jobs_in_queue() {
    local queue_name="$1"
    local state_pattern="${2:-[A-Z]}"

    qstat -u "${USER}" 2>/dev/null | awk -v target_queue="$queue_name" -v target_state_pattern="$state_pattern" '
        function queue_matches(queue_field, requested_queue) {
            if (requested_queue == "debug") {
                return queue_field == "debug"
            }

            if (requested_queue == "debug-scaling") {
                return queue_field == "debug-scaling" || queue_field ~ /^debug-s/
            }

            return queue_field == requested_queue
        }

        $1 !~ /^[0-9]/ {
            next
        }

        {
            queue = $3
            state = $(NF - 1)

            if (queue_matches(queue, target_queue) && state ~ target_state_pattern) {
                count++
            }
        }

        END {
            print count + 0
        }
    '
}

count_jobs_in_queue_group() {
    local queue_list="$1"
    local state_pattern="${2:-[A-Z]}"

    qstat -u "${USER}" 2>/dev/null | awk -v target_queues="$queue_list" -v target_state_pattern="$state_pattern" '
        BEGIN {
            split(target_queues, queues, " ")
            for (i in queues) {
                wanted[queues[i]] = 1
            }
        }

        $1 !~ /^[0-9]/ {
            next
        }

        {
            queue = $3
            state = $(NF - 1)

            if (wanted[queue] && state ~ target_state_pattern) {
                count++
            }
        }

        END {
            print count + 0
        }
    '
}

group_limits_allow_queue() {
    local candidate_queue="$1"
    local group_name
    local group_queues
    local group_capacity
    local group_state_pattern
    local group_jobs

    for group_name in "${GROUP_LIMIT_NAMES[@]}"; do
        group_queues="${GROUP_LIMIT_QUEUES[$group_name]}"

        if [[ ! " $group_queues " =~ [[:space:]]$candidate_queue[[:space:]] ]]; then
            continue
        fi

        group_capacity="${GROUP_LIMIT_CAPACITY[$group_name]}"
        group_state_pattern="${GROUP_LIMIT_STATE_PATTERN[$group_name]:-[A-Z]}"
        group_jobs="$(count_jobs_in_queue_group "$group_queues" "$group_state_pattern")"

        if (( group_jobs >= group_capacity )); then
            return 1
        fi
    done

    return 0
}

find_open_queue() {
    local queue_name
    local active_jobs
    local queue_capacity
    local queued_jobs
    local queued_capacity

    for queue_name in "${QUEUES[@]}"; do
        active_jobs="$(active_jobs_in_queue "$queue_name")"
        queue_capacity="${QUEUE_CAPACITY[$queue_name]:-1}"
        queued_jobs="$(active_jobs_in_queue "$queue_name" "^Q$")"
        queued_capacity="${QUEUE_Q_CAPACITY[$queue_name]:-$queue_capacity}"

        if (( active_jobs < queue_capacity && queued_jobs < queued_capacity )) && group_limits_allow_queue "$queue_name"; then
            printf '%s\n' "$queue_name"
            return 0
        fi
    done

    return 1
}

submit_one() {
    local queue_name="$1"
    local core_count="$2"
    local query_batch_size="$3"

    echo "Submitting QUERY batch size ${query_batch_size} with ${core_count} cores to ${queue_name}"
    (
        cd "$SCRIPT_DIR"
        QUEUE_OVERRIDE="$queue_name" \
        CORES_OVERRIDE="$core_count" \
        QUERY_BATCH_SIZE_OVERRIDE="$query_batch_size" \
        "$MANAGER_SCRIPT"
    )
}

pending=()
for batch_size in "${QUERY_BATCH_SIZES[@]}"; do
    for core_count in "${CORES[@]}"; do
        pending+=("${batch_size}:${core_count}")
    done
done

index=0
while [[ "$index" -lt "${#pending[@]}" ]]; do
    occupancy_parts=()
    for queue_name in "${QUEUES[@]}"; do
        active_jobs="$(active_jobs_in_queue "$queue_name")"
        queue_capacity="${QUEUE_CAPACITY[$queue_name]:-1}"
        queued_jobs="$(active_jobs_in_queue "$queue_name" "^Q$")"
        queued_capacity="${QUEUE_Q_CAPACITY[$queue_name]:-$queue_capacity}"
        occupancy_parts+=("${queue_name}=jobs:${active_jobs}/${queue_capacity},queued:${queued_jobs}/${queued_capacity}")
    done

    echo "Queue occupancy: ${occupancy_parts[*]}"

    for group_name in "${GROUP_LIMIT_NAMES[@]}"; do
        group_queues="${GROUP_LIMIT_QUEUES[$group_name]}"
        group_capacity="${GROUP_LIMIT_CAPACITY[$group_name]}"
        group_state_pattern="${GROUP_LIMIT_STATE_PATTERN[$group_name]:-[A-Z]}"
        group_jobs="$(count_jobs_in_queue_group "$group_queues" "$group_state_pattern")"
        echo "Group limit ${group_name}: ${group_jobs}/${group_capacity} queues=[${group_queues}] state=${group_state_pattern}"
    done

    if open_queue="$(find_open_queue)"; then
        IFS=: read -r batch_size core_count <<< "${pending[$index]}"
        submit_one "$open_queue" "$core_count" "$batch_size"
        index=$((index + 1))
        sleep 5
        continue
    fi

    echo "No opening in configured queues. Checking again in ${POLL_SECONDS}s."
    sleep "$POLL_SECONDS"
done

echo "Submitted ${#pending[@]} jobs."
