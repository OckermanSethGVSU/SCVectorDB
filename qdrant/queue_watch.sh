#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANAGER_SCRIPT="$SCRIPT_DIR/pbs_submit_manager.sh"

WATCH_VARIABLES=(DATASET CORES)

declare -A WATCH_VALUES=(
    [CORES]="128 128 128"
)
declare -A WATCH_GROUPED_VALUES=()

# Example grouped preset dimension:
# WATCH_VARIABLES=(DATASET QUERY_BATCH_SIZE CORES)
WATCH_GROUPED_VALUES[DATASET]=$'VECTOR_DIM=2560\tDISTANCE_METRIC=COSINE\tINSERT_FILEPATH=/lus/flare/projects/AuroraGPT/sockerman/pes2oEmbeddings/embeddings.npy\nVECTOR_DIM=200\tDISTANCE_METRIC=IP\tINSERT_FILEPATH=/lus/flare/projects/AuroraGPT/sockerman/text2image1B/Yandex10M.npy'
QUEUES=(debug debug-scaling)


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

validate_watch_configuration() {
    local variable_name
    local variable_values
    local grouped_values

    if [[ "${#WATCH_VARIABLES[@]}" -eq 0 ]]; then
        echo "WATCH_VARIABLES must contain at least one variable name" >&2
        exit 1
    fi

    for variable_name in "${WATCH_VARIABLES[@]}"; do
        if [[ "$variable_name" == "QUEUE" || "$variable_name" == "queue" ]]; then
            echo "QUEUE is controlled by the watcher and cannot be listed in WATCH_VARIABLES" >&2
            exit 1
        fi

        variable_values="${WATCH_VALUES[$variable_name]:-}"
        grouped_values="${WATCH_GROUPED_VALUES[$variable_name]:-}"

        if [[ -n "$variable_values" && -n "$grouped_values" ]]; then
            echo "Use either WATCH_VALUES or WATCH_GROUPED_VALUES for ${variable_name}, not both" >&2
            exit 1
        fi

        if [[ -z "$variable_values" && -z "$grouped_values" ]]; then
            echo "Missing WATCH_VALUES or WATCH_GROUPED_VALUES entry for ${variable_name}" >&2
            exit 1
        fi
    done
}

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

build_pending_specs() {
    local level="$1"
    local prefix="${2:-}"
    local variable_name
    local variable_values
    local grouped_values
    local variable_value
    local grouped_spec
    local next_prefix
    local -a parsed_values=()

    if (( level >= ${#WATCH_VARIABLES[@]} )); then
        pending+=("$prefix")
        return 0
    fi

    variable_name="${WATCH_VARIABLES[$level]}"
    variable_values="${WATCH_VALUES[$variable_name]:-}"
    grouped_values="${WATCH_GROUPED_VALUES[$variable_name]:-}"

    if [[ -n "$grouped_values" ]]; then
        while IFS= read -r grouped_spec; do
            if [[ -z "$grouped_spec" ]]; then
                continue
            fi

            if [[ -n "$prefix" ]]; then
                next_prefix="${prefix}"$'\t'"${grouped_spec}"
            else
                next_prefix="${grouped_spec}"
            fi

            build_pending_specs "$((level + 1))" "$next_prefix"
        done <<< "$grouped_values"
        return 0
    fi

    read -r -a parsed_values <<< "$variable_values"

    for variable_value in "${parsed_values[@]}"; do
        if [[ -n "$prefix" ]]; then
            next_prefix="${prefix}"$'\t'"${variable_name}=${variable_value}"
        else
            next_prefix="${variable_name}=${variable_value}"
        fi

        build_pending_specs "$((level + 1))" "$next_prefix"
    done
}

submit_one() {
    local queue_name="$1"
    local submission_spec="$2"
    local spec_part
    local override_name
    local override_value
    local -a env_args=()

    IFS=$'\t' read -r -a spec_parts <<< "$submission_spec"
    for spec_part in "${spec_parts[@]}"; do
        override_name="${spec_part%%=*}"
        override_value="${spec_part#*=}"
        env_args+=("${override_name}_OVERRIDE=${override_value}")
    done

    echo "Submitting to ${queue_name} with overrides: ${submission_spec//$'\t'/, }"
    (
        cd "$SCRIPT_DIR"
        env "QUEUE_OVERRIDE=${queue_name}" "${env_args[@]}" "$MANAGER_SCRIPT"
    )
}

validate_watch_configuration

pending=()
build_pending_specs 0

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
        submit_one "$open_queue" "${pending[$index]}"
        index=$((index + 1))
        sleep 5
        continue
    fi

    echo "No opening in configured queues. Checking again in ${POLL_SECONDS}s."
    sleep "$POLL_SECONDS"
done

echo "Submitted ${#pending[@]} jobs."
