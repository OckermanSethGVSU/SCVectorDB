#!/bin/bash

source "$ROOT_DIR/common/engine_schema_lib.sh"

ENGINE_NAME="weaviate"
ENGINE_SCHEMA_PREFIX="WEAVIATE"
schema_engine_init "weaviate" "$ENGINE_SCHEMA_PREFIX" "Weaviate"

weaviate_cores_label() {
    local cores_value="${CORES_CURRENT:-${CORES:-}}"

    if [[ -z "$cores_value" ]]; then
        printf '%s\n' "none"
    else
        printf '%s\n' "$cores_value"
    fi
}

# Print Weaviate-specific help plus the schema variable table.
weaviate_print_help() {
    cat <<'EOF'
Weaviate Help
=============

Use `--set NAME=value` to override any variable.
Any variable may be a single value or a sweep list separated by spaces.

Command examples:
  ./pbs_submit_manager.sh --engine weaviate --set TASK=INSERT --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj
  ./pbs_submit_manager.sh --generate-only --engine weaviate --set TASK=QUERY_BS --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj

Variables
---------
EOF
    echo
    schema_print_registry_table "$ENGINE_SCHEMA_PREFIX"
}

# Load Weaviate schema defaults into the shared engine contract.
engine_set_defaults() {
    schema_load "$ENGINE_SCHEMA_PREFIX" "$ROOT_DIR/common/schema.sh"
    schema_append_file "$ENGINE_SCHEMA_PREFIX" "$ENGINE_DIR/schema.sh"
    schema_apply_defaults "$ENGINE_SCHEMA_PREFIX"
    schema_assign_globals_from_values "$ENGINE_SCHEMA_PREFIX"
    REQUIRES_DAOS="false"
}

# Apply --set/env overrides and fill BASE_DIR from the current directory.
engine_apply_overrides() {
    schema_sync_values_from_current_globals "$ENGINE_SCHEMA_PREFIX"
    schema_apply_overrides_from_env "$ENGINE_SCHEMA_PREFIX"
    if [[ -z "${WEAVIATE_VALUES[BASE_DIR]:-}" ]]; then
        WEAVIATE_VALUES["BASE_DIR"]="$(pwd)"
    fi
    schema_assign_globals_from_values "$ENGINE_SCHEMA_PREFIX"
}

# Validate required values and schema choices after overrides are resolved.
engine_validate_config() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"
}

# Print resolved Weaviate settings before run directory generation/submission.
engine_print_summary() {
    schema_print_resolved_summary "$ENGINE_SCHEMA_PREFIX"
}

# Entry point used by the root submit manager for --help --engine weaviate.
engine_show_help() {
    weaviate_print_help
}

# Emit all schema sweep combinations for the root submit manager.
engine_iterate_matrix() {
    schema_emit_combos_recursive "$ENGINE_SCHEMA_PREFIX" 0 ""
}

# Load one matrix combo into scalar globals used by naming and env emission.
engine_load_combo() {
    local assignment
    local key
    local value

    IFS=';' read -r -a assignments <<< "$1"
    for assignment in "${assignments[@]}"; do
        [[ -n "$assignment" ]] || continue
        key="${assignment%%=*}"
        value="${assignment#*=}"
        printf -v "$key" '%s' "$value"
    done

    TASK="${TASK^^}"

    NODES_CURRENT="$NODES"
    WORKERS_PER_NODE_CURRENT="$WORKERS_PER_NODE"
    INSERT_CLIENTS_CURRENT="$INSERT_CLIENTS_PER_WORKER"
    QUERY_BATCH_CURRENT="$QUERY_BATCH_SIZE"
    INSERT_BATCH_CURRENT="$INSERT_BATCH_SIZE"
    CORES_CURRENT="$CORES"
    TOTAL_NODES=$((NODES_CURRENT + 1))
    JOB_NAME="${TASK}_${NODES_CURRENT}n_${WORKERS_PER_NODE_CURRENT}w_$(weaviate_cores_label)c_q${QUERY_BATCH_CURRENT}"
    REQUIRES_DAOS="false"
}

# Validate a loaded combo after sweep expansion.
engine_validate_combo() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"
}

# Build the Weaviate run directory name for the loaded combo.
engine_make_run_dir_name() {
    local timestamp
    timestamp="$(date +"%Y-%m-%d_%H_%M_%S")"

    case "$TASK" in
        INSERT)
            echo "${TASK}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_C${INSERT_CLIENTS_CURRENT}_uploadBS${INSERT_BATCH_CURRENT}_${timestamp}"
            ;;
        INDEX)
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES$(weaviate_cores_label)_CS${INSERT_CORPUS_SIZE}_${timestamp}"
            ;;
        QUERY_BS|QUERY_CORE)
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES$(weaviate_cores_label)_QBS${QUERY_BATCH_CURRENT}_Q${QUERY_WORKLOAD}_${timestamp}"
            ;;
        QUERY_SCALING)
            local total_workers=$((NODES_CURRENT * WORKERS_PER_NODE_CURRENT))
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_TW${total_workers}_UCPW${INSERT_CLIENTS_CURRENT}_QCPW${QUERY_CLIENTS_PER_WORKER}_CORES$(weaviate_cores_label)_IBS${INSERT_BATCH_CURRENT}_QBS${QUERY_BATCH_CURRENT}_${timestamp}"
            ;;
    esac
}

# Select the Weaviate launch script copied into submit.sh.
engine_main_script_path() {
    echo "main.sh"
}

# Emit run_config.env contents consumed by the Weaviate launch scripts.
engine_emit_runtime_env() {
    schema_emit_runtime_env "$ENGINE_SCHEMA_PREFIX"

    printf 'NODES=%s\n' "$NODES_CURRENT"
    printf 'WORKERS_PER_NODE=%s\n' "$WORKERS_PER_NODE_CURRENT"
    printf 'CORES=%s\n' "$CORES_CURRENT"
    printf 'QUERY_BATCH_SIZE=%s\n' "$QUERY_BATCH_CURRENT"
    printf 'INSERT_BATCH_SIZE=%s\n' "$INSERT_BATCH_CURRENT"
    printf 'INSERT_CLIENTS_PER_WORKER=%s\n' "$INSERT_CLIENTS_CURRENT"
}

# Stage the Weaviate launch files, client binaries, and optional perf assets.
engine_copy_payload() {
    local target_dir="$1"
    copy_engine_items "$ENGINE_DIR/weaviateSetup" "$target_dir" \
        "launchWeaviateNode.sh" \
        "mapping.py"

    local -a bins
    case "$TASK" in
        QUERY_SCALING) bins=("$INSERT_BIN" "$QUERY_SCALING_BIN") ;;
        *)             bins=("$WEAVIATE_CLIENT_BINARY") ;;
    esac

    mkdir -p "$target_dir/go_client"
    for bin in "${bins[@]}"; do
        if [[ ! -f "$ENGINE_DIR/clients/go_client/$bin" ]]; then
            echo "Required client binary missing: clients/go_client/$bin" >&2
            echo "Build it with: (cd $ENGINE_DIR/clients/go_client && ./build.sh $bin)" >&2
            return 1
        fi
    done
    copy_engine_items "$ENGINE_DIR/clients/go_client" "$target_dir/go_client" "${bins[@]}"
    for bin in "${bins[@]}"; do
        chmod +x "$target_dir/go_client/$bin"
    done

    if [[ -d "$ENGINE_DIR/perf" ]]; then
        copy_optional_engine_items "$ENGINE_DIR" "$target_dir" "perf"
    fi
}
