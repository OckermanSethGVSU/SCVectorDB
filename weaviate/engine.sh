#!/bin/bash

source "$ROOT_DIR/common/engine_schema_lib.sh"

ENGINE_NAME="weaviate"
ENGINE_SCHEMA_PREFIX="WEAVIATE"
schema_engine_init "weaviate" "$ENGINE_SCHEMA_PREFIX" "Weaviate"

# Print Weaviate-specific help plus the schema variable table.
weaviate_print_help() {
    cat <<'EOF'
Weaviate Help
=============

Use `--set NAME=value` to override any variable.
Any variable may be a single value or a sweep list separated by spaces.

Command examples:
  ./pbs_submit_manager.sh --engine weaviate --set TASK=insert --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj
  ./pbs_submit_manager.sh --generate-only --engine weaviate --set TASK=query_bs --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj

Variables
---------
EOF
    echo
    schema_print_registry_table "$ENGINE_SCHEMA_PREFIX"
}

# Load Weaviate schema defaults into the shared engine contract.
engine_set_defaults() {
    schema_load "$ENGINE_SCHEMA_PREFIX" "$ENGINE_DIR/schema.sh"
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

    NODES_CURRENT="$NODES"
    WORKERS_PER_NODE_CURRENT="$WORKERS_PER_NODE"
    UPLOAD_CLIENTS_CURRENT="$UPLOAD_CLIENTS_PER_WORKER"
    QUERY_BATCH_CURRENT="$QUERY_BATCH_SIZE"
    UPLOAD_BATCH_CURRENT="$UPLOAD_BATCH_SIZE"
    CORES_CURRENT="$CORES"
    TOTAL_NODES=$((NODES_CURRENT + 1))
    JOB_NAME="${TASK}_${NODES_CURRENT}n_${WORKERS_PER_NODE_CURRENT}w_${CORES_CURRENT}c_q${QUERY_BATCH_CURRENT}"
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
        insert)
            echo "${TASK}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_C${UPLOAD_CLIENTS_CURRENT}_uploadBS${UPLOAD_BATCH_CURRENT}_${timestamp}"
            ;;
        index)
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_CS${CORPUS_SIZE}_${timestamp}"
            ;;
        query_bs|query_core)
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_QBS${QUERY_BATCH_CURRENT}_Q${QUERY_WORKLOAD}_${timestamp}"
            ;;
        query_scaling)
            local total_workers=$((NODES_CURRENT * WORKERS_PER_NODE_CURRENT))
            echo "${TASK}_${DATASET_LABEL}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_TW${total_workers}_UCPW${UPLOAD_CLIENTS_CURRENT}_QCPW${QUERY_CLIENTS_PER_WORKER}_CORES${CORES_CURRENT}_IBS${UPLOAD_BATCH_CURRENT}_QBS${QUERY_BATCH_CURRENT}_${timestamp}"
            ;;
    esac
}

# Select the Weaviate launch script copied into submit.sh.
engine_main_script_path() {
    case "$TASK" in
        query_scaling) echo "main_query_scaling.sh" ;;
        *)             echo "main.sh" ;;
    esac
}

# Emit run_config.env contents consumed by the Weaviate launch scripts.
engine_emit_runtime_env() {
    schema_emit_runtime_env "$ENGINE_SCHEMA_PREFIX"

    printf 'NODES=%s\n' "$NODES_CURRENT"
    printf 'WORKERS_PER_NODE=%s\n' "$WORKERS_PER_NODE_CURRENT"
    printf 'CORES=%s\n' "$CORES_CURRENT"
    printf 'QUERY_BATCH_SIZE=%s\n' "$QUERY_BATCH_CURRENT"
    printf 'UPLOAD_BATCH_SIZE=%s\n' "$UPLOAD_BATCH_CURRENT"
    printf 'UPLOAD_CLIENTS_PER_WORKER=%s\n' "$UPLOAD_CLIENTS_CURRENT"
}

# Stage the Weaviate launch files, client binary, and optional perf assets.
engine_copy_payload() {
    local target_dir="$1"
    copy_engine_items "$ENGINE_DIR/weaviateSetup" "$target_dir" \
        "launchWeaviateNode.sh" \
        "mapping.py"

    case "$TASK" in
        query_scaling)
            copy_engine_items "$ENGINE_DIR" "$target_dir" "main_query_scaling.sh"
            mkdir -p "$target_dir/go_client"
            if [[ -f "$ENGINE_DIR/clients/insert_pes2o_streaming/$INSERT_BIN" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/insert_pes2o_streaming" \
                    "$target_dir/go_client" "$INSERT_BIN"
            else
                echo "Required insert client binary missing: clients/insert_pes2o_streaming/$INSERT_BIN" >&2
                echo "Build it with: (cd $ENGINE_DIR/clients/insert_pes2o_streaming && ./build.sh)" >&2
                return 1
            fi
            if [[ -f "$ENGINE_DIR/clients/query_scaling/$QUERY_SCALING_BIN" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/query_scaling" \
                    "$target_dir/go_client" "$QUERY_SCALING_BIN"
            else
                echo "Required query client binary missing: clients/query_scaling/$QUERY_SCALING_BIN" >&2
                echo "Build it with: (cd $ENGINE_DIR/clients/query_scaling && ./build.sh)" >&2
                return 1
            fi
            chmod +x "$target_dir/go_client/$INSERT_BIN" "$target_dir/go_client/$QUERY_SCALING_BIN"
            ;;
        *)
            if [[ -f "$ENGINE_DIR/$WEAVIATE_CLIENT_BINARY" ]]; then
                copy_engine_items "$ENGINE_DIR" "$target_dir" "$WEAVIATE_CLIENT_BINARY"
            elif [[ -f "$ENGINE_DIR/goCode/test/$WEAVIATE_CLIENT_BINARY" ]]; then
                copy_engine_items "$ENGINE_DIR" "$target_dir" "goCode/test/$WEAVIATE_CLIENT_BINARY"
                mv "$target_dir/goCode/test/$WEAVIATE_CLIENT_BINARY" "$target_dir/$WEAVIATE_CLIENT_BINARY"
                rmdir "$target_dir/goCode/test" 2>/dev/null || true
                rmdir "$target_dir/goCode" 2>/dev/null || true
            else
                echo "Required Weaviate client binary missing: $WEAVIATE_CLIENT_BINARY" >&2
                return 1
            fi
            ;;
    esac

    if [[ -d "$ENGINE_DIR/perf" ]]; then
        copy_optional_engine_items "$ENGINE_DIR" "$target_dir" "perf"
    fi
}
