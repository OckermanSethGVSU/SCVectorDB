#!/bin/bash

source "$ROOT_DIR/common/engine_schema_lib.sh"

ENGINE_NAME="weaviate"
ENGINE_SCHEMA_PREFIX="WEAVIATE"
schema_engine_init "weaviate" "$ENGINE_SCHEMA_PREFIX" "Weaviate"

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

engine_set_defaults() {
    schema_load "$ENGINE_SCHEMA_PREFIX" "$ENGINE_DIR/schema.sh"
    schema_apply_defaults "$ENGINE_SCHEMA_PREFIX"
    schema_assign_globals_from_values "$ENGINE_SCHEMA_PREFIX"
    REQUIRES_DAOS="false"
}

engine_apply_overrides() {
    schema_sync_values_from_current_globals "$ENGINE_SCHEMA_PREFIX"
    schema_apply_overrides_from_env "$ENGINE_SCHEMA_PREFIX"
    if [[ -z "${WEAVIATE_VALUES[BASE_DIR]:-}" ]]; then
        WEAVIATE_VALUES["BASE_DIR"]="$(pwd)"
    fi
    schema_assign_globals_from_values "$ENGINE_SCHEMA_PREFIX"
}

engine_validate_config() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"
}

engine_print_summary() {
    schema_print_resolved_summary "$ENGINE_SCHEMA_PREFIX"
}

engine_show_help() {
    weaviate_print_help
}

engine_iterate_matrix() {
    schema_emit_combos_recursive "$ENGINE_SCHEMA_PREFIX" 0 ""
}

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

engine_validate_combo() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"
}

engine_make_run_dir_name() {
    local timestamp
    timestamp="$(date +"%Y-%m-%d_%H_%M_%S")"

    case "$TASK" in
        insert)
            echo "${TASK}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_C${UPLOAD_CLIENTS_CURRENT}_uploadBS${UPLOAD_BATCH_CURRENT}_${timestamp}"
            ;;
        index)
            echo "${TASK}_pes2o_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_CS${CORPUS_SIZE}_${timestamp}"
            ;;
        query_bs|query_core)
            echo "${TASK}_pes2o_${STORAGE_MEDIUM}_N${NODES_CURRENT}_NP${WORKERS_PER_NODE_CURRENT}_CORES${CORES_CURRENT}_QBS${QUERY_BATCH_CURRENT}_Q${QUERY_WORKLOAD}_${timestamp}"
            ;;
    esac
}

engine_main_script_path() {
    echo "main.sh"
}

engine_emit_runtime_env() {
    schema_emit_runtime_env "$ENGINE_SCHEMA_PREFIX"

    printf 'NODES=%s\n' "$NODES_CURRENT"
    printf 'WORKERS_PER_NODE=%s\n' "$WORKERS_PER_NODE_CURRENT"
    printf 'CORES=%s\n' "$CORES_CURRENT"
    printf 'QUERY_BATCH_SIZE=%s\n' "$QUERY_BATCH_CURRENT"
    printf 'UPLOAD_BATCH_SIZE=%s\n' "$UPLOAD_BATCH_CURRENT"
    printf 'UPLOAD_CLIENTS_PER_WORKER=%s\n' "$UPLOAD_CLIENTS_CURRENT"
}

engine_copy_payload() {
    local target_dir="$1"
    copy_engine_items "$ENGINE_DIR/weaviateSetup" "$target_dir" \
        "launchWeaviateNode.sh" \
        "mapping.py"

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

    if [[ -d "$ENGINE_DIR/perf" ]]; then
        copy_optional_engine_items "$ENGINE_DIR" "$target_dir" "perf"
    fi
}
