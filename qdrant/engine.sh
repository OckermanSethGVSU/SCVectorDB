#!/bin/bash

source "$ROOT_DIR/common/engine_schema_lib.sh"
source "$ENGINE_DIR/utils/utils.sh"

ENGINE_NAME="qdrant"
ENGINE_SCHEMA_PREFIX="QDRANT"
schema_engine_init "qdrant" "$ENGINE_SCHEMA_PREFIX" "Qdrant"

qdrant_print_help() {
    cat <<'EOF'
Qdrant Help
===========

Use `--set NAME=value` to override any variable.
Any variable may be a single value or a sweep list separated by spaces.

Command examples:
  ./pbs_submit_manager.sh --engine qdrant --set TASK=QUERY --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj
  ./pbs_submit_manager.sh --generate-only --engine qdrant --set TASK=QUERY --set PLATFORM=AURORA --set WALLTIME=01:00:00 --set QUEUE=debug-scaling --set ACCOUNT=myproj

Variables
---------
EOF
    echo
    schema_print_registry_table "$ENGINE_SCHEMA_PREFIX"
}

qdrant_cores_label() {
    if [[ -n "${CORES_CURRENT:-}" ]]; then
        printf '%s\n' "$CORES_CURRENT"
    else
        printf '%s\n' "none"
    fi
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
    schema_assign_globals_from_values "$ENGINE_SCHEMA_PREFIX"
    apply_scalar_override INSERT_START_ID
}

engine_validate_config() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"

    if [[ -z "${INSERT_START_ID:-}" ]]; then
        if [[ -n "$RESTORE_DIR" ]]; then
            INSERT_START_ID=$EXPECTED_CORPUS_SIZE
        elif [[ -n "$INSERT_CORPUS_SIZE" ]]; then
            INSERT_START_ID=$INSERT_CORPUS_SIZE
        elif [[ -n "$INSERT_FILEPATH" ]]; then
            INSERT_START_ID="$(python3 -c 'import numpy as np, sys; arr = np.load(sys.argv[1], mmap_mode="r"); print(arr.shape[0])' "$INSERT_FILEPATH")"
        else
            INSERT_START_ID=0
        fi
    fi
}

engine_print_summary() {
    schema_print_resolved_summary "$ENGINE_SCHEMA_PREFIX"
}

engine_show_help() {
    qdrant_print_help
}

engine_preview_combo() {
    local run_dir_name="$1"

    cat <<EOF
- run_dir: ${run_dir_name}
  task: ${TASK}
  nodes: ${NODES_CURRENT}
  workers_per_node: ${WORKERS_PER_NODE_CURRENT}
  cores: ${CORES_CURRENT}
  insert_batch_size: ${INSERT_BATCH_CURRENT}
  query_batch_size: ${QUERY_BATCH_CURRENT}
EOF
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
    QUERY_BATCH_CURRENT="$QUERY_BATCH_SIZE"
    INSERT_BATCH_CURRENT="$INSERT_BATCH_SIZE"
    CORES_CURRENT="$CORES"
    TOTAL_NODES=$((NODES_CURRENT + 1))
    JOB_NAME="${TASK,,}_${NODES_CURRENT}n_${WORKERS_PER_NODE_CURRENT}w_$(qdrant_cores_label)c_q${QUERY_BATCH_CURRENT}"
    REQUIRES_DAOS="false"
}

engine_validate_combo() {
    schema_validate_current_values "$ENGINE_SCHEMA_PREFIX"
}

engine_make_run_dir_name() {
    local timestamp

    timestamp="$(date +"%Y-%m-%d_%H_%M_%S")"
    echo "${TASK}_${STORAGE_MEDIUM}_N${NODES_CURRENT}_${timestamp}"
}

engine_main_script_path() {
    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
        echo "local_main.sh"
    else
        echo "main.sh"
    fi
}

engine_emit_runtime_env() {
    schema_emit_runtime_env "$ENGINE_SCHEMA_PREFIX"
    printf 'INSERT_START_ID=%s\n' "${INSERT_START_ID:-}"
}

engine_copy_payload() {
    local target_dir="$1"

    mkdir -p "$target_dir/rustSrc"
    if [[ -d "$ENGINE_DIR/runtime_state" ]]; then
        copy_optional_engine_items "$ENGINE_DIR" "$target_dir" "runtime_state"
    fi

    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
        copy_engine_items "$ENGINE_DIR/clients/batch_client" "$target_dir" "batch_client"
        copy_engine_items "$ENGINE_DIR/clients/batch_client/src" "$target_dir/rustSrc" "main.rs"
        if [[ "$TASK" == "MIXED" ]]; then
            if [[ -f "$ENGINE_DIR/clients/mixed/target/release/mixed" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/mixed/target/release" "$target_dir" "mixed"
            elif [[ -f "$ENGINE_DIR/clients/mixed/mixed" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/mixed" "$target_dir" "mixed"
            else
                echo "Required mixed client binary missing for TASK=MIXED" >&2
                return 1
            fi
            cp "$ENGINE_DIR/clients/mixed/src/main.rs" "$target_dir/rustSrc/mixed_main.rs"
        fi
    else
        copy_engine_items "$ENGINE_DIR" "$target_dir" "qdrant.sif"
        copy_engine_items "$ENGINE_DIR/runtime/cluster" "$target_dir" "launchQdrantNode.sh" "launch.sh"

        if [[ -n "$QDRANT_EXECUTABLE" ]]; then
            copy_engine_items "$ENGINE_DIR/qdrantBuilds" "$target_dir" "$QDRANT_EXECUTABLE"
            mv "$target_dir/$QDRANT_EXECUTABLE" "$target_dir/qdrant"
        fi

        copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "profile.py" "gen_dirs.py" "mapping.py"

        copy_engine_items "$ENGINE_DIR/clients/batch_client" "$target_dir" "batch_client"
        copy_engine_items "$ENGINE_DIR/clients/batch_client/src" "$target_dir/rustSrc" "main.rs"
        if [[ "$TASK" == "MIXED" ]]; then
            if [[ -f "$ENGINE_DIR/clients/mixed/target/release/mixed" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/mixed/target/release" "$target_dir" "mixed"
            elif [[ -f "$ENGINE_DIR/clients/mixed/mixed" ]]; then
                copy_engine_items "$ENGINE_DIR/clients/mixed" "$target_dir" "mixed"
            else
                echo "Required mixed client binary missing for TASK=MIXED" >&2
                return 1
            fi
            cp "$ENGINE_DIR/clients/mixed/src/main.rs" "$target_dir/rustSrc/mixed_main.rs"
        fi
    fi

    if [[ -n "$RESTORE_DIR" ]]; then
        copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "fix_peer_id.py" "collection_status.py"
    fi

    copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "summarize_client_timings.py"

    if [[ -z "$RESTORE_DIR" ]]; then
        copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "configure_collection.py"
    fi

    if [[ "$TASK" == "INDEX" || "$TASK" == "QUERY" || "$TASK" == "MIXED" ]]; then
        copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "build_index.py"
    fi

    if [[ "$TASK" == "MIXED" ]]; then
        copy_engine_items "$ENGINE_DIR/scripts" "$target_dir" "mixed_timeline.py"
    fi
}
