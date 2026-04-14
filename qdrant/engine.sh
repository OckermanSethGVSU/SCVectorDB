#!/bin/bash

source "$ENGINE_DIR/utils/utils.sh"

ENGINE_NAME="qdrant"

declare -ag QDRANT_VAR_ORDER=()
declare -gA QDRANT_REQUIRED_KIND=()
declare -gA QDRANT_REQUIRED_IF=()
declare -gA QDRANT_DEFAULT=()
declare -gA QDRANT_CHOICES=()
declare -gA QDRANT_DESC=()
declare -gA QDRANT_VALUES=()

qdrant_schema_reset() {
    QDRANT_VAR_ORDER=()
    QDRANT_REQUIRED_KIND=()
    QDRANT_REQUIRED_IF=()
    QDRANT_DEFAULT=()
    QDRANT_CHOICES=()
    QDRANT_DESC=()
    QDRANT_VALUES=()
}

register_qdrant_var() {
    local name="$1"
    local required_kind="$2"
    local default_value="$3"
    local choices="$4"
    local description="$5"
    local required_if="${6:-}"

    QDRANT_VAR_ORDER+=("$name")
    QDRANT_REQUIRED_KIND["$name"]="$required_kind"
    QDRANT_REQUIRED_IF["$name"]="$required_if"
    QDRANT_DEFAULT["$name"]="$default_value"
    QDRANT_CHOICES["$name"]="$choices"
    QDRANT_DESC["$name"]="$description"
}

qdrant_load_schema() {
    qdrant_schema_reset
    source "$ENGINE_DIR/schema.sh"
}

qdrant_split_raw_values() {
    local raw_value="$1"
    local output_name="$2"
    local -n output_ref="$output_name"

    if [[ -z "$raw_value" ]]; then
        output_ref=("")
    else
        read -r -a output_ref <<< "$raw_value"
        if (( ${#output_ref[@]} == 0 )); then
            output_ref=("")
        fi
    fi
}

qdrant_first_raw_value() {
    local raw_value="$1"
    local values=()

    qdrant_split_raw_values "$raw_value" values
    printf '%s\n' "${values[0]}"
}

qdrant_assign_globals_from_values() {
    local var_name
    local first_value

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        first_value="$(qdrant_first_raw_value "${QDRANT_VALUES[$var_name]-}")"
        printf -v "$var_name" '%s' "$first_value"
    done

    if [[ -n "${QUEUE:-}" ]]; then
        queue="$QUEUE"
    fi
}

qdrant_sync_values_from_current_globals() {
    local var_name
    local raw_value

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        if raw_value="$(get_var_as_raw_string "$var_name" 2>/dev/null)"; then
            QDRANT_VALUES["$var_name"]="$raw_value"
        fi
    done
}

qdrant_apply_defaults() {
    local var_name

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        QDRANT_VALUES["$var_name"]="${QDRANT_DEFAULT[$var_name]}"
    done
}

qdrant_apply_overrides_from_env() {
    local var_name
    local override_name
    local uppercase_override_name

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        override_name="${var_name}_OVERRIDE"
        uppercase_override_name="${var_name^^}_OVERRIDE"

        if [[ -n "${!override_name:-}" ]]; then
            QDRANT_VALUES["$var_name"]="${!override_name}"
        elif [[ -n "${!uppercase_override_name:-}" ]]; then
            QDRANT_VALUES["$var_name"]="${!uppercase_override_name}"
        fi
    done
}

qdrant_value_in_choices() {
    local value="$1"
    local choices_raw="$2"

    [[ -z "$choices_raw" ]] && return 0
    [[ -z "$value" ]] && return 0

    local choices=()
    qdrant_split_raw_values "$choices_raw" choices

    local choice
    for choice in "${choices[@]}"; do
        if [[ "$value" == "$choice" ]]; then
            return 0
        fi
    done

    return 1
}

qdrant_condition_matches_current() {
    local condition="$1"
    local lhs
    local rhs
    local current
    local allowed_values=()
    local allowed

    [[ -z "$condition" ]] && return 0

    lhs="${condition%%=*}"
    rhs="${condition#*=}"
    current="$(get_var_as_raw_string "$lhs" 2>/dev/null || true)"
    current="$(qdrant_first_raw_value "$current")"

    qdrant_split_raw_values "${rhs//|/ }" allowed_values
    for allowed in "${allowed_values[@]}"; do
        if [[ "$current" == "$allowed" ]]; then
            return 0
        fi
    done

    return 1
}

qdrant_var_is_required_current() {
    local var_name="$1"
    local required_kind="${QDRANT_REQUIRED_KIND[$var_name]}"

    case "$required_kind" in
        required)
            return 0
            ;;
        conditional)
            qdrant_condition_matches_current "${QDRANT_REQUIRED_IF[$var_name]}"
            return $?
            ;;
        *)
            return 1
            ;;
    esac
}

qdrant_validate_current_values() {
    local var_name
    local raw_value
    local values=()
    local value

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        raw_value="${QDRANT_VALUES[$var_name]-}"
        qdrant_split_raw_values "$raw_value" values

        if qdrant_var_is_required_current "$var_name"; then
            if [[ -z "${values[0]}" ]]; then
                echo "Qdrant variable '$var_name' is required for the current settings." >&2
                return 1
            fi
        fi

        for value in "${values[@]}"; do
            if ! qdrant_value_in_choices "$value" "${QDRANT_CHOICES[$var_name]}"; then
                echo "Qdrant variable '$var_name' has invalid value '$value'. Allowed: ${QDRANT_CHOICES[$var_name]}" >&2
                return 1
            fi
        done
    done
}

qdrant_print_registry_table() {
    local include_mode="$1"
    local var_name
    local required_label
    local default_value

    printf "%-28s %-16s %-18s %-28s %s\n" "VARIABLE" "VALUE MODE" "REQUIRED" "DEFAULT" "CHOICES / NOTES"
    printf "%-28s %-16s %-18s %-28s %s\n" "--------" "----------" "--------" "-------" "---------------"

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        required_label="${QDRANT_REQUIRED_KIND[$var_name]}"
        if [[ -n "${QDRANT_REQUIRED_IF[$var_name]}" ]]; then
            required_label="${required_label}:${QDRANT_REQUIRED_IF[$var_name]}"
        fi

        default_value="${QDRANT_DEFAULT[$var_name]}"
        [[ -z "$default_value" ]] && default_value="<empty>"

        case "$include_mode" in
            defaults)
                printf "%-28s %-16s %-18s %-28s %s\n" "$var_name" "single|sweep" "$required_label" "$default_value" "${QDRANT_DESC[$var_name]}"
                ;;
            options)
                printf "%-28s %-16s %-18s %-28s %s\n" "$var_name" "single|sweep" "$required_label" "$default_value" "${QDRANT_CHOICES[$var_name]:-${QDRANT_DESC[$var_name]}}"
                ;;
            vars)
                printf "%-28s %-16s %-18s %-28s %s\n" "$var_name" "single|sweep" "$required_label" "$default_value" "${QDRANT_DESC[$var_name]}"
                ;;
        esac
    done
}

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
    qdrant_print_registry_table "vars"
}

qdrant_print_resolved_summary() {
    local var_name

    echo "Resolved Qdrant settings"
    echo "========================"
    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        printf "%-28s %s\n" "$var_name" "${QDRANT_VALUES[$var_name]-<unset>}"
    done
    echo
}

qdrant_cores_label() {
    if [[ -n "${CORES_CURRENT:-}" ]]; then
        printf '%s\n' "$CORES_CURRENT"
    else
        printf '%s\n' "none"
    fi
}

qdrant_cores_segment() {
    if [[ -n "${CORES_CURRENT:-}" ]]; then
        printf '%s\n' "_CORES$(qdrant_cores_label)"
    else
        printf '%s\n' ""
    fi
}

qdrant_emit_combos_recursive() {
    local index="$1"
    local prefix="$2"
    local var_name
    local values=()
    local value
    local next_prefix

    if (( index >= ${#QDRANT_VAR_ORDER[@]} )); then
        echo "${prefix#;}"
        return 0
    fi

    var_name="${QDRANT_VAR_ORDER[$index]}"
    qdrant_split_raw_values "${QDRANT_VALUES[$var_name]-}" values

    for value in "${values[@]}"; do
        next_prefix="${prefix};${var_name}=${value}"
        qdrant_emit_combos_recursive $((index + 1)) "$next_prefix"
    done
}

engine_set_defaults() {
    qdrant_load_schema
    qdrant_apply_defaults
    qdrant_assign_globals_from_values
    REQUIRES_DAOS="false"
}

engine_apply_overrides() {
    qdrant_sync_values_from_current_globals
    qdrant_apply_overrides_from_env
    qdrant_assign_globals_from_values
}

engine_validate_config() {
    qdrant_validate_current_values

    if [[ -z "${INSERT_START_ID:-}" ]]; then
        if [[ -n "$RESTORE_DIR" ]]; then
            INSERT_START_ID=$EXPECTED_CORPUS_SIZE
        else
            INSERT_START_ID=$INSERT_CORPUS_SIZE
        fi
    fi
}

engine_print_summary() {
    qdrant_print_resolved_summary
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
    qdrant_emit_combos_recursive 0 ""
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
    qdrant_validate_current_values
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
    local var_name
    local raw_value

    for var_name in "${QDRANT_VAR_ORDER[@]}"; do
        raw_value="$(get_var_as_raw_string "$var_name" 2>/dev/null || true)"
        printf '%s=%s\n' "$var_name" "$raw_value"
    done
}

engine_copy_payload() {
    local target_dir="$1"

    mkdir -p "$target_dir/rustSrc"
    if [[ -d "$ENGINE_DIR/runtime_state" ]]; then
        copy_optional_engine_items "$ENGINE_DIR" "$target_dir" "runtime_state"
    fi

    if [[ "${RUN_MODE^^}" == "LOCAL" ]]; then
        copy_engine_items "$ENGINE_DIR/clients/standard" "$target_dir" "standard"
        copy_engine_items "$ENGINE_DIR/clients/standard/src" "$target_dir/rustSrc" "main.rs"
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

        copy_engine_items "$ENGINE_DIR/clients/standard" "$target_dir" "standard"
        copy_engine_items "$ENGINE_DIR/clients/standard/src" "$target_dir/rustSrc" "main.rs"
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
