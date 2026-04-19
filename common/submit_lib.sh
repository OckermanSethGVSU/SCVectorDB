#!/bin/bash

set -euo pipefail

print_usage() {
    cat <<'EOF'
Usage:
  ./pbs_submit_manager.sh --help [--engine <engine>]
  ./pbs_submit_manager.sh --engine <engine> [--config FILE] [--set KEY=VALUE ...]
  ./pbs_submit_manager.sh --generate-only --engine <engine> [--config FILE] [--set KEY=VALUE ...]

Examples:
  ./pbs_submit_manager.sh --help --engine qdrant
  ./pbs_submit_manager.sh --engine qdrant
  ./pbs_submit_manager.sh --config qdrant.env
  ./pbs_submit_manager.sh --generate-only --engine qdrant --set TASK=QUERY --set QUERY_BATCH_SIZE="32 64"
  ENGINE=qdrant ./pbs_submit_manager.sh
EOF
}

apply_cli_set_overrides() {
    local assignment
    local key
    local value

    for assignment in "$@"; do
        if [[ "$assignment" != *=* ]]; then
            echo "Invalid --set value '$assignment'. Expected KEY=VALUE." >&2
            return 1
        fi

        key="${assignment%%=*}"
        value="${assignment#*=}"
        export "${key}_OVERRIDE=$value"
    done
}

trim_whitespace() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s\n' "$value"
}

strip_outer_quotes() {
    local value="$1"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
        value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
        value="${value:1:${#value}-2}"
    fi
    printf '%s\n' "$value"
}

load_config_overrides_file() {
    local path="$1"

    if [[ ! -f "$path" ]]; then
        echo "Config file not found: $path" >&2
        return 1
    fi

    local line_num=0
    local line
    local key
    local value

    while IFS= read -r line || [[ -n "$line" ]]; do
        line_num=$((line_num + 1))
        line="$(trim_whitespace "$line")"

        [[ -z "$line" ]] && continue
        [[ "$line" == \#* ]] && continue

        if [[ "$line" != *=* ]]; then
            echo "Invalid config line $line_num in $path: expected KEY=value" >&2
            return 1
        fi

        key="$(trim_whitespace "${line%%=*}")"
        value="$(trim_whitespace "${line#*=}")"
        value="$(strip_outer_quotes "$value")"

        if [[ -z "$key" ]]; then
            echo "Invalid config line $line_num in $path: empty key" >&2
            return 1
        fi

        if [[ "$key" == "ENGINE" && -z "${ENGINE:-}" ]]; then
            ENGINE="$value"
        fi

        export "${key}_OVERRIDE=$value"
    done < "$path"
}

print_file_with_header() {
    local title="$1"
    local path="$2"

    if [[ ! -f "$path" ]]; then
        echo "No $title file found at $path" >&2
        return 1
    fi

    echo "$title: $path"
    echo
    cat "$path"
}

get_var_as_raw_string() {
    local var_name="$1"

    if ! declare -p "$var_name" >/dev/null 2>&1; then
        return 1
    fi

    local decl
    decl="$(declare -p "$var_name" 2>/dev/null || true)"
    if [[ "$decl" == declare\ -a* ]]; then
        local -n arr_ref="$var_name"
        printf '%s\n' "${arr_ref[*]}"
    else
        local -n scalar_ref="$var_name"
        printf '%s\n' "$scalar_ref"
    fi
}

apply_scalar_override() {
    local var_name="$1"
    local override_name="${var_name}_OVERRIDE"
    local uppercase_override_name="${var_name^^}_OVERRIDE"
    local override_value=""

    if [[ -n "${!override_name:-}" ]]; then
        override_value="${!override_name}"
    elif [[ -n "${!uppercase_override_name:-}" ]]; then
        override_value="${!uppercase_override_name}"
    fi

    if [[ -n "$override_value" ]]; then
        printf -v "$var_name" '%s' "$override_value"
    fi
}

apply_array_override() {
    local var_name="$1"
    local override_name="${var_name}_OVERRIDE"
    local uppercase_override_name="${var_name^^}_OVERRIDE"
    local override_value=""
    local -n var_ref="$var_name"

    if [[ -n "${!override_name:-}" ]]; then
        override_value="${!override_name}"
    elif [[ -n "${!uppercase_override_name:-}" ]]; then
        override_value="${!uppercase_override_name}"
    fi

    if [[ -n "$override_value" ]]; then
        read -r -a var_ref <<< "$override_value"
    fi
}

common_set_defaults() {
    NODES=(1)
    CORES=(112)
    WALLTIME=""
    QUEUE=""
    queue=""
    PLATFORM=""
    ACCOUNT=""
    STORAGE_MEDIUM="memory"
    GENERATE_ONLY="false"
}

apply_common_overrides() {
    apply_array_override NODES
    apply_array_override CORES
    apply_scalar_override WALLTIME
    apply_scalar_override QUEUE
    apply_scalar_override queue
    apply_scalar_override PLATFORM
    apply_scalar_override ACCOUNT
    apply_scalar_override STORAGE_MEDIUM
    apply_scalar_override GENERATE_ONLY

    if [[ -z "${QUEUE:-}" && -n "${queue:-}" ]]; then
        QUEUE="$queue"
    fi
    if [[ -n "${QUEUE:-}" ]]; then
        queue="$QUEUE"
    fi
}

validate_common_required_vars() {
    local missing=()
    local run_mode_upper="${RUN_MODE^^}"

    if [[ "$run_mode_upper" == "PBS" ]]; then
        [[ -n "${PLATFORM:-}" ]] || missing+=("PLATFORM")
        [[ -n "${WALLTIME:-}" ]] || missing+=("WALLTIME")
        [[ -n "${QUEUE:-}" ]] || missing+=("QUEUE")
        [[ -n "${ACCOUNT:-}" ]] || missing+=("ACCOUNT")
    fi

    if (( ${#missing[@]} > 0 )); then
        echo "Missing required common settings: ${missing[*]}" >&2
        return 1
    fi
}

resolve_engine_dir() {
    local root_dir="$1"
    local engine="$2"

    case "$engine" in
        qdrant)
            printf '%s\n' "$root_dir/qdrant"
            ;;
        weaviate|weaivate)
            printf '%s\n' "$root_dir/weaviate"
            ;;
        milvus)
            printf '%s\n' "$root_dir/milvus"
            ;;
        *)
            echo "Unknown engine '$engine'. Expected qdrant, weaviate, or milvus." >&2
            return 1
            ;;
    esac
}

copy_engine_items_internal() {
    local src_root="$1"
    local dest_root="$2"
    local missing_ok="$3"
    shift 2
    shift 1

    local item
    for item in "$@"; do
        if [[ ! -e "$src_root/$item" ]]; then
            if [[ "$missing_ok" == "true" ]]; then
                continue
            fi
            echo "Required payload item missing: $src_root/$item" >&2
            return 1
        fi

        mkdir -p "$dest_root/$(dirname "$item")"
        cp -R "$src_root/$item" "$dest_root/$item"
    done
}

copy_engine_items() {
    local src_root="$1"
    local dest_root="$2"
    shift 2
    copy_engine_items_internal "$src_root" "$dest_root" "false" "$@"
}

copy_optional_engine_items() {
    local src_root="$1"
    local dest_root="$2"
    shift 2
    copy_engine_items_internal "$src_root" "$dest_root" "true" "$@"
}

prompt_remove_path() {
    local target_path="$1"
    local reply=""
    local prompt_tty="/dev/tty"

    while true; do
        if [[ ! -r "$prompt_tty" || ! -w "$prompt_tty" ]]; then
            echo "No interactive terminal available; leaving '$target_path' in place." >&2
            return 1
        fi

        read -r -p "Remove '$target_path'? [y/n] " reply < "$prompt_tty" > "$prompt_tty"
        case "${reply,,}" in
            y|yes)
                rm -rf -- "$target_path"
                return 0
                ;;
            n|no)
                echo "Leaving '$target_path' unchanged." >&2
                return 1
                ;;
            *)
                echo "Please answer y or n." >&2
                ;;
        esac
    done
}

emit_platform_filesystems() {
    if [[ "$PLATFORM" == "POLARIS" ]]; then
        echo "#PBS -l filesystems=home:eagle"
    elif [[ "$PLATFORM" == "AURORA" ]]; then
        if [[ "${REQUIRES_DAOS:-false}" == "true" || "$STORAGE_MEDIUM" == "DAOS" ]]; then
            echo "#PBS -l filesystems=home:flare:daos_user_fs"
            echo "#PBS -l daos=daos_user"
        else
            echo "#PBS -l filesystems=home:flare"
        fi
    fi
}

write_pbs_header() {
    cat <<EOF
#!/bin/bash -l
#PBS -N ${JOB_NAME}
#PBS -l select=${TOTAL_NODES}
#PBS -l place=scatter
#PBS -l walltime=${WALLTIME}
#PBS -q ${queue}
EOF

    emit_platform_filesystems

    cat <<EOF
#PBS -A ${ACCOUNT}
#PBS -o workflow.out
#PBS -e workflow.err

EOF
}

write_submit_script() {
    local target_file="$1"
    local run_dir="$2"
    local engine_dir="$3"
    local main_script="$4"

    {
        write_pbs_header
        cat <<'EOF'
EOF
        echo
        cat "$engine_dir/$main_script"
    } > "$target_file"

    chmod +x "$target_file"
}

write_run_config_snapshot() {
    local target_file="$1"
    local engine_name="$2"
    local run_dir="$3"

    {
        {
            echo "ENGINE=${engine_name}"
            echo "myDIR=${run_dir}"
            echo "PLATFORM=${PLATFORM}"
            echo "ACCOUNT=${ACCOUNT}"
            echo "WALLTIME=${WALLTIME}"
            echo "QUEUE=${QUEUE}"
            echo "STORAGE_MEDIUM=${STORAGE_MEDIUM}"
            echo "GENERATE_ONLY=${GENERATE_ONLY}"
            engine_emit_runtime_env
        } | awk -F= '!seen[$1]++'
    } > "$target_file"
}

maybe_submit_job() {
    local run_dir="$1"
    local submit_script="$2"
    local run_mode="${3:-PBS}"

    echo "[SUBMIT] $run_dir/$submit_script"
    if [[ "${GENERATE_ONLY}" == "true" ]]; then
        echo "Generated only; submission skipped."
        return 0
    fi

    if [[ "${run_mode^^}" == "PBS" ]]; then
        (
            cd "$run_dir"
            qsub "$submit_script"
        )
    else
        echo "Run mode is ${run_mode}; submission skipped."
    fi
}
