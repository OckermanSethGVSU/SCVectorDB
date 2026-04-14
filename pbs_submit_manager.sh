#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$ROOT_DIR/common/submit_lib.sh"

ENGINE="${ENGINE:-}"
CONFIG_FILE=""
CLI_SET_OVERRIDES=()
HELP_REQUESTED="false"
GENERATE_ONLY_REQUESTED="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --engine)
            ENGINE="$2"
            shift 2
            ;;
        --generate-only)
            GENERATE_ONLY_REQUESTED="true"
            shift
            ;;
        --config)
            CONFIG_FILE="$2"
            shift 2
            ;;
        --set)
            CLI_SET_OVERRIDES+=("$2")
            shift 2
            ;;
        -h|--help)
            HELP_REQUESTED="true"
            shift
            ;;
        *)
            if [[ -z "$ENGINE" && "$1" != --* ]]; then
                ENGINE="$1"
                shift
            else
                echo "Unknown argument: $1" >&2
                print_usage >&2
                exit 1
            fi
            ;;
    esac
done

if [[ -n "$CONFIG_FILE" ]]; then
    load_config_overrides_file "$CONFIG_FILE"
fi

if [[ -z "$ENGINE" ]]; then
    if [[ "$HELP_REQUESTED" == "true" ]]; then
        print_usage
        exit 0
    fi
    print_usage
    exit 1
fi

ENGINE_DIR="$(resolve_engine_dir "$ROOT_DIR" "$ENGINE")"
ENGINE_BASENAME="$(basename "$ENGINE_DIR")"

if [[ ! -f "$ENGINE_DIR/engine.sh" ]]; then
    echo "No unified engine definition found for '$ENGINE_BASENAME'." >&2
    exit 1
fi

source "$ENGINE_DIR/engine.sh"

common_set_defaults
engine_set_defaults
apply_cli_set_overrides "${CLI_SET_OVERRIDES[@]}"
if [[ "$GENERATE_ONLY_REQUESTED" == "true" ]]; then
    GENERATE_ONLY="true"
fi
apply_common_overrides
engine_apply_overrides

if [[ "$HELP_REQUESTED" == "true" ]]; then
    if [[ "$(type -t engine_show_help || true)" == "function" ]]; then
        engine_show_help
        exit 0
    elif [[ "$(type -t engine_show_vars || true)" == "function" ]]; then
        engine_show_vars
        exit 0
    fi
    echo "Engine '$ENGINE_BASENAME' does not expose help metadata yet." >&2
    exit 1
fi

validate_common_required_vars
engine_validate_config

if [[ "$(type -t engine_print_summary || true)" == "function" ]]; then
    engine_print_summary
fi

while IFS= read -r combo; do
    [[ -n "$combo" ]] || continue

    engine_load_combo "$combo"

    if [[ "$(type -t engine_validate_combo || true)" == "function" ]]; then
        engine_validate_combo
    fi

    RUN_DIR_NAME="$(engine_make_run_dir_name)"
    RUN_DIR_PATH="$ENGINE_DIR/$RUN_DIR_NAME"
    mkdir -p "$RUN_DIR_PATH"

    engine_copy_payload "$RUN_DIR_PATH"

    MAIN_SCRIPT_RELATIVE_PATH="$(engine_main_script_path)"
    write_submit_script \
        "$RUN_DIR_PATH/submit.sh" \
        "$RUN_DIR_NAME" \
        "$ENGINE_DIR" \
        "$MAIN_SCRIPT_RELATIVE_PATH"

    write_run_config_snapshot \
        "$RUN_DIR_PATH/run_config.env" \
        "$ENGINE_BASENAME" \
        "$RUN_DIR_NAME"

    maybe_submit_job "$RUN_DIR_PATH" "submit.sh" "${RUN_MODE:-PBS}"
done < <(engine_iterate_matrix)
