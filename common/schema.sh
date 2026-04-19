#!/bin/bash

# Shared schema entries used across all engines.
# register_common_var "NAME" "REQUIREMENT" "DEFAULT" "CHOICES" "DESCRIPTION" ["REQUIRED_IF"]

register_common_var "TASK" "required" "" "" "Experiment task"
register_common_var "RUN_MODE" "default" "PBS" "PBS LOCAL local" "Run under PBS or create a local harness"
register_common_var "PLATFORM" "conditional" "" "POLARIS AURORA" "Target platform" "RUN_MODE=PBS"
register_common_var "ACCOUNT" "conditional" "" "" "PBS project/account to charge for the run" "RUN_MODE=PBS"
register_common_var "WALLTIME" "conditional" "" "" "PBS walltime" "RUN_MODE=PBS"
register_common_var "QUEUE" "conditional" "" "" "PBS queue name" "RUN_MODE=PBS"
register_common_var "NODES" "conditional" "" "" "Compute-node count to allocate for worker ranks" "RUN_MODE=PBS"
register_common_var "CORES" "default" "" "" "CPU cores assigned per worker rank; empty disables explicit CPU binding"
register_common_var "STORAGE_MEDIUM" "default" "memory" "memory DAOS lustre SSD" "Storage medium for engine data"
register_common_var "BASE_DIR" "default" "" "" "Base directory containing generated run directories; auto-filled by the submit manager when empty"
register_common_var "ENV_PATH" "default" "" "" "Python environment path"
register_common_var "ALLOW_SYSTEM_PYTHON" "default" "False" "True False" "Allow PBS runs to use the already-loaded Python environment when ENV_PATH is empty"
