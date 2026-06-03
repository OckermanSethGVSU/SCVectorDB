# Overview

This repo is a workflow collection for vector-database experiments on PBS-based HPC systems plus local harnesses for selected engines. It is not a single library or service with one build/test entrypoint.

## Current Mental Model

The active control plane is unified:

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine milvus --config path/to/run.env
./pbs_submit_manager.sh --generate-only --engine weaviate --config path/to/run.env
```

High-level flow:

1. `pbs_submit_manager.sh` resolves the selected engine directory.
2. It sources `common/submit_lib.sh` and `<engine>/engine.sh`.
3. Shared schema comes from `common/schema.sh`.
4. Engine-specific schema comes from `<engine>/schema.sh`.
5. Config is assembled from defaults, `--config`, and `--set KEY=value`.
6. Sweep values expand into one or more parameter combinations.
7. A generated run directory is created inside the engine directory.
8. Engine payload files are staged into that run directory.
9. The manager writes `submit.sh` and `run_config.env`.
10. Submission happens automatically only when `RUN_MODE=PBS`.

## Active Areas

- `pbs_submit_manager.sh`: unified top-level submit/generate entrypoint
- `common/`: shared schema and submit helpers
- `qdrant/`: active Qdrant workflows, clients, runtime, and helpers
- `milvus/`: active Milvus workflows, clients, runtime, and helpers
- `weaviate/`: active Weaviate workflow, clients, runtime, and helpers

## Source vs Generated Runs

There are two distinct layers in this repo:

- tracked source files under version control
- generated run directories and experiment outputs

If you want workflow behavior changes, edit tracked source files. Do not treat generated run directories as canonical source unless the task is explicitly about inspecting one generated run.

## Engine Status

- `qdrant/`: active, supports `RUN_MODE=PBS` and `RUN_MODE=LOCAL`
- `milvus/`: active, supports `RUN_MODE=PBS` and `RUN_MODE=LOCAL`
- `weaviate/`: active, supports `RUN_MODE=PBS` and `RUN_MODE=LOCAL` in the
  current tracked workflow
