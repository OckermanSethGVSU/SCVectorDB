# Qdrant

This directory contains the Qdrant engine implementation used by the unified submit interface.

## Main entrypoint

From the repo root:

```bash
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine qdrant --config qdrant_run.env
./pbs_submit_manager.sh --generate-only --engine qdrant --config qdrant_run.env
```

## Directory structure

- `engine.sh`: Qdrant engine wiring for the unified submit manager
- `schema.sh`: Qdrant variable registry and defaults
- `main.sh`: PBS/HPC runtime flow
- `local_main.sh`: local container-backed runtime flow
- `runtime/cluster/`: Qdrant node launch scripts for PBS runs
- `scripts/`: collection setup, indexing, profiling, summaries, and mixed timeline tools
- `clients/batch_client/`: insert/query Rust client
- `clients/mixed/`: mixed insert/query Rust client
- `clients/query/`, `clients/upload/`: older specialized clients kept in the tree
- `runtime_state/`: optional seed runtime-state payload copied into generated runs

## Important run artifacts

Generated Qdrant runs now typically contain:

- `submit.sh`
- `run_config.env`
- `runtime_state/`
- `uploadNPY/`
- `queryNPY/`
- `clientTiming/`
- `systemStats/`

`run_config.env` is the canonical resolved run config for the generated run directory.

## Common Qdrant variables

Required for runs:

- `TASK`
- `PLATFORM`
- `WALLTIME`
- `QUEUE`
- `ACCOUNT`

Common runtime knobs:

- `RUN_MODE`: `PBS` or `local`
- `NODES`
- `WORKERS_PER_NODE`
- `CORES`
- `STORAGE_MEDIUM`
- `LOG_LEVEL`
- `VECTOR_DIM`
- `DISTANCE_METRIC`
- `GPU_INDEX`
- `REBALANCE_TOPOLOGY`

Insert/query file inputs:

- `INSERT_FILEPATH`
- `QUERY_FILEPATH`
- `INSERT_CORPUS_SIZE`
- `QUERY_CORPUS_SIZE`

Index/search tuning:

- `HNSW_M`
- `HNSW_EF_CONSTRUCTION`
- `HNSW_EF_SEARCH`

Mixed-workload knobs:

- `MIXED_DATA_FILEPATH`
- `MIXED_CORPUS_SIZE`
- `INSERT_MODE`
- `QUERY_MODE`
- `INSERT_OPS_PER_SEC`
- `QUERY_OPS_PER_SEC`
- `INSERT_START_ID`

Use:

```bash
./pbs_submit_manager.sh --help --engine qdrant
```

to see the full variable list, defaults, and requirement rules.

## Local mode

Local runs use:

- `local_main.sh`
- a local Qdrant container
- `clients/batch_client`
- optional `clients/mixed`

Typical workflow:

```bash
./pbs_submit_manager.sh --generate-only --engine qdrant --config qdrant_local.env
cd qdrant/<generated-run-dir>
bash submit.sh
```

## Notes on current behavior

- Empty `INSERT_CORPUS_SIZE` / `QUERY_CORPUS_SIZE` means use all rows in the `.npy` file.
- Empty `CORES` means no explicit CPU binding.
- `REBALANCE_TOPOLOGY=False` keeps `configure_collection.py` in simple setup mode.
- `clientTiming/` holds timing CSVs and summary outputs.
- `runtime_state/flag.txt` is the shared stop signal for worker-side consumers; the top-level `flag.txt` is cleaned up at the end.
