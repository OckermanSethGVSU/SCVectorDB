# Qdrant workflow

This directory contains the Qdrant workflows used in this repo: distributed PBS launches, Rust ingest/query clients, a mixed insert/query runner, and a local container-backed smoke test.

Main HPC entrypoint:

- `pbs_submit_manager.sh`

Local smoke-test entrypoint:

- `local_test.sh`

## Current workflow capabilities

- Task modes: `INSERT`, `INDEX`, `QUERY` (`TASK` in `pbs_submit_manager.sh`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage media: `memory`, `DAOS`, `lustre`, `SSD` (`STORAGE_MEDIUM`)
- Perf modes: `NONE`, `STAT`, `TRACE` (`PERF`)
- Insert/query balancing strategies:
  - `INSERT_BALANCE_STRATEGY`: `NO_BALANCE`, `WORKER_BALANCE`
  - `QUERY_BALANCE_STRATEGY`: `NO_BALANCE`, `WORKER_BALANCE`
- Vector/index controls:
  - `VECTOR_DIM`
  - `DISTANCE_METRIC`: `IP`, `COSINE`, `L2`
  - `GPU_INDEX`: `True`, `False`
- Restore/status path via `RESTORE_DIR` and `EXPECTED_CORPUS_SIZE`

## Important files

- `pbs_submit_manager.sh`: parameter sweep and PBS script generation
- `main.sh`: runtime orchestration for cluster launch, ingest, query, and index paths
- `local_test.sh`: local container-backed test for standard or mixed runs
- `check_dependencies.sh`: dependency validation helper
- `qdrantSetup/`: cluster launch scripts and local collection/data preparation
- `generalPython/`: topology setup, profiling, summaries, index/status helpers
- `rustCode/`: Rust clients and build helpers

## Current Rust clients used here

See `rustCode/README.md` for the full breakdown. The important projects are:

- `rustCode/multiClientOP/`: current combined insert/query client used by `main.sh`
- `rustCode/mixedrunner/`: mixed insert/query runner with JSONL event logs
- `rustCode/multiClientUpload/`: older upload-only client kept in the repo
- `rustCode/multiClientQuery/`: older query-only client kept in the repo

## HPC runtime flow (`main.sh`)

1. Load platform modules and Python environment.
2. Optionally mount DAOS storage.
3. Generate per-rank storage/config directories.
4. Launch Qdrant worker ranks across compute nodes.
5. Start profiling helpers on the client and worker nodes.
6. Wait for cluster readiness and run `configureTopo.py`.
7. Run `./multiClientOP` with `ACTIVE_TASK=INSERT` for ingest.
8. Run `python3 multi_client_summary.py` and move `.npy` timing outputs into `uploadNPY/`.
9. If `TASK=INDEX`, run `generalPython/index.py`.
10. If `TASK=QUERY`, run `./multiClientOP` again with `ACTIVE_TASK=QUERY`, then summarize results.

## Important submit variables (`pbs_submit_manager.sh`)

### Sweep variables

- `NODES=(...)`
- `WORKERS_PER_NODE=(...)`
- `CORES=(...)`
- `INSERT_BATCH_SIZE=(...)`
- `QUERY_BATCH_SIZE=(...)`

### Runtime variables

- `TASK`
- `STORAGE_MEDIUM`
- `PERF`
- `VECTOR_DIM`
- `DISTANCE_METRIC`
- `GPU_INDEX`
- `PLATFORM`
- `QDRANT_EXECUTABLE`
- Insert path:
  - `INSERT_FILEPATH`
  - `INSERT_CORPUS_SIZE`
  - `INSERT_BATCH_SIZE`
  - `INSERT_CLIENTS_PER_WORKER`
  - `INSERT_BALANCE_STRATEGY`
- Query path:
  - `QUERY_FILEPATH`
  - `QUERY_CORPUS_SIZE`
  - `QUERY_BATCH_SIZE`
  - `QUERY_CLIENTS_PER_WORKER`
  - `QUERY_BALANCE_STRATEGY`
- Restore/status path:
  - `RESTORE_DIR`
  - `EXPECTED_CORPUS_SIZE`

### Scheduler variables

- `WALLTIME`
- `queue`

## Local test script (`local_test.sh`)

`local_test.sh` starts a local Qdrant container, prepares a local collection plus synthetic `.npy` files, and then runs either:

- `standard` mode: insert followed by query via `multiClientOP`
- `mixed` mode: the Rust mixed runner with configurable insert/query worker counts and pacing

Examples:

```bash
./local_test.sh
./local_test.sh --mixed
./local_test.sh --mixed --insert-clients 2 --query-clients 2 --mode max
./local_test.sh --mixed --insert-clients 1 --query-clients 1 --insert-mode rate --insert-ops-per-sec 100 --query-mode max
```

## Build notes

Build Rust clients from `qdrant/rustCode`:

```bash
cd rustCode
./compile.sh multiClientOP
./compile.sh mixedrunner
```

Older client projects can also be built if needed:

```bash
./compile.sh multiClientUpload
./compile.sh multiClientQuery
```

## Dependency check

Run manually:

```bash
./check_dependencies.sh
./check_dependencies.sh --missing-only
```

## Expected outputs

HPC runs usually produce:

- `workflow.out`, `output.log`
- `insert_times.csv` or query timing summaries
- `uploadNPY/*.npy`
- `systemStats/*.csv`
- cluster status files and per-node logs

Local mixed runs produce:

- `local_test_data/mixed_logs/insert_client_*.jsonl`
- `local_test_data/mixed_logs/query_client_*.jsonl`

## Notes

- `pbs_submit_manager.sh` still writes `submit.sh` in the working directory and stages workflow files from there.
- In the current submit manager, `qsub $target_file` is commented out, so submission is not automatic until you re-enable it.
- The Rust Qdrant client uses gRPC. For local mixed runs, `local_test.sh` now points `QDRANT_URL` at the local gRPC port.
