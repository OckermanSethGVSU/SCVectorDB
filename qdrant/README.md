# Qdrant workflow

This workflow launches a distributed Qdrant cluster on PBS systems, performs multi-client vector upload, and can optionally run index construction.

Main entrypoint:

- `pbs_submit_manager.sh`

## What is currently supported

- Task modes: `insert`, `index` (controlled by `TASK`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage medium flags: `memory`, `DAOS`, `lustre`, `SSD` (`STORAGE_MEDIUM`)
- Upload balancing strategies:
  - `NO_BALANCE`
  - `WORKER_BALANCE`
- Vector parameters:
  - `VECTOR_DIM`
  - `DISTANCE_METRIC` (`IP`, `COSINE`, `L2`)

## Workflow files

- `pbs_submit_manager.sh`: parameter sweep + PBS script generation + staging + submission
- `main.sh`: runtime orchestration (cluster bring-up, upload, optional index pass)
- `check_dependencies.sh`: validates required files before submission
- `qdrantSetup/*.sh`: node/bootstrap launch scripts
- `generalPython/*.py`: profiling, topology setup, summaries, and index step
- `rustCode/multiClientUpload/`: Rust multi-client upload program

## Important submit variables (`pbs_submit_manager.sh`)

### Sweep variables

- `NODES=(...)`
- `WORKERS_PER_NODE=(...)`
- `CORES=(...)`
- `UPLOAD_BATCH_SIZE=(...)`
- `QUERY_BATCH_SIZE=(...)`
- `UPLOAD_CLIENTS_PER_WORKER=(...)`

### Runtime variables

- `TASK`
- `STORAGE_MEDIUM`
- `usePerf` (exported as `USEPERF`)
- `CORPUS_SIZE`
- `UPLOAD_BALANCE_STRATEGY`
- `DATA_FILEPATH`
- `VECTOR_DIM`
- `DISTANCE_METRIC`
- `PLATFORM`

### Scheduler variables

- `WALLTIME`
- `queue`

## Runtime flow (`main.sh`)

1. Loads platform modules/env and activates Python env.
2. Optionally mounts DAOS via `launch-dfuse.sh`.
3. Uses MPI to generate per-rank directories and launch Qdrant worker processes.
4. Starts profiling on client and worker nodes.
5. Waits for Qdrant readiness, then runs `configureTopo.py` until ready.
6. Runs Rust uploader (`./multiClientUpload`) for data ingest.
7. Writes summary/timing outputs.
8. If `TASK=index`, runs `generalPython/index.py` after upload.

## Dependency check

Run manually:

```bash
./check_dependencies.sh
```

The submit manager runs the missing-only check automatically:

```bash
./check_dependencies.sh --missing-only
```

## Build notes

Build the Rust uploader with:

```bash
cd rustCode
./compile.sh multiClientUpload
```

This emits `multiClientUpload` into the current directory and should then be placed where `pbs_submit_manager.sh` expects it (`rustCode/multiClientUpload/multiClientUpload`).

## Required local artifacts (not committed)

- `qdrant.sif`
- `qdrant` server binary
- `perf/perf`
- Built uploader: `rustCode/multiClientUpload/multiClientUpload`

## Typical run outputs

Per run directory:

- `workflow.out`, `output.log`
- `insert_times.csv`
- `uploadNPY/*.npy`
- `systemStats/*.csv`
- cluster/setup status files and worker logs
