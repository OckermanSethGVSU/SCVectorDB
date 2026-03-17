# Qdrant workflow

This workflow launches a distributed Qdrant cluster on PBS systems, performs multi-client vector upload, and can optionally run index construction.

Main entrypoint:

- `pbs_submit_manager.sh`

## What is currently supported

- Task modes: `insert`, `index` (`TASK`)
- Platforms: `POLARIS`, `AURORA` (`PLATFORM`)
- Storage medium flags: `memory`, `DAOS`, `lustre`, `SSD` (`STORAGE_MEDIUM`)
- Perf modes: `NONE`, `STAT`, `TRACE` (`PERF`)
- Upload balancing strategies (`UPLOAD_BALANCE_STRATEGY`):
  - `NO_BALANCE`
  - `WORKER_BALANCE`
- Vector/index controls:
  - `VECTOR_DIM`
  - `DISTANCE_METRIC` (`IP`, `COSINE`, `L2`)
  - `GPU_INDEX` (`True`/`False`)

## Workflow files

- `pbs_submit_manager.sh`: parameter sweep + PBS script generation + staging
- `main.sh`: runtime orchestration (cluster bring-up, upload, optional index pass)
- `check_dependencies.sh`: validates required files before submission
- `qdrantSetup/*.sh`: node/bootstrap launch scripts
- `generalPython/*.py`: topology setup, profiling, summaries, index pass
- `rustCode/multiClientUpload/`: Rust multi-client uploader used by this workflow
- `rustCode/multiClientQuery/`: Rust query client project (present in repo, not wired into `main.sh`)

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
- `PERF`
- `CORPUS_SIZE`
- `UPLOAD_BALANCE_STRATEGY`
- `DATA_FILEPATH`
- `VECTOR_DIM`
- `DISTANCE_METRIC`
- `GPU_INDEX`
- `PLATFORM`
- `QDRANT_EXECUTABLE`

### Scheduler variables

- `WALLTIME`
- `queue`

## Runtime flow (`main.sh`)

1. Loads platform modules/env.
2. Optionally mounts DAOS via `launch-dfuse.sh`.
3. Uses MPI to generate per-rank directories and launch Qdrant workers.
4. Starts profiling helpers on client/worker nodes.
5. Waits for Qdrant readiness, then runs `configureTopo.py`.
6. Runs Rust uploader (`./multiClientUpload`) for ingest.
7. Writes summary/timing outputs.
8. If `TASK=index`, runs `generalPython/index.py`.

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

Build a Rust client from `qdrant/rustCode`:

```bash
cd rustCode
./compile.sh multiClientUpload
# or
./compile.sh multiClientQuery
```

Expected runtime binary path for submit workflow:

- `rustCode/multiClientUpload/multiClientUpload`

## Required local artifacts (not committed)

- `qdrant.sif`
- Qdrant executable under `qdrantBuilds/` (selected by `QDRANT_EXECUTABLE`)
- `perf/perf`
- Built uploader: `rustCode/multiClientUpload/multiClientUpload`

## Typical run outputs

Per run directory:

- `workflow.out`, `output.log`
- `insert_times.csv`
- `uploadNPY/*.npy`
- `systemStats/*.csv`
- Cluster/setup status files and worker logs

## Notes

- In the current `pbs_submit_manager.sh`, `qsub $target_file` is commented out. Re-enable it to submit automatically.
