# Qdrant workflow

This folder contains the Qdrant ingest benchmark workflow. The main entry point is:

- `pbs_submit_manager.sh`

It generates per-run folders, writes a PBS `submit.sh`, stages required files, and submits via `qsub`.

## `pbs_submit_manager.sh` variables

### Parameter sweep variables

- `NODES=(...)`: Number of worker nodes (plus one client node is requested in PBS).
- `WORKERS_PER_NODE=(...)`: Qdrant worker processes per worker node.
- `CORES=(...)`: CPU depth for launched worker processes.
- `UPLOAD_BATCH_SIZE=(...)`: Upload batch sizes to sweep.
- `QUERY_BATCH_SIZE=(...)`: Query batch size values (currently not used in active upload path, but exported).
- `UPLOAD_CLIENTS_PER_WORKER=(...)`: Number of upload clients per worker used in loop (`UCPW`).

### PBS/job variables

- `WALLTIME="..."`: PBS walltime.
- `queue=...`: PBS queue.

### Runtime variables

- `task="insert"`: Selects `insert/main.sh` workflow.
- `STORAGE_MEDIUM="..."`: Storage backend (`memory`, `DAOS`, `lustre`, `SSD`).
- `usePerf="..."`: Enables/disables performance instrumentation consumed as `USEPERF`.
- `CORPUS_SIZE=...`: Number of vectors to ingest.
- `UPLOAD_BALANCE_STRATEGY="..."`: Strategy passed to uploader (`NO_BALANCE`, `WORKER_BALANCE`).
- `DATA_FILEPATH="..."`: Absolute path to embeddings `.npy` file.
- `PLATFORM="..."`: Platform-specific PBS/filesystem/module behavior (`POLARIS` or `AURORA`).

### Variables written into `submit.sh`

The manager writes/exports these for `insert/main.sh`:

- `NODES`, `WORKERS_PER_NODE`, `myDIR`
- `STORAGE_MEDIUM`, `CORPUS_SIZE`, `USEPERF`, `CORES`
- `QUERY_BATCH_SIZE`, `UPLOAD_BATCH_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`, `DATA_FILEPATH`
- `UPLOAD_BALANCE_STRATEGY`, `PLATFORM`

## Overall flow

1. **Generate PBS script**
   - Builds `submit.sh` with queue/walltime/select/filesystem directives.
2. **Encode run metadata**
   - Constructs run dir name like `insert_<storage>_N..._uploadBS..._<timestamp>`.
   - Writes runtime variables into `submit.sh`.
3. **Append main workflow**
   - Concatenates `insert/main.sh` into `submit.sh`.
4. **Stage run directory**
   - Creates run dir and copies images, binaries, launch scripts, helper Python, perf folder, and summary script.
5. **Submit job**
   - Moves `submit.sh` into run dir and runs `qsub submit.sh`.

## Files assumed but not in this repo

The script references several artifacts that are expected to exist locally in `qdrant/` but are not committed:

- `qdrant.sif` (Apptainer image)
- `qdrant` (Qdrant binary)
- `perf/` directory
- `rustCode/multiClientUpload/multiClientUpload` (prebuilt Rust uploader binary)

Runtime scripts also assume platform-specific paths and environments not in the repo, for example:

- Python env activation paths under `/eagle/...` or `/lus/flare/...`
- Dataset file path from `DATA_FILEPATH`
- PBS/MPI/module/DAOS utilities available on target HPC system

## Expected run outputs

Per-run directories typically contain:

- Worker/profile logs
- Timing and perf CSVs (`systemStats`, `times.csv`)
- Upload timing `.npy` files (`uploadNPY/`)
- Aggregate summary from `multi_client_summary.py`
