# SCVectorDB

Benchmark and orchestration scripts for running **distributed vector database experiments** on HPC systems (PBS scheduler + Apptainer containers), with workflows for:

- **Qdrant** (`qdrantEval/`)
- **Milvus** (`milvus/`)

The repository is focused on multi-node launch automation, storage back-end experiments (memory/DAOS/Lustre), and ingestion performance collection.

## What this repo does

At a high level, each stack includes:

1. A **submission manager** that generates a PBS job script and stages a run directory.
2. Per-node launch scripts to start database workers inside Apptainer images.
3. A workload script (currently centered on insert/ingest) with profiling hooks.
4. Post-processing utilities that summarize timing/statistics artifacts.

## Repository layout

```text
.
├── qdrantEval/
│   ├── pbs_submit_manager.sh       # Generates + submits PBS jobs for Qdrant workflow
│   ├── insert/main.sh              # Main run workflow (cluster setup, ingest, summarize)
│   ├── qdrantSetup/                # Worker/container launch scripts
│   ├── generalPython/              # Topology + profiling + helper scripts
│   ├── rustCode/                   # Rust uploader client + build helper
│   └── insert/multi_client_summary.py
└── milvus/
    ├── pbs_submit_manager.sh       # Generates PBS jobs for Milvus workflow
    ├── insert/main.sh              # Main Milvus run workflow
    ├── milvusSetup/                # Worker/container launch scripts
    └── generalPython/              # Network/config helper scripts
```

## Prerequisites

This project is built around an HPC environment similar to ALCF Polaris/Aurora-style systems. Expected tooling includes:

- **PBS Pro** (`qsub`, `$PBS_NODEFILE`)
- **MPI launcher** (`mpirun`)
- **Apptainer**
- Cluster module environment (`module load ...`)
- Python environment with packages used by scripts (not exhaustively pinned in this repo), including:
  - `qdrant-client`
  - `numpy`
  - `psutil`
  - `jq` (shell dependency used by launch scripts)
- Rust toolchain (for building Qdrant multi-client uploader)

You will also need database/runtime artifacts that are referenced by scripts but not committed here (for example container images/binaries such as `qdrant.sif`, `qdrant`, and `milvus.sif`, plus environment-specific base paths).

## Qdrant workflow (recommended entry point)

### 1) Build uploader client (once per target environment)

From `qdrantEval/rustCode/`:

```bash
bash compile.sh
```

This produces the multi-client uploader binary used by the insert workflow.

### 2) Configure experiment parameters

Edit:

- `qdrantEval/pbs_submit_manager.sh`

Key knobs include:

- `NODES`, `WORKERS_PER_NODE`, `CORES`
- `UPLOAD_BATCH_SIZE`, `QUERY_BATCH_SIZE`
- `STORAGE_MEDIUM` (`memory`, `DAOS`, `lustre`)
- `CORPUS_SIZE`
- `UPLOAD_CLIENTS_PER_WORKER`
- `DATA_FILEPATH`

### 3) Launch

From `qdrantEval/`:

```bash
bash pbs_submit_manager.sh
```

The script generates a per-run directory, copies required assets, writes a `submit.sh`, and submits it with `qsub`.

### 4) What happens inside the job

The `insert/main.sh` workflow performs:

- container/runtime setup
- rank-to-node directory prep
- Qdrant worker launch
- topology/collection configuration
- Rust multi-client ingestion
- profiling/stat collection and summary generation

### 5) Outputs

Per-run output directory typically contains:

- worker logs (`rank*.out`, workflow logs)
- perf/system stat files
- per-client `.npy` timing artifacts
- aggregate `summary.csv` via `insert/multi_client_summary.py`

## Milvus workflow

### 1) Configure parameters

Edit:

- `milvus/pbs_submit_manager.sh`

Adjust node counts, batching, storage medium, corpus size, and paths.

### 2) Launch

From `milvus/`:

```bash
bash pbs_submit_manager.sh
```

The manager creates a run directory and staged `submit.sh` similar to Qdrant.

### 3) Runtime notes

Milvus worker launch scripts provision local config, map network addresses, and start standalone Milvus inside Apptainer with environment-specific binds.

## Adapting to a new environment

The scripts currently include site-specific assumptions (paths, project allocations, module names, queue names, account names). To port:

1. Update hard-coded filesystem paths (`/lus/...`, `/tmp/...`, etc.).
2. Update PBS queue/account directives.
3. Ensure container image and binary paths are available in staging.
4. Verify Python/Rust environments and dependencies are installed.
5. Validate node/core topology assumptions used for rank mapping.

## Caveats

- Several scripts are intentionally tuned for internal workflows and may require cleanup/generalization before broader use.
- There is no unified dependency lockfile at repo root.
- Some currently commented sections in scripts indicate prior/alternate experiment paths.

## Quick start checklist

- [ ] Build Rust uploader (`qdrantEval/rustCode/compile.sh`)
- [ ] Confirm container artifacts are available (`*.sif`, binaries)
- [ ] Set dataset path (`DATA_FILEPATH`)
- [ ] Set storage medium (`memory` / `DAOS` / `lustre`)
- [ ] Tune node/client/batch settings in submit manager
- [ ] Submit with `bash pbs_submit_manager.sh`
- [ ] Inspect generated run directory outputs and `summary.csv`
