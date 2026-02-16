# SCVectorDB

SCVectorDB is a set of **HPC orchestration scripts** for distributed vector database ingest benchmarking on PBS-based systems (for example, Polaris/Aurora style environments).

This repo contains two workflows:

- `qdrant/` for Qdrant ingest experiments.
- `milvus/` for Milvus ingest experiments.

Both workflows use a `pbs_submit_manager.sh` script to:

1. Sweep one or more parameter combinations.
2. Generate a per-run `submit.sh` PBS job script.
3. Stage required runtime files into a run directory.
4. Submit (or prepare) the run.

---

## Repository layout

```text
.
├── README.md
├── qdrant/
│   ├── README.md
│   ├── pbs_submit_manager.sh
│   ├── insert/main.sh
│   ├── qdrantSetup/
│   ├── generalPython/
│   └── rustCode/
└── milvus/
    ├── README.md
    ├── pbs_submit_manager.sh
    ├── insert/main.sh
    ├── milvusSetup/
    ├── generalPython/
    └── goCode/
```

---

## Common execution model

At a high level, both submit managers follow this pattern:

- Define loop variables (`NODES`, batch sizes, etc.).
- Define runtime/platform variables (`STORAGE_MEDIUM`, `PLATFORM`, dataset paths).
- For each parameter combination:
  - Build PBS directives into `submit.sh`.
  - Export variables consumed by `insert/main.sh`.
  - Append the selected task script (`insert/main.sh`) to `submit.sh`.
  - Create a run directory and copy required runtime assets.
  - Move `submit.sh` into that run directory.
  - Submit (`qsub`) or stage for manual submission.

See database-specific details in:

- `qdrant/README.md`
- `milvus/README.md`

---

## Prerequisites

These scripts assume an HPC environment with:

- PBS Pro (`qsub`, `$PBS_NODEFILE`)
- `mpirun`
- Apptainer
- Python 3 and required packages used by helper scripts
- `jq` (used in runtime scripts)
- Site modules (`module`/`ml`) and site-specific allocations/queues

In addition, each workflow assumes extra artifacts that are **not committed in this repo** (container images, prebuilt binaries, and site-specific paths). Those assumptions are documented in each workflow README.

---

## Quick start

### Qdrant

```bash
cd qdrant
bash pbs_submit_manager.sh
```

### Milvus

```bash
cd milvus
bash pbs_submit_manager.sh
```

Before running, review and update platform-specific variables in each `pbs_submit_manager.sh`.
