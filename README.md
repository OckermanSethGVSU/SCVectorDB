# SCVectorDB

SCVectorDB is a set of **PBS-based HPC orchestration workflows** for distributed vector database ingest and indexing experiments.

The repository currently supports:

- `qdrant/`: Qdrant cluster launch + multi-client upload + optional index build.
- `milvus/`: Milvus standalone/distributed launch + multi-client upload + optional index conversion.

## Repository layout

```text
.
├── README.md
├── qdrant/
│   ├── README.md
│   ├── pbs_submit_manager.sh
│   ├── main.sh
│   ├── check_dependencies.sh
│   ├── qdrantSetup/
│   ├── generalPython/
│   └── rustCode/
└── milvus/
    ├── README.md
    ├── pbs_submit_manager.sh
    ├── main.sh
    ├── check_dependencies.sh
    ├── milvusSetup/
    ├── generalPython/
    ├── cpuMilvus/
    └── goCode/
```

## Common execution model

Both workflows use a two-stage launch:

1. `pbs_submit_manager.sh` validates required local artifacts with `check_dependencies.sh`.
2. It sweeps parameter combinations and generates a per-run `submit.sh`.
3. It exports run configuration variables into `submit.sh`.
4. It appends the workflow runtime script (`main.sh`) to `submit.sh`.
5. It stages all required binaries/scripts into a run directory.
6. It submits with `qsub`.

## Environment expectations

These workflows are designed for systems with:

- PBS Pro (`qsub`, `$PBS_NODEFILE`)
- MPI runtime (`mpirun`)
- Apptainer
- Python 3 + required Python packages
- `jq`
- Site module system (`module` / `ml`)
- Optional DAOS utilities for DAOS-backed runs

Most runs also rely on site-specific absolute paths for:

- Container images
- Prebuilt client binaries
- Python environments
- Dataset `.npy` embedding files

See database-specific setup details in:

- `qdrant/README.md`
- `milvus/README.md`

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

Before submitting jobs, update site-specific values in each `pbs_submit_manager.sh` (queue, account/filesystem settings, platform, dataset path, env path, etc.).
