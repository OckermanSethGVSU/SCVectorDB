# SCVectorDB

SCVectorDB is a collection of **PBS-based HPC workflows** for large-scale vector database experiments.

The repository currently contains:

- `qdrant/`: Qdrant cluster launch + multi-client upload + optional index build.
- `milvus/`: Milvus standalone/distributed launch + multi-client insert/query + optional index workflow.
- `weaivate/`: early Weaviate workflow scaffolding (`pbs_submit_manager.sh`, `main.sh`, setup script).

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
├── milvus/
│   ├── README.md
│   ├── pbs_submit_manager.sh
│   ├── main.sh
│   ├── check_dependencies.sh
│   ├── milvusSetup/
│   ├── generalPython/
│   ├── cpuMilvus/
│   ├── goCode/
│   └── utils/
└── weaivate/
    ├── pbs_submit_manager.sh
    ├── main.sh
    └── weaviateSetup/
```

## Common execution model

Most workflows follow the same pattern:

1. Configure experiment variables in `pbs_submit_manager.sh`.
2. Generate a run-specific `submit.sh` with exported runtime variables.
3. Stage required binaries/images/scripts into a run directory.
4. Append workflow logic (`main.sh`) to `submit.sh`.
5. Submit with `qsub` (or leave submission commented for dry-run workflows).

## Environment expectations

These workflows are designed for HPC systems with:

- PBS Pro (`qsub`, `$PBS_NODEFILE`)
- MPI runtime (`mpirun`)
- Apptainer
- Python 3 + workflow dependencies
- `jq`
- Site module system (`module` / `ml`)
- Optional DAOS utilities (`launch-dfuse.sh`) for DAOS-backed runs

Most runs also rely on site-specific absolute paths for:

- Container images (`*.sif`)
- Prebuilt client binaries
- Python environments
- Dataset `.npy` files

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

Before submitting jobs, update site-specific values in each `pbs_submit_manager.sh` (queue/account/filesystem settings, platform, dataset paths, env paths, binary/image locations).
