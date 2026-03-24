# SCVectorDB

SCVectorDB is a collection of PBS-oriented HPC workflows for large-scale vector database experiments. The repository currently contains working workflows for Qdrant and Milvus, plus early Weaviate scaffolding and graphing utilities used for post-run analysis.

## Top-level layout

```text
.
├── README.md
├── graphing/
├── milvus/
├── qdrant/
└── weaivate/
```

## Current repository contents

- `qdrant/`: Qdrant cluster launch, ingest/query clients, local test helpers, and Rust mixed insert/query runner.
- `milvus/`: Milvus standalone/distributed workflows, Go client binaries, and mixed insert/query runner for timeline-style experiments.
- `graphing/`: notebooks and scripts for analyzing insert/query/index outputs.
- `weaivate/`: early workflow scaffolding for Weaviate.

## Common execution model

Most HPC workflows in this repo follow the same pattern:

1. Set experiment variables in `pbs_submit_manager.sh`.
2. Generate a run-specific PBS submission script with exported runtime variables.
3. Stage workflow scripts, binaries, and site-specific assets into a run directory.
4. Append the workflow entrypoint (`main.sh`) to the generated script.
5. Submit with `qsub` when ready.

## Environment expectations

These workflows assume an HPC environment with most or all of the following:

- PBS Pro (`qsub`, `$PBS_NODEFILE`)
- MPI (`mpirun`)
- Apptainer
- Python 3 and project-specific Python environments
- `jq`
- Site module system (`module` / `ml`)
- Optional DAOS helpers (`launch-dfuse.sh`, `clean-dfuse.sh`)

Most submitted runs also rely on site-specific absolute paths for:

- container images or executables
- Python environments
- dataset `.npy` files
- prebuilt client binaries
- output base directories

## Local vs HPC usage

- Use `qdrant/local_test.sh` for a local container-backed smoke test of Qdrant clients.
- Use `qdrant/pbs_submit_manager.sh` and `milvus/pbs_submit_manager.sh` for full HPC runs.
- The local Qdrant script can now run either the standard insert-then-query path or the Rust mixed runner.

## Quick start

### Qdrant

```bash
cd qdrant
bash pbs_submit_manager.sh
```

For a local container-backed test:

```bash
cd qdrant
./local_test.sh
./local_test.sh --mixed --insert-clients 1 --query-clients 1 --mode max
```

### Milvus

```bash
cd milvus
bash pbs_submit_manager.sh
```

## Notes

- Site-specific values in each submit manager still need to be reviewed before submitting jobs.
- Generated local test artifacts such as `.local/`, `ip_registry.txt`, `local_test_data/`, and Rust `target/` directories are not part of the intended source tree.
