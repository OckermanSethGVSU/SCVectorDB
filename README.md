# SCVectorDB

SCVectorDB contains HPC workflows for vector database experiments across Qdrant, Milvus, and Weaviate.

## Layout

```text
.
├── pbs_submit_manager.sh
├── common/
├── graphing/
├── milvus/
├── qdrant/
└── weaviate/
```

## Unified submit flow

The repository now uses a single top-level submit entrypoint:

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine qdrant --config path/to/run.env
./pbs_submit_manager.sh --generate-only --engine qdrant --config path/to/run.env
```

Behavior:

- normal execution generates the run directory and submits if `RUN_MODE=PBS`
- `--generate-only` generates the run directory but does not submit
- `--help --engine <engine>` prints the engine variables, defaults, and requirement status

Each engine keeps its engine-specific logic in its own directory:

- `qdrant/engine.sh`
- `milvus/engine.sh`
- `weaviate/engine.sh`

Shared submit helpers live in:

- `common/submit_lib.sh`

## Config model

Runs can be configured with:

- `--set KEY=value`
- `--config path/to/file.env`

Config files are plain `KEY=value` env-style files. Example:

```bash
ENGINE=qdrant
TASK=QUERY
RUN_MODE=local
PLATFORM=AURORA
WALLTIME=00:10:00
QUEUE=debug-scaling
ACCOUNT=myproj
INSERT_FILEPATH=/path/to/base.npy
QUERY_FILEPATH=/path/to/query.npy
```

## Current engine status

- `qdrant/`: actively migrated to the unified interface and current focus
- `milvus/`: using the unified top-level interface with engine-specific logic in `milvus/`
- `weaviate/`: using the unified top-level interface with engine-specific logic in `weaviate/`

## Environment expectations

These workflows assume some combination of:

- PBS Pro
- MPI / `mpirun`
- Apptainer
- Python 3
- site modules
- optional DAOS helpers

Most HPC runs still depend on site-specific paths for:

- datasets
- Python environments
- container images or server executables
- output/storage locations

## Qdrant quick start

Inspect variables:

```bash
./pbs_submit_manager.sh --help --engine qdrant
```

Generate only:

```bash
./pbs_submit_manager.sh --generate-only --engine qdrant --config qdrant_run.env
```

Normal run:

```bash
./pbs_submit_manager.sh --engine qdrant --config qdrant_run.env
```

For PBS Qdrant runs, provide a Qdrant SIF before generating or submitting:

```bash
qdrant/utils/download_sif.sh 1.16.1
```

Then set `QDRANT_SIF` in the run config to the filename under `qdrant/sifs/`:

```bash
QDRANT_SIF=qdrant_v1.16.1.sif
```

Sample configs live under `qdrant/sampleConfigs/`:

```bash
./pbs_submit_manager.sh --generate-only --config qdrant/sampleConfigs/aurora_yandex_query.env
```

## Notes

- Generated run directories include `run_config.env`, which is the resolved run configuration used by `submit.sh`.
- Generated artifacts, local data, and build outputs should generally not be committed.
