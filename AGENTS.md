# Agents Guide for SCVectorDB

This file is a practical operator guide for future Codex runs in this repo. It is not canonical product documentation. Treat it as a current-state map of the tracked workflow files, the active entrypoints, and the main footguns.

## Repository Summary

SCVectorDB is a research repo for vector-database experiments on PBS-based HPC systems, plus local harnesses for some engines and post-run graphing utilities.

The active tracked areas are:

- `pbs_submit_manager.sh`: unified top-level submit/generate entrypoint for all engines
- `common/`: shared schema and submit helpers used by every engine
- `qdrant/`: active Qdrant workflow, Rust clients, PBS runtime, local runtime, and summary helpers
- `milvus/`: active Milvus workflow, Go clients, PBS runtime, local runtime, tracing helpers, and summary helpers
- `weaviate/`: active Weaviate PBS workflow, Go clients, and sample configs
- `graphing/`: analysis scripts and notebooks for generated outputs

The repo is not a normal library or service with one build or test command. It is a workflow collection with:

- shell-driven orchestration
- schema-backed config expansion
- generated run directories
- site-specific HPC assumptions
- locally built client binaries
- container images or SIF payloads that may not exist in a fresh checkout

## Current Mental Model

The current control plane is unified.

You do not start from per-engine submit managers. You start from the repo root:

```bash
./pbs_submit_manager.sh --help
./pbs_submit_manager.sh --help --engine qdrant
./pbs_submit_manager.sh --engine milvus --config path/to/run.env
./pbs_submit_manager.sh --generate-only --engine weaviate --config path/to/run.env
```

The top-level flow is:

1. `pbs_submit_manager.sh` resolves the selected engine directory.
2. It sources `common/submit_lib.sh` plus `<engine>/engine.sh`.
3. The engine loads shared schema from `common/schema.sh` and appends engine-specific schema from `<engine>/schema.sh`.
4. Config is assembled from defaults, `--config`, and `--set KEY=value`.
5. Sweep values expand into one or more parameter combinations.
6. For each combo, the manager creates a generated run directory inside the engine directory.
7. The engine stages payload files into that run directory.
8. The manager writes `submit.sh` and `run_config.env`.
9. It submits automatically only when `RUN_MODE=PBS` and generation succeeds.

There are two distinct layers in this repo:

- tracked source files under version control
- generated run directories and experiment outputs

If the user wants workflow changes, edit tracked source files, not generated run directories.

## Active Entry Points

### Top level

- `pbs_submit_manager.sh`: single submit/generate entrypoint for Qdrant, Milvus, and Weaviate
- `common/submit_lib.sh`: shared generation/submission helpers
- `common/schema.sh`: shared variable registry used by all engines
- `common/engine_schema_lib.sh`: shared schema registry and validation helpers

### Qdrant

- `qdrant/engine.sh`: unified-engine contract implementation
- `qdrant/schema.sh`: Qdrant-specific variables and defaults
- `qdrant/main.sh`: PBS runtime
- `qdrant/local_main.sh`: local container-backed runtime
- `qdrant/runtime/cluster/`: PBS launch helpers
- `qdrant/scripts/`: collection setup, profiling, indexing, summaries, mixed timeline helpers
- `qdrant/utils/`: inspect, dependency checks, SIF download, queue helpers
- `qdrant/clients/build.sh`: Rust client build helper
- `qdrant/clients/batch_client/`: main insert/query Rust client
- `qdrant/clients/mixed/`: mixed insert/query Rust client

### Milvus

- `milvus/engine.sh`: unified-engine contract implementation
- `milvus/schema.sh`: Milvus-specific variables and defaults
- `milvus/main.sh`: PBS runtime
- `milvus/local_main.sh`: local container-backed runtime
- `milvus/runtime/cluster/`: PBS role launchers
- `milvus/runtime/configs/`: staged runtime config payloads
- `milvus/scripts/`: collection setup, profiling, indexing, bulk import, summaries
- `milvus/utils/`: tracing, status, local standalone, inspect helpers
- `milvus/clients/build.sh`: Go client build helper
- `milvus/clients/batch_client/`: main insert/query Go client
- `milvus/clients/mixed/`: mixed insert/query Go client

### Weaviate

- `weaviate/engine.sh`: unified-engine contract implementation
- `weaviate/schema.sh`: Weaviate-specific variables and defaults
- `weaviate/main.sh`: PBS runtime
- `weaviate/weaviateSetup/`: cluster launch and interface mapping helpers
- `weaviate/clients/build_all.sh`: convenience build wrapper
- `weaviate/clients/go_client/build.sh`: Go client build helper
- `weaviate/clients/go_client/`: Weaviate client binaries built from individual `*.go` mains

Important: the directory is `weaviate/`, not `weaivate/`.

## Engine Status

Current reality:

- `qdrant/` is active and supports both `RUN_MODE=PBS` and `RUN_MODE=local`
- `milvus/` is active and supports both `RUN_MODE=PBS` and `RUN_MODE=local`
- `weaviate/` is active under the unified manager, but it currently stages PBS runs only

Do not assume older docs or stale scripts are authoritative if they conflict with `engine.sh`, `schema.sh`, or `main.sh`.

## Config Model

Config is schema-driven.

Common variables live in `common/schema.sh`. Engine-specific variables live in each engine `schema.sh`.

Typical config inputs:

- `--config path/to/file.env`
- `--set KEY=value`
- environment overrides translated by the submit layer

Config files are plain env-style `KEY=value` files. Important shared variables include:

- `TASK`
- `RUN_MODE`
- `PLATFORM` for PBS runs
- `NODES`
- `CORES`
- `STORAGE_MEDIUM`
- `ACCOUNT`, `QUEUE`, `WALLTIME` for PBS runs
- `ENV_PATH` or `ALLOW_SYSTEM_PYTHON=True` for PBS Python activation
- `VECTOR_DIM`
- `DISTANCE_METRIC`
- `INSERT_DATA_FILEPATH`
- `INSERT_CORPUS_SIZE`
- `INSERT_BATCH_SIZE`
- `QUERY_DATA_FILEPATH`
- `QUERY_CORPUS_SIZE`
- `QUERY_BATCH_SIZE`

When diagnosing config issues, inspect these files first:

- `common/schema.sh`
- `<engine>/schema.sh`
- `<engine>/engine.sh`

Those files are the truth for required variables, choices, and runtime staging.

## Per-Engine Notes

### Qdrant

Current Qdrant tasks in active runtime paths:

- `INSERT`
- `INDEX`
- `QUERY`
- `MIXED`
- `LAUNCH` in local and PBS runtime control flow

Operational notes:

- `qdrant/engine.sh` enforces `ENV_PATH` for PBS unless `ALLOW_SYSTEM_PYTHON=True`
- PBS runs require `QDRANT_SIF` as a filename under `qdrant/sifs/`
- local runs use `qdrant/local_main.sh` and a Docker or Podman Qdrant container
- mixed runs derive `INSERT_START_ID` from `RESTORE_DIR`, `INSERT_CORPUS_SIZE`, or `INSERT_DATA_FILEPATH` if needed
- generated run directories often contain `runtime_state/`, `uploadNPY/`, `queryNPY/`, `clientTiming/`, and `systemStats/`

Build targets:

```bash
cd qdrant/clients
./build.sh batch_client
./build.sh mixed
```

### Milvus

Current Milvus tasks in active runtime paths:

- `INSERT`
- `INDEX`
- `QUERY`
- `IMPORT`
- `MIXED`

Operational notes:

- `milvus/engine.sh` enforces `ENV_PATH` for PBS unless `ALLOW_SYSTEM_PYTHON=True`
- `MODE` can be `STANDALONE` or `DISTRIBUTED`
- `MINIO_MODE` is derived automatically when unset: `stripped` for distributed, `off` otherwise
- `ETCD_MEDIUM` defaults to `STORAGE_MEDIUM` when unset
- bulk ingest is controlled by `INSERT_METHOD`, `BULK_UPLOAD_TRANSPORT`, and `BULK_UPLOAD_STAGING_MEDIUM`
- mixed runs also derive `INSERT_START_ID` when possible
- tracing goes through `milvus/utils/launch_otel.sh` when `TRACING=True`

Build targets:

```bash
cd milvus/clients
./build.sh batch_client
./build.sh mixed
```

### Weaviate

Current Weaviate tasks in active runtime paths:

- `INSERT`
- `INDEX`
- `QUERY_BS`
- `QUERY_CORE`
- `QUERY_SCALING`

Operational notes:

- `weaviate/main.sh` is PBS-only in current tracked workflow
- the engine stages selected binaries from `weaviate/clients/go_client/`
- for `QUERY_SCALING`, two binaries must already exist: `INSERT_BIN` and `QUERY_SCALING_BIN`
- the launcher pulls the upstream Weaviate container inline via Apptainer; there is no local `download_sif.sh`
- `PLATFORM=AURORA` is the currently supported platform path in `weaviate/main.sh`

Build targets:

```bash
cd weaviate/clients
./build_all.sh
./go_client/build.sh query
./go_client/build.sh insert_streaming query
```

## Generated Artifacts and Dirty Trees

This repo frequently has untracked or generated experiment output. Expect dirty trees.

Common examples:

- generated run directories inside `qdrant/`, `milvus/`, or `weaviate/`
- `.local/` local runtime state
- `runtime_state/`
- `workflow.out`, `workflow.log`, `output.log`
- `uploadNPY/`, `queryNPY/`, `clientTiming/`, `systemStats/`
- built client binaries
- container payloads in `sifs/`
- notebooks, plots, CSV summaries, and graph outputs under `graphing/`

Rules for future agents:

- do not treat generated artifacts as source by default
- do not delete generated artifacts unless the user asked for cleanup
- do not edit generated run directories when the user actually wants source changes
- check `git status --short` before editing

## HPC Assumptions

Many tracked scripts assume some combination of:

- PBS Pro and `$PBS_NODEFILE`
- `mpirun`
- Apptainer
- site module systems
- Python environments at absolute paths
- DAOS helper scripts on the target cluster
- Aurora- or Polaris-specific runtime behavior

Do not casually replace site-specific absolute paths with placeholders. In this repo, those paths are often part of the real deployment contract.

## Validation Strategy

Prefer validation that matches the part of the repo being edited.

### Unified submit changes

Inspect:

- `pbs_submit_manager.sh`
- `common/submit_lib.sh`
- `common/schema.sh`
- `common/engine_schema_lib.sh`

Useful checks:

- `./pbs_submit_manager.sh --help`
- `./pbs_submit_manager.sh --help --engine qdrant`
- generate-only runs against sample configs when they do not require unavailable cluster assets

### Qdrant changes

Best local options:

- static inspection of `qdrant/engine.sh`, `qdrant/main.sh`, and `qdrant/local_main.sh`
- `cargo build` via `qdrant/clients/build.sh`

### Milvus changes

Best local options:

- static inspection of `milvus/engine.sh`, `milvus/main.sh`, and `milvus/local_main.sh`
- `go build` via `milvus/clients/build.sh`

### Weaviate changes

Best local options:

- static inspection of `weaviate/engine.sh` and `weaviate/main.sh`
- `go build` via `weaviate/clients/build_all.sh` or `weaviate/clients/go_client/build.sh`

### Graphing changes

Best local options:

- run the affected Python scripts against existing outputs if present
- otherwise validate imports and syntax only

When a change depends on PBS, MPI, Aurora, Polaris, Apptainer caches, or site-local files, say so explicitly instead of pretending to validate end to end.

## Known Footguns

Current hazards worth remembering:

- old docs or old mental models may still refer to per-engine submit managers; the active flow is now unified through the repo-root `pbs_submit_manager.sh`
- large generated run directories can live next to source and look deceptively similar
- some helper scripts and dependency checkers may lag behind the active workflow layout
- local binaries may already exist in the tree; do not assume they were just built from current source
- `milvus/` and `qdrant/` both support local and PBS runtimes; `weaviate/` does not currently have a local harness
- `qdrant` and `milvus` mixed modes rely on auto-derived `INSERT_START_ID` behavior; do not break that path casually
- Weaviate client binaries are built in place under `weaviate/clients/go_client/`; the engine expects those exact filenames

## Safe Editing Strategy

When working in this repo:

1. Identify the target area: unified submit path, `qdrant/`, `milvus/`, `weaviate/`, or `graphing/`.
2. Read the corresponding `README.md`, `engine.sh`, `schema.sh`, and runtime entrypoint.
3. Separate tracked source from generated outputs.
4. Make the smallest tracked-source change that satisfies the request.
5. Validate locally with builds or static checks when possible. Note that this may time out if the machine does not have enough CPU power. 
6. If cluster-only behavior is involved, call out the validation gap clearly.

## Recommended Starting Point for Future Runs

For most tasks:

1. Run `git status --short`.
2. Read `README.md` plus the engine-specific `README.md`.
3. Inspect the relevant `engine.sh`, `schema.sh`, and runtime entrypoint.
4. Confirm whether the user wants a source change or a generated-run artifact change.
5. Prefer editing tracked source files and regenerating behavior through the unified flow.

## Status of This Guide

This guide was rewritten to match the current tracked repo layout and unified submit flow on 2026-04-19. Update it again when:

- the unified submit contract changes
- an engine adds or removes local-mode support
- task names or schema variables change
- client build locations move
- major generated artifact conventions change
