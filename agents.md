# Agents Guide for SCVectorDB

This file is intended to help future Codex runs work effectively in this repository without re-discovering the same context each time. It is a living document. Treat it as a practical operator guide, not as canonical product documentation.

## Repository Summary

SCVectorDB is a research repository for large-scale vector database experiments on PBS-based HPC systems. The main active workflows are:

- `qdrant/`: distributed Qdrant launch, Rust clients, local smoke test, and post-run summaries.
- `milvus/`: standalone and distributed Milvus launch, Go clients, tracing helpers, and post-run summaries.
- `graphing/`: analysis scripts and notebooks for insert/query/index outputs.
- `weaivate/`: early Weaviate scaffolding. The directory name is spelled `weaivate` in the repo, even though the scripts inside refer to `weaviateSetup`.

The repo is not a normal library or app with one build/test entrypoint. It is a workflow collection with:

- shell-driven orchestration
- site-specific absolute paths
- generated PBS submission scripts
- generated run directories
- prebuilt binaries and containers that may not exist in a fresh checkout

Assume that many scripts are intended for Aurora or Polaris specifically.

## Top-Level Mental Model

The common pattern is:

1. Edit or override variables in `pbs_submit_manager.sh`.
2. Generate `submit.sh` by echoing PBS directives and environment variables into it.
3. Append `main.sh` into the generated script.
4. Create a run directory named from the experiment parameters.
5. Copy runtime scripts and assets into that run directory.
6. Optionally call `qsub submit.sh`.

This means there are two levels of behavior:

- source files under version control
- generated run directories and artifacts created from those source files

When changing workflow logic, prefer editing the source script in the repo, not a generated run directory.

## Important Entry Points

### Qdrant

- Main HPC submit manager: `qdrant/pbs_submit_manager.sh`
- Main HPC runtime: `qdrant/main.sh`
- Local smoke test: `qdrant/local_test.sh`
- Rust client build helper: `qdrant/rustCode/compile.sh`
- Topology/config helpers: `qdrant/generalPython/*.py`

Operational notes:

- `qdrant/pbs_submit_manager.sh` currently runs dependency checks at startup.
- It supports environment overrides via `*_OVERRIDE` variables.
- It writes `submit.sh` in-place, stages a run directory, and comments indicate `qsub` is not always enabled.
- `qdrant/main.sh` expects a PBS environment and uses `$PBS_NODEFILE`.
- `qdrant/main.sh` launches worker ranks with MPI, waits for signal files, then runs the Rust client with `ACTIVE_TASK=INSERT` or `ACTIVE_TASK=QUERY`.
- `qdrant/local_test.sh` is for testing on local devices not HPC machines. It can exercise either:
  - the standard insert-then-query path via `multiClientOP`
  - the mixed insert/query local runner via `mixedrunner`

### Milvus

- Main HPC submit manager: `milvus/pbs_submit_manager.sh`
- Main HPC runtime: `milvus/main.sh`
- Dependency checker: `milvus/check_dependencies.sh`
- Go build helper: `milvus/goCode/build.sh`
- Collection/index/profiling helpers: `milvus/generalPython/*.py`
- Runtime launch scripts: `milvus/milvusSetup/*.sh`

Operational notes:

- `milvus/pbs_submit_manager.sh` has a richer configuration surface than the Qdrant one.
- It supports override variables such as `QUEUE_OVERRIDE`, `TASK_OVERRIDE`, `MODE_OVERRIDE`, and array overrides for values like `NODES`.
- The dependency check near the top is currently commented out.
- `milvus/main.sh` supports `MODE=STANDALONE` and `MODE=DISTRIBUTED`.
- Distributed mode launches etcd, MinIO, coordinator, streaming nodes, query, data, and proxy roles.
- Tracing is optional and goes through `milvus/utils/launch_otel.sh`.

### Graphing

- `graphing/cloud_analysis.py`
- `graphing/extract.py`
- `graphing/index/*.py`
- `graphing/query/*.py`
- several notebooks for interactive analysis

These appear to be post-processing utilities, not part of the runtime control plane. Prefer not to introduce workflow assumptions here unless they match real output formats produced by `qdrant/` or `milvus/`.

### Weaviate Scaffold

- `weaivate/pbs_submit_manager.sh`
- `weaivate/main.sh`
- `weaivate/weaviateSetup/launchWeaviateNode.sh`

This area looks incomplete compared with Qdrant and Milvus. Treat it as scaffolding unless the user explicitly wants to invest in it.

## Languages and Tooling

The repo mixes:

- Bash for orchestration
- Python for setup, status, summaries, and graphing
- Rust for Qdrant clients
- Go for Milvus clients
- Jupyter notebooks for analysis

There is no single package manager or monorepo build system.

## HPC Assumptions

Many scripts assume the following exist on the target system:

- PBS Pro and `$PBS_NODEFILE`
- `mpirun`
- `module` or `ml`
- Apptainer
- Python environments at absolute site-specific paths
- prebuilt binaries
- shared filesystems like `flare` or `eagle`
- optional DAOS helper scripts such as `launch-dfuse.sh` and `clean-dfuse.sh`

Do not “clean up” these absolute paths casually. They are often the real deployment contract for this repo.

## Generated and Non-Source Artifacts

Expect untracked generated artifacts. Do not treat them as source by default. Examples already present in this repo include:

- `qdrant/.local/`
- `qdrant/ip_registry.txt`
- `qdrant/local_test_data/`
- `qdrant/rustCode/mixedRunner/target/`
- generated `submit.sh` files inside run directories
- `workflow.out`, `workflow.log`, `output.log`
- `uploadNPY/`, `systemStats/`, and JSONL mixed-run logs

If a task involves cleanup, confirm whether the user wants generated artifacts removed. Do not delete them automatically.

## Known Inconsistencies and Footguns

These are important for future agents:

### Naming and path inconsistencies

- The repo directory is `weaivate/`, not `weaviate/`.
- Qdrant docs refer to `mixedrunner`, but the actual tracked directory is `qdrant/rustCode/mixedRunner/`.
- Some dependency scripts appear stale relative to current code layout.

### Stale dependency expectations

- `qdrant/check_dependencies.sh` still expects older artifacts like `rustCode/multiClientUpload/multiClientUpload` and `perf/perf`.
- `milvus/check_dependencies.sh` references paths such as `goCode/multiClientInsert/...` and `generalPython/insert_multi_client_summary.py`, which do not match the currently visible tree.

Do not assume dependency checkers are fully authoritative. Inspect them before relying on them as truth.

### HPC-specific hardcoding

- Many scripts `cd` into absolute paths on Aurora or Polaris.
- Several launch scripts assume exact module names and filesystem mounts.
- Some scripts embed project allocation names such as `radix-io`.

When making changes, preserve these contracts unless the user explicitly asks to generalize them.

### Generated-script behavior

- The submit managers append to `submit.sh` with `>>`.
- If `submit.sh` already exists and is not removed first, repeated runs may concatenate stale content.

If asked to harden the workflows, this is a good place to improve.

## Safe Editing Strategy

When working in this repo, follow this order:

1. Identify whether the request targets Qdrant, Milvus, graphing, or Weaviate.
2. Read the corresponding `README.md`, submit manager, and `main.sh`.
3. Check whether helper scripts or client code are actually wired into the main path.
4. Prefer minimal, source-level changes in the tracked workflow files.
6. If validation requires Aurora/Polaris or PBS, say so explicitly instead of guessing.

## Validation Strategy by Area

### Qdrant changes

Best options:

- static inspection of `qdrant/pbs_submit_manager.sh` and `qdrant/main.sh`
- `cargo build` in the relevant Rust project

Useful targets:

- `qdrant/rustCode/multiClientOP`
- `qdrant/rustCode/mixedRunner`

### Milvus changes

Best options:

- static inspection of shell scripts and Python helpers
- `go build` through `milvus/goCode/build.sh`

Limitations:

- true end-to-end validation is likely unavailable off-cluster
- distributed launch assumptions depend on PBS, MPI, modules, and site-local assets

### Graphing changes

Best options:

- run the affected Python scripts against existing output samples if present
- otherwise validate syntax and imports only

## Repo Conventions Future Agents Should Follow

- Preserve shell style unless there is a strong reason to refactor.
- Do not replace absolute HPC paths with placeholders unless explicitly requested.
- Do not normalize naming inconsistencies without checking for downstream references.
- Prefer adding small comments only where the orchestration flow is genuinely hard to infer.
- Treat generated local test outputs as disposable artifacts, not repo content.
- Be careful with dependency scripts; they may lag behind the active workflow.
- Check `git status --short` before editing because this repo often has local experiment artifacts.

## File-Level Notes

### `qdrant/pbs_submit_manager.sh`

Use this when changes involve:

- experiment parameter surfaces
- override behavior
- generated run directory naming
- submission behavior

Be careful about:

- `submit.sh` accumulation
- whether `qsub` is commented out
- staging rules for copied runtime assets

### `qdrant/main.sh`

Use this when changes involve:

- cluster launch order
- profiler start/stop behavior
- insert/index/query control flow
- readiness and status signaling

Be careful about:

- assumptions around `$PBS_NODEFILE`
- `ip_registry.txt`
- `NO_PROXY`/`HTTP_PROXY` clearing around Python and Rust clients


Be careful about:

- local container runtime requirements
- local artifact generation under `.local/` and `local_test_data/`

### `milvus/pbs_submit_manager.sh`

Use this when changes involve:

- sweep variables
- standalone versus distributed mode configuration
- tracing/perf controls
- generated run directory setup

Be careful about:

- many override variables
- platform-specific path defaults
- distributed-role sizing parameters

### `milvus/main.sh`

Use this when changes involve:

- standalone launch
- distributed component startup
- tracing
- collection setup
- Go client execution

Be careful about:

- rank placement logic
- signal-file readiness checks
- DAOS setup and cleanup

## Suggested Future Improvements

These are good candidates for later cleanup work:

- add a real repo-level `.gitignore` for generated local test artifacts and build output
- fix stale dependency scripts so they reflect current client binaries and paths
- normalize `mixedrunner` vs `mixedRunner`
- make submit managers overwrite `submit.sh` explicitly instead of only appending
- document expected run-directory contents with examples
- add one lightweight validation command per active workflow
- decide whether `weaivate/` should remain scaffolding or be completed/renamed

## How Future Codex Runs Should Start

For most tasks in this repo:

1. Run `git status --short`.
2. Inspect the relevant README and workflow entrypoint files.
3. Separate tracked source from generated experiment output.
4. Make the smallest source change that matches the user request.
5. Validate with local builds or local smoke tests when possible.
6. If the change depends on Aurora/Polaris-only behavior, state the validation gap clearly.

## Status of This Guide

This guide is a first-pass operational map generated from the current repository contents on 2026-03-24. It should be updated as the repo evolves, especially when:

- a workflow becomes the new canonical path
- dependency scripts are repaired
- directory names are normalized
- local validation paths improve
