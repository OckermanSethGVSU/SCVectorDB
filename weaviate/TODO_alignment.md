# Weaviate Alignment TODO

This note tracks follow-up work after recent Weaviate schema/runtime changes:

- insert-side env vars were renamed to the shared `INSERT_*` convention
- `TASK` is now normalized to uppercase internally
- several insert-side variables were moved into `common/schema.sh`

## Immediate Fixes

- Add explicit `TASK` choices to [weaviate/schema.sh](/home/seth/Documents/research/SCVectorDB/weaviate/schema.sh) and make them uppercase:
  `INSERT INDEX QUERY_BS QUERY_CORE QUERY_SCALING`
  Reason: shared insert variable conditions currently use uppercase task names, but Weaviate itself does not yet advertise its valid task set in schema choices.

- Revisit the shared `INSERT_DATA_FILEPATH` requirement for Weaviate.
  Current shared rule is `TASK=INSERT|INDEX|QUERY|MIXED`.
  Weaviate tasks are `INSERT`, `INDEX`, `QUERY_BS`, `QUERY_CORE`, `QUERY_SCALING`.
  Result: the shared condition is not semantically correct for Weaviate.
  Fix options:
  1. Extend schema conditions to support more than one task family per engine.
  2. Keep the shared variable, but add an explicit Weaviate-side validation for the right task set.

- Recheck Weaviate defaults that changed when insert vars moved to shared.
  Current shared defaults now override old Weaviate-local ones:
  `INSERT_DATA_FILEPATH=""`, `INSERT_CORPUS_SIZE=""`, `INSERT_BATCH_SIZE=512`
  Old Weaviate defaults were dataset-specific path, `10000000`, and `2048`.
  Decide whether Weaviate should:
  1. keep shared defaults exactly, or
  2. reintroduce engine-specific fallback defaults in `engine_apply_overrides()`.

## Behavior Alignment With Qdrant/Milvus

- Add `INSERT_STREAMING` support to Weaviate or explicitly document that it is unsupported.
  Qdrant and Milvus expose `INSERT_STREAMING`; Weaviate currently inherits it from shared schema but does not use it.

- Decide whether `INSERT_CLIENTS_PER_WORKER` should stay Weaviate-specific or be moved to shared later.
  It now matches Qdrant in naming and meaning, but Milvus still uses `INSERT_CLIENTS_PER_PROXY`.
  This blocks a clean shared move.

- Decide whether query-side names should also be aligned.
  Weaviate still uses query-specific naming that does not match Qdrant/Milvus, especially:
  `QUERY_WORKLOAD`, `QUERY_TOPK`, `QUERY_EF`, and `QUERY_CLIENT_MODE`.

- Review whether Weaviate should adopt the same uppercase task naming everywhere, including docs and examples outside the sample config files already updated.
  Search targets:
  `README.md`, helper scripts, graphing configs, and any external runbooks.

## Validation / Tooling

- Build and test the Go clients after the env-var rename:
  [weaviate/clients/go_client/insert_streaming.go](/home/seth/Documents/research/SCVectorDB/weaviate/clients/go_client/insert_streaming.go)
  [weaviate/clients/go_client/index.go](/home/seth/Documents/research/SCVectorDB/weaviate/clients/go_client/index.go)
  This could not be completed in the sandbox because Go dependency/module cache writes were blocked.

- Run one real Weaviate `generate-only` flow after the client binary exists.
  Current verification is blocked by missing `weaviate/clients/go_client/insert_streaming`.

- Fix `prompt_remove_path` behavior when `/dev/tty` is unavailable.
  Current failed generate-only runs can loop on cleanup prompts in non-interactive execution.
  This is not Weaviate-specific, but it makes Weaviate verification harder.

## Nice-to-Have Cleanup

- Consider adding a small Weaviate task-normalization helper in one place instead of inlining `TASK="${TASK^^}"`.

- Consider normalizing run-directory naming across engines.
  Weaviate currently includes task names like `QUERY_SCALING` directly in run-dir names, while other engines use their own naming conventions.
