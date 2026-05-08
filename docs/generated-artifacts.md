# Generated Artifacts

This repo often has a dirty tree because experiment runs generate files next to
tracked source.

## Common Generated Content

- generated run directories under `qdrant/`, `milvus/`, and `weaviate/`
- `.local/` local runtime state
- `runtime_state/`
- `workflow.out`, `workflow.log`, `output.log`
- `uploadNPY/`, `queryNPY/`, `clientTiming/`, `systemStats/`
- built client binaries
- container payloads such as `sifs/`
- graph outputs, CSV summaries, and notebooks under `graphing/`

## Practical Rules

- Do not treat generated run directories as canonical source.
- Do not edit generated run directories when the user wants workflow changes.
- Do not delete generated artifacts unless the user explicitly asked for cleanup.
- Check `git status --short` before editing anything.
