# Runtime And Artifacts

This directory holds mutable local files that should not live beside source
code.

- `runtime/` — local state such as `sync_state.json`.
- `artifacts/` — generated CSVs, reports, and exploratory outputs.

The production Docker deployment keeps its own runtime state in named volumes.
This directory is for local/manual runs and checked-in structure only.
