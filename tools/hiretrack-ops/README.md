# HireTrack Operator Tools

These scripts are useful for exploration, one-off migrations, and operator
workflows. They are intentionally kept out of `apps/sync-worker/` so the
production cron service only contains runtime code.

## Scripts

- `discover_catalog.py` — infer candidate primary keys / modified columns.
- `probe_sizes.py` — count rows per table and emit a CSV sizing report.
- `sync_to_sqlite.py` — one-off sync into a local SQLite replica.
- `sync_to_postgres.py` — one-off sync into PostgreSQL.

## Setup

```bash
pip install -r tools/hiretrack-ops/requirements.txt
```
