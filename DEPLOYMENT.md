# Deployment Guide

This repo now deploys in two parts:

- Windows: Flask API next to NexusDB
- Linux VPS: Docker Compose stack with Ofelia scheduling one-shot sync worker
  containers

## 1. Windows API

The Windows host runs `apps/api/app.py` as a service and exposes:

- `GET /health`
- `GET /api/tables`
- `GET /api/table/<name>`
- `GET /api/table/<name>/schema`
- `GET /api/table/<name>/count`

Recommended network shape:

- Keep the API behind Tailscale or a tightly scoped firewall rule
- Enable `API_USERNAME` / `API_PASSWORD` if traffic leaves a private network

Basic deployment flow:

```bash
scp -r apps/api/* Admin@<WINDOWS_HOST>:"E:/hiretrack-flask-api/server/"
```

Then on Windows, install the service from `E:\hiretrack-flask-api\server`.

## 2. Linux VPS Stack

Expected checkout path:

```text
/opt/hiretrack-sync
```

### Configure environment

```bash
cd /opt/hiretrack-sync/deploy
cp env.production.example .env
$EDITOR .env
```

Set:

- `COMPOSE_PROJECT_NAME`
- `COMPOSE_PROFILES`
- `DEPLOY_WORKDIR`
- `API_URL`
- `API_USERNAME` / `API_PASSWORD` if needed
- `SYNC_CLIENT_IMAGE`
- `MYSQL_ROOT_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `SUPERSET_SECRET_KEY`
- `TZ`

### Start the stack

```bash
cd /opt/hiretrack-sync/deploy
docker compose up -d mysql adminer superset metabase scheduler-runner ofelia
```

Set `COMPOSE_PROFILES=proxy` in `.env` if you want Caddy enabled in the normal
startup path. You can still override it ad hoc:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile proxy up -d
```

The sync worker is not started with `docker compose up`. It stays a one-shot
job definition, and Ofelia schedules `docker compose run --rm sync-worker ...`
against this same compose file. The VPS only needs the `deploy/` runtime bundle
plus the prebuilt sync-worker image referenced by `SYNC_CLIENT_IMAGE`.

## 3. Sync Worker Execution Model

Incremental run:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile manual run --rm sync-worker
```

Full refresh:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile manual run --rm sync-worker full-refresh
```

Selected tables:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile manual run --rm sync-worker --tables JOBS EQLISTS
```

Ofelia schedules two jobs from labels on `scheduler-runner`:

- incremental sync every 15 minutes
- full refresh every Sunday at 03:00 UTC

## 4. Operations

Validate the compose config:

```bash
cd /opt/hiretrack-sync/deploy
docker compose config -q
```

Check the scheduler:

```bash
cd /opt/hiretrack-sync/deploy
docker compose ps ofelia scheduler-runner
docker compose logs -f ofelia
```

Run a sync immediately:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile manual run --rm sync-worker
```

Run a full refresh immediately:

```bash
cd /opt/hiretrack-sync/deploy
docker compose --profile manual run --rm sync-worker full-refresh
```

Pause or resume scheduling:

```bash
cd /opt/hiretrack-sync/deploy
docker compose stop ofelia
docker compose start ofelia
```

Compose service logs:

```bash
cd /opt/hiretrack-sync/deploy
docker compose logs -f mysql superset metabase
```

## 5. Data Safety Model

- Incremental sync writes into a temp staging table, then merges into the live
  table and updates `_sync_table_state` together.
- Full refresh writes into a dedicated staging table, then atomically swaps it
  into place with `RENAME TABLE`.
- Watermarks are stored in MySQL metadata table `_sync_table_state`, not in a
  JSON file.

Scheduled runs use Ofelia's `no-overlap` option, and the worker itself still
uses the MySQL advisory run lock via `--lock-timeout 0`, so overlap protection
exists at both the scheduler and application layers.
