# HireTrack Sync API

Expose the HireTrack NexusDB database from Windows over HTTP, then replicate it
into MySQL on a Linux VPS for Superset and other analytics tools.

## Architecture

```text
Windows Server                      Linux VPS
┌─────────────────────────┐         ┌────────────────────────────────────┐
│ Flask API               │  HTTP   │ docker compose                    │
│ apps/api/app.py         │ ─────►  │ - mysql                           │
│                         │         │ - superset                        │
│ ODBC -> NexusDB         │         │ - adminer / metabase / caddy      │
└─────────────────────────┘         │                                    │
                                    │ ofelia scheduler                   │
                                    │ - runs scheduled compose jobs      │
                                    │                                    │
                                    │ one-shot worker container          │
                                    │ - docker compose run --rm          │
                                    │   sync-worker [full-refresh]       │
                                    └────────────────────────────────────┘
```

## Project Structure

```text
apps/
  api/                  Windows Flask API near NexusDB
  sync-worker/          One-shot sync worker container
deploy/
  docker-compose.yml    Linux VPS stack
  env.local.example     Local compose environment template
  env.production.example Production compose environment template
tools/hiretrack-ops/    Operator-only discovery and one-off scripts
```

## Quick Start

### 1. Deploy the Windows API

Copy `apps/api/` to the Windows host and install it as a service. See
[DEPLOYMENT.md](DEPLOYMENT.md) for the Windows-side steps.

### 2. Configure the Linux VPS

```bash
cd deploy
cp env.local.example .env
$EDITOR .env
```

Set at least:

- `API_URL`
- `API_USERNAME` / `API_PASSWORD` if the Windows API uses auth
- `COMPOSE_PROJECT_NAME`
- `COMPOSE_PROFILES`
- `DEPLOY_WORKDIR`
- `SYNC_CLIENT_IMAGE`
- `MYSQL_*`
- `SUPERSET_SECRET_KEY`

### 3. Start the Compose Stack

```bash
cd deploy
docker compose up -d mysql adminer superset metabase scheduler-runner ofelia
```

Set `COMPOSE_PROFILES=proxy` in `.env` if you want Caddy enabled as part of the
normal `docker compose up -d` flow.

The sync worker is no longer a long-running container. It is started on demand
by `docker compose --profile manual run --rm sync-worker`, and Ofelia schedules
the same one-shot compose run on the VPS.

### 4. Run the Sync Worker Manually

```bash
cd deploy

# Incremental sync
docker compose --profile manual run --rm sync-worker

# Selected tables
docker compose --profile manual run --rm sync-worker --tables JOBS EQLISTS

# Full refresh
docker compose --profile manual run --rm sync-worker full-refresh
```

### 5. Check the Scheduler

```bash
docker compose logs -f ofelia
```

## Operations

```bash
# Run immediately
docker compose --profile manual run --rm sync-worker
docker compose --profile manual run --rm sync-worker full-refresh

# Pause/resume scheduling
docker compose stop ofelia
docker compose start ofelia
```

## Notes

- The VPS deploy bundle is only `deploy/`, not the whole repo checkout.
- Incremental watermarks live in MySQL metadata table `_sync_table_state`.
- Incremental sync uses temp staging plus atomic metadata update.
- Full refresh uses a staging table plus atomic `RENAME TABLE` swap.
- One-off utilities remain in `tools/hiretrack-ops/` and are not part of the
  production scheduler path.

## License

Internal use only.
