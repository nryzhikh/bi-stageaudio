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
                                    │ systemd timers                     │
                                    │ - hiretrack-sync.timer            │
                                    │ - hiretrack-sync-full-refresh.timer│
                                    │                                    │
                                    │ one-shot worker container          │
                                    │ - docker compose run --rm          │
                                    │   sync-client [full-refresh]       │
                                    └────────────────────────────────────┘
```

## Project Structure

```text
apps/
  api/                  Windows Flask API near NexusDB
  sync-worker/          One-shot sync worker container
deploy/
  docker-compose.yml    Linux VPS stack
  env.example           Compose environment file template
  systemd/              Host timer/service units for sync scheduling
tools/hiretrack-ops/    Operator-only discovery and one-off scripts
```

## Quick Start

### 1. Deploy the Windows API

Copy `apps/api/` to the Windows host and install it as a service. See
[DEPLOYMENT.md](DEPLOYMENT.md) for the Windows-side steps.

### 2. Configure the Linux VPS

```bash
cd deploy
cp env.example .env
$EDITOR .env
```

Set at least:

- `API_URL`
- `API_USERNAME` / `API_PASSWORD` if the Windows API uses auth
- `SYNC_CLIENT_IMAGE`
- `MYSQL_*`
- `SUPERSET_SECRET_KEY`

### 3. Start the Compose Stack

```bash
cd deploy
docker compose up -d mysql adminer superset metabase caddy
```

The sync worker is no longer a long-running container. It is started on demand
by `docker compose run --rm sync-client`, and the VPS pulls it by image tag
instead of building it from source.

### 4. Run the Sync Worker Manually

```bash
cd deploy

# Incremental sync
docker compose run --rm sync-client

# Selected tables
docker compose run --rm sync-client --tables JOBS EQLISTS

# Full refresh
docker compose run --rm sync-client full-refresh
```

### 5. Install Production Timers

```bash
sudo cp deploy/systemd/hiretrack-sync.service /etc/systemd/system/
sudo cp deploy/systemd/hiretrack-sync.timer /etc/systemd/system/
sudo cp deploy/systemd/hiretrack-sync-full-refresh.service /etc/systemd/system/
sudo cp deploy/systemd/hiretrack-sync-full-refresh.timer /etc/systemd/system/

sudo systemctl daemon-reload
sudo systemctl enable --now hiretrack-sync.timer
sudo systemctl enable --now hiretrack-sync-full-refresh.timer
```

## Operations

```bash
# Timer status
systemctl list-timers 'hiretrack-sync*'

# Run immediately
sudo systemctl start hiretrack-sync.service
sudo systemctl start hiretrack-sync-full-refresh.service

# Inspect logs
journalctl -u hiretrack-sync.service -n 200 --no-pager
journalctl -u hiretrack-sync-full-refresh.service -n 200 --no-pager
```

## Notes

- The VPS deploy bundle is only `deploy/`, not the whole repo checkout.
- Incremental watermarks live in MySQL metadata table `_sync_table_state`.
- Incremental sync uses temp staging plus atomic metadata update.
- Full refresh uses a staging table plus atomic `RENAME TABLE` swap.
- One-off utilities remain in `tools/hiretrack-ops/` and are not part of the
  production timer path.

## License

Internal use only.
