# Deployment Guide

This repo now deploys in two parts:

- Windows: Flask API next to NexusDB
- Linux VPS: Docker Compose stack plus host-side `systemd` timers that launch
  one-shot sync worker containers

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
cp env.example .env
$EDITOR .env
```

Set:

- `API_URL`
- `API_USERNAME` / `API_PASSWORD` if needed
- `SYNC_CLIENT_IMAGE`
- `MYSQL_ROOT_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `SUPERSET_SECRET_KEY`

### Start long-running services

```bash
cd /opt/hiretrack-sync/deploy
docker compose up -d mysql adminer superset metabase caddy
```

The sync worker is not started with `docker compose up`. It is a one-shot job.
The VPS only needs the `deploy/` runtime bundle plus the prebuilt sync-worker
image referenced by `SYNC_CLIENT_IMAGE`.

## 3. Sync Worker Execution Model

Incremental run:

```bash
cd /opt/hiretrack-sync/deploy
docker compose run --rm sync-client
```

Full refresh:

```bash
cd /opt/hiretrack-sync/deploy
docker compose run --rm sync-client full-refresh
```

Selected tables:

```bash
cd /opt/hiretrack-sync/deploy
docker compose run --rm sync-client --tables JOBS EQLISTS
```

## 4. Install systemd Timers

Copy the provided units:

```bash
sudo cp /opt/hiretrack-sync/deploy/systemd/hiretrack-sync.service /etc/systemd/system/
sudo cp /opt/hiretrack-sync/deploy/systemd/hiretrack-sync.timer /etc/systemd/system/
sudo cp /opt/hiretrack-sync/deploy/systemd/hiretrack-sync-full-refresh.service /etc/systemd/system/
sudo cp /opt/hiretrack-sync/deploy/systemd/hiretrack-sync-full-refresh.timer /etc/systemd/system/
```

Enable them:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hiretrack-sync.timer
sudo systemctl enable --now hiretrack-sync-full-refresh.timer
```

Check them:

```bash
systemctl list-timers 'hiretrack-sync*'
systemctl status hiretrack-sync.timer
systemctl status hiretrack-sync-full-refresh.timer
```

## 5. Operations

Run immediately:

```bash
sudo systemctl start hiretrack-sync.service
sudo systemctl start hiretrack-sync-full-refresh.service
```

Logs:

```bash
journalctl -u hiretrack-sync.service -n 200 --no-pager
journalctl -u hiretrack-sync-full-refresh.service -n 200 --no-pager
```

Compose service logs:

```bash
cd /opt/hiretrack-sync/deploy
docker compose logs -f mysql superset
```

## 6. Data Safety Model

- Incremental sync writes into a temp staging table, then merges into the live
  table and updates `_sync_table_state` together.
- Full refresh writes into a dedicated staging table, then atomically swaps it
  into place with `RENAME TABLE`.
- Watermarks are stored in MySQL metadata table `_sync_table_state`, not in a
  JSON file.

## 7. Suggested Next Hardening Step

Add a database-backed run lock so two manual or scheduled executions cannot
overlap.
