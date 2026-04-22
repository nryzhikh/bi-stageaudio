# syntax=docker/dockerfile:1.7
# -----------------------------------------------------------------------------
# HireTrack sync client — one-shot worker container that drains the Flask API on
# the Windows host into the MySQL service in this compose stack.
#
# Scheduling is handled by host-side systemd timers that run:
#   docker compose run --rm sync-client [full-refresh]
# This keeps the container single-purpose: run a sync, write logs, exit.
# -----------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Layer caching: install deps before copying source so code edits don't
# invalidate the pip layer.
COPY apps/sync-worker/requirements.txt /app/apps/sync-worker/requirements.txt
RUN pip install --no-cache-dir -r /app/apps/sync-worker/requirements.txt

COPY apps/sync-worker/ /app/apps/sync-worker/

# Non-root runtime. /data/reports is bind-mountable for persisted sync reports.
RUN useradd --system --uid 10001 --home-dir /app --shell /usr/sbin/nologin sync \
 && mkdir -p /data/reports \
 && chmod +x /app/apps/sync-worker/entrypoint.sh \
 && chown -R sync:sync /data /app

USER sync

ENV SYNC_REPORT_DIR=/data/reports

# Default mode: one-shot incremental sync.
ENTRYPOINT ["/app/apps/sync-worker/entrypoint.sh"]
CMD []
