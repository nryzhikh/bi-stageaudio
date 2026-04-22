#!/bin/sh
# HireTrack sync-client entrypoint. Intended to be run as a one-shot container
# via `docker compose run --rm sync-client [full-refresh]`.
#
# Modes:
#   <no args>     One-shot incremental sync. Respects sync_config.yaml.
#   full-refresh  One-shot reload of every non-skipped table.
#   <anything>    Forwarded directly to sync_to_mysql.py, e.g.
#                     docker compose run --rm sync-client --tables OpScans
set -eu

CMD_DEFAULT_ARGS="--report ${SYNC_REPORT_DIR}/sync_report.csv"

case "${1:-}" in
  "")
    # shellcheck disable=SC2086
    exec python -u /app/apps/sync-worker/sync_to_mysql.py ${CMD_DEFAULT_ARGS}
    ;;
  now)
    shift
    # shellcheck disable=SC2086  # intentional word splitting for arg list
    exec python -u /app/apps/sync-worker/sync_to_mysql.py ${CMD_DEFAULT_ARGS} "$@"
    ;;
  full-refresh)
    shift
    # shellcheck disable=SC2086
    exec python -u /app/apps/sync-worker/sync_to_mysql.py ${CMD_DEFAULT_ARGS} --full-refresh "$@"
    ;;
  *)
    # shellcheck disable=SC2086
    exec python -u /app/apps/sync-worker/sync_to_mysql.py ${CMD_DEFAULT_ARGS} "$@"
    ;;
esac
