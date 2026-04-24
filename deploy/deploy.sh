#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./deploy.sh local [remote]
  ./deploy.sh prod [remote]

Examples:
  ./deploy.sh prod
  ./deploy.sh prod deploy@87.242.119.130
  ./deploy.sh local localhost

Environment overrides (all optional; values in .env.* files take precedence):
  DEPLOY_DIR     Absolute path to the deploy dir on the target host.
                 Single source of truth: read from the env file, also
                 consumed by docker-compose.yml as ${DEPLOY_DIR}.
  DEPLOY_REMOTE  SSH target (e.g. deploy-hiretrack or user@host). Read
                 from the env file if not passed as the second argument.
  DEPLOY_SSH_KEY Optional SSH private key path.
  DEPLOY_SSH_PORT Optional SSH port. Default: 22.
USAGE
}

mode="${1:-}"
remote_arg="${2:-}"

case "$mode" in
  local|prod) ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    echo "Unknown deploy mode: $mode" >&2
    usage >&2
    exit 2
    ;;
esac

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

if [[ "$mode" == "prod" ]]; then
  env_file="$script_dir/.env.production"
  env_example="$script_dir/env.production.example"
  default_remote="deploy-hiretrack"
else
  env_file="$script_dir/.env.local"
  env_example="$script_dir/env.local.example"
  default_remote="localhost"
fi

if [[ ! -f "$env_file" ]]; then
  env_file="$env_example"
fi

if [[ ! -f "$env_file" ]]; then
  echo "Missing env file for mode '$mode': expected $env_file" >&2
  exit 1
fi

# Read DEPLOY_DIR / DEPLOY_REMOTE from the env file in a sub-shell so the
# parent script never inherits arbitrary keys. Caller env wins via -u defaults.
read_env_value() {
  local key="$1"
  ( set -a; . "$env_file" >/dev/null 2>&1; printf '%s' "${!key:-}" )
}

deploy_dir="${DEPLOY_DIR:-$(read_env_value DEPLOY_DIR)}"
if [[ -z "$deploy_dir" ]]; then
  echo "DEPLOY_DIR is not set in $env_file or the environment." >&2
  exit 1
fi

remote="${remote_arg:-${DEPLOY_REMOTE:-$(read_env_value DEPLOY_REMOTE)}}"
remote="${remote:-$default_remote}"
ssh_port="${DEPLOY_SSH_PORT:-22}"

ssh_opts=(-p "$ssh_port")
scp_opts=(-P "$ssh_port")
rsync_ssh=(ssh -p "$ssh_port")
if [[ -n "${DEPLOY_SSH_KEY:-}" ]]; then
  ssh_opts+=(-i "$DEPLOY_SSH_KEY")
  scp_opts+=(-i "$DEPLOY_SSH_KEY")
  rsync_ssh+=(-i "$DEPLOY_SSH_KEY")
fi

echo "Deploy mode: $mode"
echo "Remote:     $remote"
echo "Deploy dir: $deploy_dir"
echo "Env source: $env_file"

ssh "${ssh_opts[@]}" "$remote" "mkdir -p '$deploy_dir'"

rsync -az --delete \
  --exclude 'ansible/' \
  --exclude 'backups/' \
  --exclude '.env' \
  --exclude '.env.local' \
  --exclude '.env.production' \
  --exclude 'env.local.example' \
  --exclude 'env.production.example' \
  --exclude '.DS_Store' \
  -e "${rsync_ssh[*]}" \
  "$script_dir/" "$remote:$deploy_dir/"

scp "${scp_opts[@]}" "$env_file" "$remote:$deploy_dir/.env"

ssh "${ssh_opts[@]}" "$remote" bash -s <<REMOTE_EOF
set -euo pipefail
cd '$deploy_dir'
docker compose config -q
docker compose pull
docker compose up -d --remove-orphans
docker compose ps
REMOTE_EOF
