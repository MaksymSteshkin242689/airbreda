#!/usr/bin/env bash
# Deploy AirBreda to the VM provisioned by infra/provision.sh.
#
#   ./infra/deploy.sh            # sync code, build images, migrate DB, install cron, (re)start dashboard
#   ./infra/deploy.sh --no-build # sync + restart only
#
# Reads VM_IP / KEY_FILE from infra/resources.env (written by provision.sh) and ships the
# git-ignored .env (cloud DB + bucket) to the VM over scp. Idempotent: safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
. infra/resources.env

SSH=(ssh -o StrictHostKeyChecking=accept-new -i "$KEY_FILE" "ec2-user@$VM_IP")
REMOTE=/home/ec2-user/airbreda
BUILD=1; [ "${1:-}" = "--no-build" ] && BUILD=0

log() { printf '\n==> %s\n' "$*"; }

log "Waiting for SSH + Docker on $VM_IP"
for _ in $(seq 1 30); do "${SSH[@]}" 'docker info >/dev/null 2>&1' && break; sleep 10; done
"${SSH[@]}" 'docker info >/dev/null 2>&1' || { echo "docker not ready on VM"; exit 1; }

log "Syncing sources"
"${SSH[@]}" "mkdir -p $REMOTE/logs"
rsync -az --delete -e "ssh -i $KEY_FILE" \
  --include='src/***' --include='db/***' --include='model/***' --include='infra/' --include='infra/crontab.txt' \
  --include='Dockerfile*' --include='requirements*.txt' --include='docker-compose.yml' \
  --exclude='*' ./ "ec2-user@$VM_IP:$REMOTE/"
scp -q -i "$KEY_FILE" .env "ec2-user@$VM_IP:$REMOTE/.env"
"${SSH[@]}" "chmod 600 $REMOTE/.env"

# The dashboard image bakes in the trained model (model/). It builds without one too — the API then
# serves the real readings with a null prediction — so the live system can go up before training.
[ -f model/model.pkl ] && echo "model/model.pkl present: predictions enabled" || echo "WARNING: no model/model.pkl yet — dashboard will serve readings without predictions"

if [ "$BUILD" = 1 ]; then
  log "Building ingestion images on the VM"
  "${SSH[@]}" "cd $REMOTE && docker build -q -t airbreda-air . && docker build -q -t airbreda-traffic -f Dockerfile.traffic ."
  log "Building dashboard image on the VM"
  "${SSH[@]}" "cd $REMOTE && docker build -q -t airbreda-dashboard -f Dockerfile.dashboard ."
  "${SSH[@]}" "docker image prune -f >/dev/null"   # dangling layers would fill the 8 GB disk
fi

log "Applying database migrations"
"${SSH[@]}" "cd $REMOTE && docker run --rm --env-file .env airbreda-air python migrate.py"

log "Installing cron schedule"
"${SSH[@]}" "crontab $REMOTE/infra/crontab.txt && crontab -l"

log "Restarting dashboard"
# --restart unless-stopped + Docker enabled in systemd (user-data) = the dashboard survives a VM reboot (ADR-005).
"${SSH[@]}" "cd $REMOTE && (docker rm -f dashboard >/dev/null 2>&1 || true) && docker run -d --name dashboard --restart unless-stopped --env-file .env -p 8000:8000 airbreda-dashboard >/dev/null && sleep 8 && docker ps --format '{{.Names}}\t{{.Status}}'"
log "Smoke test"
for path in /health /site/hrl; do
  printf '%s -> ' "$path"; curl -s -o /dev/null -w '%{http_code}\n' "http://$VM_IP:8000$path"
done
echo "Dashboard: http://$VM_IP:8000"
