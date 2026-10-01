#!/usr/bin/env bash
# One command from training data to a live, verified dashboard with the document updated:
#   ./scripts/retrain_and_deploy.sh            # train on everything collected so far, deploy, verify, push
#   ./scripts/retrain_and_deploy.sh --no-push  # same, but leave the git push to you
#
# Steps: build_training_data (S3 + RDS) → train → commit model+data → deploy.sh → verify.sh →
# fill_metrics → commit docs → push. Stops at the first failure. Safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/.."
PUSH=1; [ "${1:-}" = "--no-push" ] && PUSH=0
PY=./venv/bin/python
set -a; . ./.env; set +a
log() { printf '\n==> %s\n' "$*"; }

log "1/6 Building training set from S3 + RDS"
$PY src/build_training_data.py --traffic-source s3 | tail -3
ROWS=$(($(wc -l < data/training_data.csv) - 1))
echo "training rows: $ROWS"

log "2/6 Training"
$PY src/train.py | tail -4

log "3/6 Tests (incl. the shipped-model test)"
set +a; $PY -m pytest -q 2>&1 | tail -1

log "4/6 Committing model + training data"
git add model/model.pkl model/metrics.json data/training_data.csv
git commit -q -m "Model trained on $ROWS hourly rows ($(date -u +%Y-%m-%d\ %H:%M) UTC)" || echo "(nothing new to commit)"

log "5/6 Deploying to the VM"
./infra/deploy.sh 2>&1 | grep -v "post-quantum\|store now, decrypt\|openssh.com/pq"
./scripts/verify.sh

log "6/6 Metrics into the document"
if grep -q METRICS_ROWS docs/index.md; then $PY scripts/fill_metrics.py; else $PY scripts/fill_metrics.py --update; fi
git add docs/index.md
git commit -q -m "ADR-006: metrics of the deployed model ($ROWS rows)" || echo "(docs unchanged)"
if [ "$PUSH" = 1 ]; then git push -q origin main && echo "pushed — GitHub Pages rebuilds in ~1 min"; fi

echo
echo "DONE. Live: http://$(. infra/resources.env; echo "$VM_IP"):8000   Docs: https://maksymsteshkin242689.github.io/airbreda/"
