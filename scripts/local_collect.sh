#!/usr/bin/env bash
# Development helper: run the two ingestion scripts on a schedule from a laptop, against whatever
# database .env.local points at. Used on Day 1-3 to start accumulating training data before the
# VM was ready; on the VM the same two commands run from cron instead (see infra/crontab.txt).
#
#   ./scripts/local_collect.sh            # foreground
#   nohup ./scripts/local_collect.sh > /tmp/airbreda-collect.log 2>&1 &
set -uo pipefail
cd "$(dirname "$0")/.."
set -a; . ./.env.local; set +a

TRAFFIC_EVERY_S=300
AIR_EVERY_S=3600
last_air=0

while true; do
  now=$(date +%s)
  ./venv/bin/python src/ingest_traffic.py
  if (( now - last_air >= AIR_EVERY_S )); then
    ./venv/bin/python src/ingest_air.py && last_air=$now
  fi
  sleep "$TRAFFIC_EVERY_S"
done
