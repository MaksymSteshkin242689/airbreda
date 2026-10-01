# AirBreda

Does traffic congestion at the A27 interchange near Breda cause NO₂ exceedances nearby?
AirBreda is a small cloud data platform that ingests two live Dutch open-data streams, stores
them, trains a regression model on its own accumulated readings, and serves a dashboard + JSON API.

- **Architecture Design Document (ADRs):** [`docs/`](docs/) — published on GitHub Pages.
- **Live system:** `http://<vm-ip>:8000` — `GET /`, `GET /site/{hrl|hrr|vwd|vwa}`, `GET /health`.

## Data sources

| Source | What | Cadence |
|---|---|---|
| [RIVM Luchtmeetnet](https://api.luchtmeetnet.nl/open_api/stations/NL10240/measurements) station **NL10240** (Breda-Tilburgseweg) | NO₂, µg/m³ | hourly |
| [NDW open data](https://opendata.ndw.nu/) `snelheden_en_intensiteiten_meetgegevens.xml.gz` | vehicle intensity (veh/h) and speed (km/h) at four A27 sites: `hrl`, `hrr` (mainline), `vwd`, `vwa` (slip roads) | per minute |

## How it runs

```
Luchtmeetnet ──► airbreda-air     (cron, hourly)   ──► RDS Postgres: sensor_readings (+is_flagged)
NDW          ──► airbreda-traffic (cron, 5 min)    ──► S3: ndw/YYYY-MM-DD/HH-{site}.csv  (raw)
                                                    ──► RDS Postgres: traffic_readings     (clean)
RDS + S3     ──► build_training_data.py ──► train.py ──► model/model.pkl (baked into the dashboard image)
                 airbreda-dashboard (FastAPI :8000) ──► /site/{id}  /health  /
```

All three containers run on one EC2 `t3.micro` in `eu-north-1`; the VM reaches S3 through an
IAM instance role scoped to its own bucket, and Postgres over TLS. See the ADRs for the why.

## Repository layout

```
src/            application code (one flat module per concern)
  ingest_air.py, ingest_traffic.py        the two ingestion jobs
  dashboard.py, templates/                FastAPI app + HTML page
  predict.py, train.py, build_training_data.py   model pipeline
  settings.py, observability.py, db.py, sites.py, migrate.py   shared plumbing
db/migrations/  versioned SQL migrations (yoyo-migrations), with rollbacks
tests/          pytest: unit tests + database integration tests
infra/          AWS CLI provisioning (provision.sh), deploy.sh, crontab, IAM policies, user-data
docs/           Architecture Design Document (GitHub Pages)
model/          trained model + metrics.json       data/  training_data.csv
Dockerfile, Dockerfile.traffic, Dockerfile.dashboard, docker-compose.yml
```

## Local development

```bash
python3.12 -m venv venv && ./venv/bin/pip install -r requirements.txt
docker run -d --name airbreda-pg -e POSTGRES_USER=airbreda -e POSTGRES_PASSWORD=localdev \
  -e POSTGRES_DB=airbreda -p 5440:5432 postgres:16-alpine
cp .env.example .env.local      # DB_HOST=localhost DB_PORT=5440 DB_SSLMODE=disable ...
set -a; . ./.env.local; set +a

./venv/bin/python src/migrate.py                 # apply migrations
./venv/bin/python src/ingest_air.py --backfill-hours 72
./venv/bin/python src/ingest_traffic.py          # add --skip-s3 without AWS credentials
./venv/bin/python -m pytest                      # 23 tests; DB tests run when DB_HOST is set

./venv/bin/python src/build_training_data.py     # → data/training_data.csv
./venv/bin/python src/train.py                   # → model/model.pkl, model/metrics.json
(cd src && ../venv/bin/uvicorn dashboard:app --reload)   # http://127.0.0.1:8000
```

`scripts/local_collect.sh` runs both ingestion jobs on a laptop on the same schedule as the VM's
cron; it was used to start accumulating training data on Day 1.

## Cloud deployment

```bash
./infra/provision.sh    # S3 bucket, security groups, RDS Postgres, key pair, IAM role, EC2 (idempotent)
./infra/deploy.sh       # rsync code, build images on the VM, migrate, install cron, start dashboard
```

Secrets live only in `.env` (git-ignored) and are shipped to the VM over scp. The VM has no AWS
keys: object storage access comes from the instance role. Terraform was deliberately not used for
this one-off deployment (ADR-004); `infra/provision.sh` is the reproducibility record instead.

## API

`GET /site/hrl`

```json
{
  "site_id": "hrl",
  "ndw_site_id": "RWS01_MONIBAS_0271hrl0063ra",
  "no2_ug_m3": 40.17,
  "no2_timestamp": "2026-10-01T19:00:00+00:00",
  "no2_is_flagged": false,
  "intensity_veh_per_hr": 1320,
  "speed_kmh": 110.6,
  "no2_ug_m3_predicted": 21.83,
  "no2_exceedance_risk": 0.026,
  "prediction_basis": {"total_intensity_veh_per_hr": 2160, "hour_of_day": 21, "sites_in_total": ["hrl","hrr","vwa","vwd"], "as_of": "2026-10-01T19:56:00+00:00"},
  "timestamp": "2026-10-01T19:56:00+00:00"
}
```

`GET /health` reports both ingestion sources side by side (`last_successful_fetch`,
`bad_data_count`, freshness) plus model status; `status` is `ok`, `degraded` (a source is stale)
or `error` (database unreachable, HTTP 503).

## Data quality rules

- **NDW `speed = -1`** (no vehicles / detector fault): logged as a structured `DATA_QUALITY_ERROR`,
  kept in the raw CSV in S3, **not** written to `traffic_readings`.
- **Luchtmeetnet null or frozen value** (identical for 3+ consecutive hours): logged, written to
  `sensor_readings` **with `is_flagged = TRUE`** — a visible gap beats a silent one.
- More than 10 bad readings from one source within an hour → one `BAD_DATA_THRESHOLD_EXCEEDED`
  ERROR log line (the alert hook).

All three services log one JSON object per line (`event`, `source`, `site_id`, …).
