# airbreda-air — Luchtmeetnet NO₂ ingestion. One run = one fetch; scheduled by cron on the VM.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt

COPY src/settings.py src/observability.py src/db.py src/migrate.py src/sites.py src/ingest_air.py ./
COPY db/migrations db/migrations

# Run unprivileged: the container only needs outbound HTTPS and a database connection.
RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "ingest_air.py"]
