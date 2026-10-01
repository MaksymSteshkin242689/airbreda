# airbreda-air — Luchtmeetnet NO₂ ingestion. One run = one fetch; scheduled by cron on the VM.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

COPY requirements-ingest.txt .
RUN pip install --no-cache-dir -r requirements-ingest.txt

COPY src/common.py src/schema.sql src/ingest_air.py ./

# Run unprivileged: the container only needs outbound HTTPS and a database connection.
RUN useradd --create-home --uid 1000 app
USER app

CMD ["python", "ingest_air.py"]
