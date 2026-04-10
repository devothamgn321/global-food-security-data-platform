#!/bin/sh
set -e

echo "Waiting for PostgreSQL..."

until python3 -c "import psycopg2; psycopg2.connect(host='${DB_HOST}', port='${DB_PORT}', dbname='${DB_NAME}', user='${DB_USER}', password='${DB_PASSWORD}').close()" >/dev/null 2>&1
do
  sleep 2
done

echo "PostgreSQL is up."

echo "Running backfill pipeline..."
PIPELINE_MODE=backfill python3 master.py

echo "Starting cron..."
crontab /app/cronjob
cron

echo "Starting Flask API..."
python3 app.py