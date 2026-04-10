#!/bin/sh
set -e

echo "Waiting for PostgreSQL..."

until python3 -c "import psycopg2; psycopg2.connect(host='${DB_HOST}', port='${DB_PORT}', dbname='${DB_NAME}', user='${DB_USER}', password='${DB_PASSWORD}').close()" >/dev/null 2>&1
do
  sleep 2
done

echo "PostgreSQL is up."

echo "Running incremental source updates..."
PIPELINE_MODE=incremental python3 world_bank.py
PIPELINE_MODE=incremental python3 eia_energy.py
PIPELINE_MODE=incremental python3 weather_backfill.py
python3 food_download.py
python3 food_prices.py

echo "Incremental run complete."