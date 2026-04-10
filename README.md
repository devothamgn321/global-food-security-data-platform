# Team 9 Data Engineering Pipeline

## Overview
This project implements an end-to-end ETL pipeline that integrates multiple heterogeneous data sources into a centralized PostgreSQL warehouse. The pipeline combines World Bank economic indicators, EIA energy data, World Food Programme food prices, and weather data into a unified analytical dataset keyed by `country_code` and `year_month`.

The project includes:
- a full historical backfill pipeline
- source-level incremental update support
- a PostgreSQL warehouse
- a Flask API for querying the processed data
- Docker Compose for one-command startup

## What This Project Does
The pipeline collects and transforms data from multiple updateable and static sources:

- **World Bank API** for macroeconomic indicators
- **EIA API** for energy production and consumption data
- **WFP dataset** for food prices
- **Weather API** for climate-related variables

These sources are standardized and loaded into PostgreSQL tables, then merged into a final integrated analytical table called `master_data`.

## Key Features
- One-command Docker startup for peer review
- Full warehouse backfill from scratch through `master.py`
- Incremental refresh support for updateable sources
- Automated WFP food dataset download
- PostgreSQL schema with primary and foreign keys
- Flask API endpoints for data access
- Environment-based configuration through `.env`

## Project Structure
- `master.py` — main orchestration script for full backfill
- `world_bank.py` — World Bank ingestion and transformation
- `food_download.py` — downloads the latest WFP food prices file
- `food_prices.py` — merges historical and latest WFP food data
- `eia_energy.py` — EIA energy ingestion
- `weather_backfill.py` — weather ingestion
- `weather_update.py` — optional weather incremental updater
- `config.py` — shared configuration
- `app.py` — Flask API
- `start.sh` — Docker startup script for backfill and API launch
- `run_incremental.sh` — incremental refresh runner
- `Dockerfile` — app container definition
- `docker-compose.yml` — multi-container setup
- `requirements.txt` — Python dependencies
- `.env.example` — environment template
- `wfp_food_prices_master_2023_2025.csv` — included base food dataset

## Data Sources
1. World Bank API — macroeconomic indicators  
2. EIA API — energy data  
3. WFP Dataset via HDX — food prices  
4. Open-Meteo API — weather data  

## Warehouse Join Keys
- `country_code`
- `year_month`

## Execution Modes

### Backfill mode
Backfill mode performs a full historical warehouse build from scratch. This is the default startup path and is used for clean initialization.

### Incremental mode
Incremental mode supports scheduled or manual refreshes by pulling only the newest available source data window or latest available period, depending on the source.

## Requirements

### For Docker run
Install:
- Docker Desktop

### For local Python run
Install:
- Python 3.9 or newer
- PostgreSQL
- pip

## Environment Configuration

Create a `.env` file in the project root.

### Option 1: copy from template
```bash
cp .env.example .env