# Team 9 Data Engineering Pipeline

## Overview
This project implements an automated ETL pipeline that integrates multiple heterogeneous data sources into a centralized PostgreSQL database. The pipeline combines World Bank economic indicators, EIA energy data, a static food prices dataset, and weather data into a unified analytical structure. The project also includes a Flask API to expose the processed data through queryable endpoints.

The automation layer is designed so that the full pipeline can be executed through a single command rather than requiring manual notebook execution.

---

## Project Structure

- `master.py` – Main automation script that runs the full ETL pipeline
- `world_bank.py` – World Bank API ingestion, transformation, and loading
- `food_prices.py` – Static food prices CSV ingestion, transformation, and loading
- `eia_energy.py` – EIA API ingestion, transformation, and loading
- `weather_backfill.py` – Historical weather ingestion script for 2022–2025
- `weather_update.py` – Script for recurring weather updates
- `config.py` – Shared environment-based configuration
- `app.py` – Flask API for querying the database
- `requirements.txt` – Python dependencies
- `.env.example` – Template for required environment variables
- `wfp_food_prices_master_2023_2025.csv` – Static WFP food prices source file

---

## Data Sources

This project uses at least three datasets from at least three different sources:

1. **World Bank API**  
   Source of macroeconomic indicators such as GDP, inflation, and population.

2. **EIA API**  
   Source of international energy production and consumption data.

3. **World Food Programme static dataset**  
   Static CSV dataset containing food prices.

4. **Weather API**  
   Historical and updateable weather data used for environmental context.

The datasets are joined using shared keys:
- `country_code`
- `year_month`


## Static Source File

The food prices dataset is included in the project as:

`wfp_food_prices_master_2023_2025.csv`

No additional download is required. The automated pipeline reads this file directly during execution.
---

## Environment Configuration

The project uses environment variables to store database credentials and API keys securely.

### Create the environment file
```bash
cp .env.example .env