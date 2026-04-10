#!/usr/bin/env python
# coding: utf-8

import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import requests

from config import EIA_API_KEY


BASE_DIR = Path(__file__).resolve().parent

COUNTRIES = ["USA", "BRA", "IND", "PHL", "NGA"]

PRODUCTS = [
    {"product_id": "79", "activity_id": "1", "name": "electricity_production"},
    {"product_id": "79", "activity_id": "2", "name": "electricity_consumption"},
    {"product_id": "12", "activity_id": "1", "name": "petroleum_production"},
    {"product_id": "12", "activity_id": "2", "name": "petroleum_consumption"},
    {"product_id": "26", "activity_id": "1", "name": "natural_gas_production"},
    {"product_id": "26", "activity_id": "2", "name": "natural_gas_consumption"},
]


def get_pipeline_mode() -> str:
    return os.getenv("PIPELINE_MODE", "backfill").strip().lower()


def world_safe_latest_year() -> int:
    """
    Conservative latest year for annual EIA international data.
    Uses prior year to avoid assuming the current year is fully available.
    """
    return datetime.now().year - 1


def get_year_range() -> Tuple[int, int]:
    """
    backfill    -> 2022 to latest safely available full year
    incremental -> latest safely available full year only
    """
    mode = get_pipeline_mode()
    latest_year = world_safe_latest_year()

    if mode == "incremental":
        return latest_year, latest_year

    return 2022, latest_year


def fetch_eia_annual(country_code: str, product_id: str, activity_id: str) -> pd.DataFrame:
    url = "https://api.eia.gov/v2/international/data/"
    start_year, end_year = get_year_range()

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "annual",
        "data[0]": "value",
        "facets[countryRegionId][]": country_code,
        "facets[productId][]": product_id,
        "facets[activityId][]": activity_id,
        "start": str(start_year),
        "end": str(end_year),
        "length": 5000,
    }

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        records = data.get("response", {}).get("data", [])

        if records:
            return pd.DataFrame(records)

    except Exception as e:
        print(f"Error fetching {country_code} ({product_id}, {activity_id}): {e}")

    return pd.DataFrame()


def interpolate_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for _, row in df.iterrows():
        try:
            year = int(row["period"])
            annual_value = float(row.get("value"))
        except (ValueError, TypeError):
            continue

        if pd.isna(annual_value):
            continue

        monthly_value = annual_value / 12

        for month in range(1, 13):
            rows.append(
                {
                    "year": year,
                    "month": month,
                    "value": round(monthly_value, 4),
                    "unit": row.get("unit", ""),
                }
            )

    return pd.DataFrame(rows)


def run() -> Optional[pd.DataFrame]:
    if not EIA_API_KEY:
        raise ValueError(
            "Missing EIA_API_KEY. Add it to your .env file before running the pipeline."
        )

    start_year, end_year = get_year_range()
    mode = get_pipeline_mode()

    print(f"Fetching EIA energy data... mode={mode}, years={start_year}-{end_year}")
    all_data: List[pd.DataFrame] = []

    for country_code in COUNTRIES:
        for product in PRODUCTS:
            df_annual = fetch_eia_annual(
                country_code,
                product["product_id"],
                product["activity_id"],
            )

            if df_annual.empty:
                continue

            df_monthly = interpolate_to_monthly(df_annual)

            if df_monthly.empty:
                continue

            df_monthly["Country_Code"] = country_code
            df_monthly["metric"] = product["name"]
            all_data.append(df_monthly)

    if not all_data:
        print("No data retrieved.")
        return None

    energy_df = pd.concat(all_data, ignore_index=True)

    energy_df["Year_Month"] = (
        energy_df["year"].astype(str) + "-" + energy_df["month"].astype(str).str.zfill(2)
    )

    energy_pivot = energy_df.pivot_table(
        index=["Country_Code", "Year_Month"],
        columns="metric",
        values="value",
        aggfunc="first",
    ).reset_index()

    energy_pivot.columns.name = None

    output_path = BASE_DIR / "energy.csv"
    energy_pivot.to_csv(output_path, index=False)
    print(f"Saved: {output_path.name} {energy_pivot.shape}")

    return energy_pivot


if __name__ == "__main__":
    run()