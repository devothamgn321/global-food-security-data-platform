#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import requests
from pathlib import Path
from config import EIA_API_KEY


# EIA Energy Data Pipeline
#
# This script fetches energy production and consumption data from the U.S.
# Energy Information Administration (EIA) API. Since EIA only provides annual
# data for international countries, the values are distributed equally across
# 12 months to align with the monthly structure used in the rest of the project.

# Base directory for this script. Using this makes file paths portable,
# so the pipeline can run even if the folder is moved somewhere else.
BASE_DIR = Path(__file__).resolve().parent

# Our 5 target countries for the project
COUNTRIES = ["USA", "BRA", "IND", "PHL", "NGA"]

# Date range to match our other datasets
START_YEAR = 2022
END_YEAR = 2024

# Energy metrics from EIA
# Each has a product_id and activity_id that EIA uses to identify the data
PRODUCTS = [
    {"product_id": "79", "activity_id": "1", "name": "electricity_production"},
    {"product_id": "79", "activity_id": "2", "name": "electricity_consumption"},
    {"product_id": "12", "activity_id": "1", "name": "petroleum_production"},
    {"product_id": "12", "activity_id": "2", "name": "petroleum_consumption"},
    {"product_id": "26", "activity_id": "1", "name": "natural_gas_production"},
    {"product_id": "26", "activity_id": "2", "name": "natural_gas_consumption"},
]


def fetch_eia_annual(country_code, product_id, activity_id):
    """
    Calls the EIA API to get annual energy data for a specific country and metric.
    Returns a dataframe with the results, or an empty dataframe if the request fails.
    """
    url = "https://api.eia.gov/v2/international/data/"

    params = {
        "api_key": EIA_API_KEY,
        "frequency": "annual",
        "data[0]": "value",
        "facets[countryRegionId][]": country_code,
        "facets[productId][]": product_id,
        "facets[activityId][]": activity_id,
        "start": str(START_YEAR),
        "end": str(END_YEAR),
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


def interpolate_to_monthly(df):
    """
    Takes annual data and distributes equally across 12 months.
    """
    rows = []

    for _, row in df.iterrows():
        year = int(row["period"])

        try:
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


def run():
    """
    Main function that loops through all countries and metrics,
    fetches the data, interpolates to monthly, and saves to CSV.
    """
    if not EIA_API_KEY:
        raise ValueError(
            "Missing EIA_API_KEY. Add it to your .env file before running the pipeline."
        )

    print("Fetching EIA energy data...")
    all_data = []

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
        return

    energy_df = pd.concat(all_data, ignore_index=True)

    energy_df["Year_Month"] = (
        energy_df["year"].astype(str) + "-" +
        energy_df["month"].astype(str).str.zfill(2)
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


if __name__ == "__main__":
    run()