#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from config import DB_CONFIG


# Static Food Prices Pipeline
#
# This script reads the WFP master CSV file, filters it to the
# selected countries and commodities, aggregates prices by month,
# pivots the data into a wide format, and loads the result into
# the PostgreSQL database.

BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "wfp_food_prices_master_2023_2025.csv"
OUTPUT_FILE = BASE_DIR / "food_prices.csv"


def run():
    print("Starting Food Prices Pipeline...")

    # 1. EXTRACT: Load the static CSV
    try:
        df = pd.read_csv(INPUT_FILE)
        print(f"Successfully loaded the master WFP file: {INPUT_FILE.name}")
    except Exception as e:
        print(f"Error reading the master CSV file: {e}")
        raise

    # 2. TRANSFORM: Filter and clean
    target_crops = ["Rice", "Maize", "Wheat flour"]
    target_countries = ["IND", "PHL", "NGA"]

    df_filtered = df[
        (df["commodity"].isin(target_crops)) &
        (df["pricetype"] == "Retail") &
        (df["countryiso3"].isin(target_countries))
    ].copy()

    # Standardize join keys
    df_filtered["Year_Month"] = pd.to_datetime(df_filtered["date"]).dt.strftime("%Y-%m")
    df_filtered.rename(columns={"countryiso3": "Country_Code"}, inplace=True)

    # Aggregate monthly average prices by commodity
    df_grouped = (
        df_filtered
        .groupby(["Country_Code", "Year_Month", "commodity"], as_index=False)["usdprice"]
        .mean()
    )

    # Pivot to wide format
    df_clean = (
        df_grouped
        .pivot(index=["Country_Code", "Year_Month"], columns="commodity", values="usdprice")
        .reset_index()
    )

    df_clean.columns.name = None

    # Rename columns to consistent metric names
    df_clean.rename(
        columns={
            "Maize": "Maize_Price_USD_per_KG",
            "Rice": "Rice_Price_USD_per_KG",
            "Wheat flour": "Wheat_Flour_Price_USD_per_KG",
        },
        inplace=True,
    )

    # Sort for clean reproducible output
    df_clean = df_clean.sort_values(["Country_Code", "Year_Month"]).reset_index(drop=True)

    # Save processed CSV for reproducibility
    df_clean.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved processed file: {OUTPUT_FILE.name}")

    # 3. LOAD: Push to PostgreSQL
    engine_url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    try:
        engine = create_engine(engine_url)
        df_clean.to_sql("food_prices", engine, if_exists="replace", index=False)
        print("Successfully loaded food prices into the database table 'food_prices'!")
    except Exception as e:
        print(f"Error loading to database: {e}")
        raise

    return df_clean


if __name__ == "__main__":
    run()