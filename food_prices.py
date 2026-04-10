#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine

from config import DB_CONFIG


BASE_DIR = Path(__file__).resolve().parent
INPUT_FILE = BASE_DIR / "wfp_food_prices_master_2023_2025.csv"
LATEST_FILE = BASE_DIR / "latest_food_prices.csv"
OUTPUT_FILE = BASE_DIR / "food_prices.csv"


def get_engine():
    engine_url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(engine_url)


def run():
    print("Starting Food Prices Pipeline...")

    try:
        df_base = pd.read_csv(INPUT_FILE)
        print(f"Successfully loaded the master WFP file: {INPUT_FILE.name}")
    except Exception as e:
        print(f"Error reading the master CSV file: {e}")
        raise

    if LATEST_FILE.exists():
        try:
            df_latest = pd.read_csv(LATEST_FILE)
            print(f"Successfully loaded latest downloaded food file: {LATEST_FILE.name}")
            df = pd.concat([df_base, df_latest], ignore_index=True).drop_duplicates()
            print("Merged historical and latest food data.")
        except Exception as e:
            print(f"Error reading latest downloaded food file: {e}")
            raise
    else:
        df = df_base
        print("No latest_food_prices.csv found. Using base file only.")

    target_crops = ["Rice", "Maize", "Wheat flour"]
    target_countries = ["IND", "PHL", "NGA"]

    df_filtered = df[
        (df["commodity"].isin(target_crops))
        & (df["pricetype"] == "Retail")
        & (df["countryiso3"].isin(target_countries))
    ].copy()

    df_filtered["year_month"] = pd.to_datetime(df_filtered["date"]).dt.strftime("%Y-%m")
    df_filtered.rename(columns={"countryiso3": "country_code"}, inplace=True)

    df_grouped = (
        df_filtered.groupby(["country_code", "year_month", "commodity"], as_index=False)["usdprice"]
        .mean()
    )

    df_clean = (
        df_grouped.pivot(
            index=["country_code", "year_month"],
            columns="commodity",
            values="usdprice",
        )
        .reset_index()
    )

    df_clean.columns.name = None

    df_clean.rename(
        columns={
            "Maize": "maize_price_usd_per_kg",
            "Rice": "rice_price_usd_per_kg",
            "Wheat flour": "wheat_flour_price_usd_per_kg",
        },
        inplace=True,
    )

    df_clean = df_clean.sort_values(["country_code", "year_month"]).reset_index(drop=True)

    df_clean.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved processed file: {OUTPUT_FILE.name}")

    try:
        engine = get_engine()
        df_clean.to_sql("food_prices", engine, if_exists="replace", index=False)
        print("Successfully loaded food prices into the database table 'food_prices'!")
    except Exception as e:
        print(f"Error loading to database: {e}")
        raise

    return df_clean


if __name__ == "__main__":
    run()