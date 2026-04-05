#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
import requests
from sqlalchemy import create_engine

from config import DB_CONFIG, WEATHER_BASE_URL


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "weather.csv"

COUNTRIES = {
    "USA": {"lat": 39.2904, "lon": -76.6122},   # Baltimore
    "BRA": {"lat": -23.5505, "lon": -46.6333},  # Sao Paulo
    "IND": {"lat": 22.5726, "lon": 88.3639},    # Kolkata
    "PHL": {"lat": 14.5995, "lon": 120.9842},   # Manila
    "NGA": {"lat": 6.5244, "lon": 3.3792},      # Lagos
}

START_DATE = "2022-01-01"
END_DATE = "2025-12-31"


def fetch_weather_data(country_code: str, lat: float, lon: float) -> pd.DataFrame:
    """
    Fetch daily weather data for one country proxy location
    using the Open-Meteo historical archive API.
    """
    if not WEATHER_BASE_URL:
        raise ValueError("Missing WEATHER_BASE_URL in .env")

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "auto",
    }

    response = requests.get(WEATHER_BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    daily = data.get("daily", {})
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame(
        {
            "date": daily.get("time", []),
            "temperature_mean": daily.get("temperature_2m_mean", []),
            "precipitation_sum": daily.get("precipitation_sum", []),
        }
    )

    if df.empty:
        return df

    df["Country_Code"] = country_code
    return df


def transform_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert daily weather data into monthly aggregates.
    """
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["Year_Month"] = df["date"].dt.strftime("%Y-%m")

    monthly_df = (
        df.groupby(["Country_Code", "Year_Month"], as_index=False)
        .agg(
            {
                "temperature_mean": "mean",
                "precipitation_sum": "sum",
            }
        )
    )

    monthly_df.rename(
        columns={
            "temperature_mean": "Avg_Temperature",
            "precipitation_sum": "Total_Precipitation",
        },
        inplace=True,
    )

    monthly_df = monthly_df.sort_values(["Country_Code", "Year_Month"]).reset_index(drop=True)
    return monthly_df


def load_to_database(df: pd.DataFrame, table_name: str = "weather") -> None:
    engine_url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(engine_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Successfully loaded weather data into '{table_name}'")


def run() -> pd.DataFrame:
    print("Starting weather backfill pipeline...")

    all_data = []

    for country_code, coords in COUNTRIES.items():
        print(f"Fetching historical weather for {country_code}...")
        df = fetch_weather_data(country_code, coords["lat"], coords["lon"])

        if not df.empty:
            all_data.append(df)

    if not all_data:
        raise RuntimeError("No weather data fetched during backfill.")

    daily_df = pd.concat(all_data, ignore_index=True)
    monthly_df = transform_to_monthly(daily_df)

    monthly_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE.name} {monthly_df.shape}")

    load_to_database(monthly_df, table_name="weather")

    return monthly_df


if __name__ == "__main__":
    run()