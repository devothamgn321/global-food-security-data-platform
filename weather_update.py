#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import requests
from sqlalchemy import create_engine, text

from config import DB_CONFIG, WEATHER_BASE_URL


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = BASE_DIR / "weather_update.csv"

COUNTRIES = {
    "USA": {"lat": 39.2904, "lon": -76.6122},
    "BRA": {"lat": -23.5505, "lon": -46.6333},
    "IND": {"lat": 22.5726, "lon": 88.3639},
    "PHL": {"lat": 14.5995, "lon": 120.9842},
    "NGA": {"lat": 6.5244, "lon": 3.3792},
}


def get_update_window(days_back: int = 90) -> tuple[str, str]:
    end_date = datetime.today().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)
    return start_date.isoformat(), end_date.isoformat()


def fetch_weather_data(country_code: str, lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    if not WEATHER_BASE_URL:
        raise ValueError("Missing WEATHER_BASE_URL in .env")

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_mean,precipitation_sum",
        "timezone": "auto",
    }

    response = requests.get(WEATHER_BASE_URL, params=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    daily = data.get("daily", {})
    if not daily:
        return pd.DataFrame()

    df = pd.DataFrame({
        "date": daily.get("time", []),
        "temperature_mean": daily.get("temperature_2m_mean", []),
        "precipitation_sum": daily.get("precipitation_sum", []),
    })

    if df.empty:
        return df

    df["Country_Code"] = country_code
    return df


def transform_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"])
    df["Year_Month"] = df["date"].dt.strftime("%Y-%m")

    monthly_df = (
        df.groupby(["Country_Code", "Year_Month"], as_index=False)
        .agg({
            "temperature_mean": "mean",
            "precipitation_sum": "sum",
        })
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


def upsert_to_database(df: pd.DataFrame, table_name: str = "weather") -> None:
    engine_url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    engine = create_engine(engine_url)

    temp_table = f"{table_name}_staging"

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        df.to_sql(temp_table, conn, if_exists="replace", index=False)

        conn.execute(text(f"""
            INSERT INTO {table_name} ("Country_Code", "Year_Month", "Avg_Temperature", "Total_Precipitation")
            SELECT "Country_Code", "Year_Month", "Avg_Temperature", "Total_Precipitation"
            FROM {temp_table}
            ON CONFLICT ("Country_Code", "Year_Month")
            DO UPDATE SET
                "Avg_Temperature" = EXCLUDED."Avg_Temperature",
                "Total_Precipitation" = EXCLUDED."Total_Precipitation"
        """))

        conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

    print(f"Successfully upserted weather updates into '{table_name}'")


def run() -> pd.DataFrame:
    print("Starting weather update pipeline...")

    start_date, end_date = get_update_window()
    print(f"Update window: {start_date} to {end_date}")

    all_data = []

    for country_code, coords in COUNTRIES.items():
        print(f"Fetching updated weather for {country_code}...")
        df = fetch_weather_data(country_code, coords["lat"], coords["lon"], start_date, end_date)

        if not df.empty:
            all_data.append(df)

    if not all_data:
        raise RuntimeError("No weather data fetched during update.")

    daily_df = pd.concat(all_data, ignore_index=True)
    monthly_df = transform_to_monthly(daily_df)

    monthly_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved: {OUTPUT_FILE.name} {monthly_df.shape}")

    upsert_to_database(monthly_df, table_name="weather")

    return monthly_df


if __name__ == "__main__":
    run()