#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import food_download
import world_bank
import food_prices
import eia_energy
import weather_backfill

from config import DB_CONFIG


BASE_DIR = Path(__file__).resolve().parent

COUNTRIES = {
    "USA": "United States",
    "BRA": "Brazil",
    "IND": "India",
    "PHL": "Philippines",
    "NGA": "Nigeria",
}


def get_database_engine():
    connection_string = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def run_pipelines() -> None:
    print("Running individual pipelines...")
    print("-" * 40)

    print("Starting food download...")
    food_download.run()
    print("-" * 40)

    print("Starting World Bank pipeline...")
    # Let master own the final database rebuild.
    original_flag = world_bank.os.getenv("WORLD_BANK_LOAD_TO_DB")
    world_bank.os.environ["WORLD_BANK_LOAD_TO_DB"] = "false"
    world_bank.run()
    if original_flag is None:
        del world_bank.os.environ["WORLD_BANK_LOAD_TO_DB"]
    else:
        world_bank.os.environ["WORLD_BANK_LOAD_TO_DB"] = original_flag
    print("-" * 40)

    print("Starting food prices ingestion...")
    food_prices.run()
    print("-" * 40)

    print("Starting energy pipeline...")
    eia_energy.run()
    print("-" * 40)

    print("Starting weather backfill pipeline...")
    weather_backfill.run()
    print("-" * 40)

    print("All pipelines complete.")


def drop_existing_tables(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS master_data CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS worldbank CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS food_prices CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS energy CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS weather CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS countries CASCADE"))
        print("Dropped existing tables if they existed.")


def create_countries_table(engine) -> pd.DataFrame:
    df_countries = pd.DataFrame(
        [{"country_code": code, "country_name": name} for code, name in COUNTRIES.items()]
    )
    df_countries.to_sql("countries", con=engine, if_exists="replace", index=False)
    print(f"Created table 'countries': {df_countries.shape}")
    return df_countries


def load_csv_table(engine, file_path: Path, table_name: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)
    df = standardize_columns(df)
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"Created table '{table_name}': {df.shape}")
    return df


def load_individual_tables(engine) -> dict:
    dataframes = {}

    dataframes["worldbank"] = load_csv_table(
        engine,
        BASE_DIR / "world_bank_monthly.csv",
        "worldbank",
    )

    dataframes["food_prices"] = load_csv_table(
        engine,
        BASE_DIR / "food_prices.csv",
        "food_prices",
    )

    dataframes["energy"] = load_csv_table(
        engine,
        BASE_DIR / "energy.csv",
        "energy",
    )

    weather_path = BASE_DIR / "weather.csv"
    if weather_path.exists():
        dataframes["weather"] = load_csv_table(engine, weather_path, "weather")
    else:
        print("weather.csv not found. Skipping weather table.")

    return dataframes


def create_master_table(engine, dataframes: dict) -> pd.DataFrame:
    df_master = dataframes["worldbank"].copy()

    if "food_prices" in dataframes:
        df_master = pd.merge(
            df_master,
            dataframes["food_prices"],
            on=["country_code", "year_month"],
            how="outer",
        )

    if "energy" in dataframes:
        df_master = pd.merge(
            df_master,
            dataframes["energy"],
            on=["country_code", "year_month"],
            how="outer",
        )

    if "weather" in dataframes:
        df_master = pd.merge(
            df_master,
            dataframes["weather"],
            on=["country_code", "year_month"],
            how="outer",
        )

    df_master = df_master.sort_values(["country_code", "year_month"]).reset_index(drop=True)
    df_master.to_sql("master_data", con=engine, if_exists="replace", index=False)
    print(f"Created table 'master_data': {df_master.shape}")

    return df_master


def add_key_constraints(engine, include_weather: bool = False) -> None:
    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE countries ADD PRIMARY KEY (country_code)"))
        print("Added primary key to 'countries'")

        conn.execute(text("ALTER TABLE worldbank ADD PRIMARY KEY (country_code, year_month)"))
        conn.execute(text("ALTER TABLE worldbank ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)"))
        print("Added primary key and foreign key to 'worldbank'")

        conn.execute(text("ALTER TABLE food_prices ADD PRIMARY KEY (country_code, year_month)"))
        conn.execute(text("ALTER TABLE food_prices ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)"))
        print("Added primary key and foreign key to 'food_prices'")

        conn.execute(text("ALTER TABLE energy ADD PRIMARY KEY (country_code, year_month)"))
        conn.execute(text("ALTER TABLE energy ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)"))
        print("Added primary key and foreign key to 'energy'")

        if include_weather:
            conn.execute(text("ALTER TABLE weather ADD PRIMARY KEY (country_code, year_month)"))
            conn.execute(text("ALTER TABLE weather ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)"))
            print("Added primary key and foreign key to 'weather'")

        conn.execute(text("ALTER TABLE master_data ADD PRIMARY KEY (country_code, year_month)"))
        conn.execute(text("ALTER TABLE master_data ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)"))
        print("Added primary key and foreign key to 'master_data'")

        print("-" * 40)
        print("All primary and foreign keys added successfully.")


def run() -> None:
    print("=" * 50)
    print("MASTER PIPELINE")
    print("=" * 50)

    run_pipelines()

    print("\nLoading to PostgreSQL...")
    print("-" * 40)

    try:
        engine = get_database_engine()

        drop_existing_tables(engine)
        create_countries_table(engine)
        dataframes = load_individual_tables(engine)
        create_master_table(engine, dataframes)

        print("\nAdding database primary and foreign keys...")
        print("-" * 40)
        add_key_constraints(engine, include_weather=("weather" in dataframes))

        print("-" * 40)
        print("All tables created successfully.")

    except Exception as e:
        print(f"Database error: {e}")
        print("Make sure PostgreSQL is running and your .env settings are correct.")
        raise

    print("=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)


if __name__ == "__main__":
    run()