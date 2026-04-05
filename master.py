#!/usr/bin/env python
# coding: utf-8

from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import world_bank
import food_prices
import eia_energy
# import weather_backfill  # Uncomment when weather script is finalized

from config import DB_CONFIG


# Master Pipeline Script
#
# This script orchestrates the entire data pipeline:
# 1. Runs each individual pipeline to generate CSV files
# 2. Creates a countries lookup table
# 3. Loads each CSV into its own PostgreSQL table
# 4. Creates a master joined table
# 5. Adds primary keys and foreign keys for proper database design

BASE_DIR = Path(__file__).resolve().parent

COUNTRIES = {
    "USA": "United States",
    "BRA": "Brazil",
    "IND": "India",
    "PHL": "Philippines",
    "NGA": "Nigeria",
}


def run_pipelines() -> None:
    """
    Run each individual pipeline to generate the CSV files.
    """
    print("Running individual pipelines...")
    print("-" * 40)

    world_bank.run()
    print("-" * 40)

    food_prices.run()
    print("-" * 40)

    eia_energy.run()
    print("-" * 40)

    # weather_backfill.run()  # Uncomment when ready
    # print("-" * 40)

    print("All pipelines complete.")


def get_database_engine():
    """
    Create and return a SQLAlchemy engine for PostgreSQL.
    """
    connection_string = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )
    return create_engine(connection_string)


def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all column names to lowercase for easier PostgreSQL compatibility.
    """
    df.columns = [col.lower() for col in df.columns]
    return df


def create_countries_table(engine) -> pd.DataFrame:
    """
    Create the countries lookup table.
    """
    df_countries = pd.DataFrame(
        [{"country_code": code, "country_name": name} for code, name in COUNTRIES.items()]
    )

    df_countries.to_sql("countries", con=engine, if_exists="replace", index=False)
    print(f"Created table 'countries': {df_countries.shape}")

    return df_countries


def load_individual_tables(engine) -> dict:
    """
    Load each CSV file to its own table in PostgreSQL.
    Returns dataframes for joining later.
    """
    dataframes = {}

    # World Bank
    world_bank_path = BASE_DIR / "world_bank_monthly.csv"
    df_worldbank = pd.read_csv(world_bank_path)
    df_worldbank = standardize_columns(df_worldbank)
    df_worldbank.to_sql("worldbank", con=engine, if_exists="replace", index=False)
    print(f"Created table 'worldbank': {df_worldbank.shape}")
    dataframes["worldbank"] = df_worldbank

    # Food Prices
    food_prices_path = BASE_DIR / "food_prices.csv"
    df_food_prices = pd.read_csv(food_prices_path)
    df_food_prices = standardize_columns(df_food_prices)
    df_food_prices.to_sql("food_prices", con=engine, if_exists="replace", index=False)
    print(f"Created table 'food_prices': {df_food_prices.shape}")
    dataframes["food_prices"] = df_food_prices

    # Energy
    energy_path = BASE_DIR / "energy.csv"
    df_energy = pd.read_csv(energy_path)
    df_energy = standardize_columns(df_energy)
    df_energy.to_sql("energy", con=engine, if_exists="replace", index=False)
    print(f"Created table 'energy': {df_energy.shape}")
    dataframes["energy"] = df_energy

    # Weather (optional)
    weather_path = BASE_DIR / "weather.csv"
    if weather_path.exists():
        df_weather = pd.read_csv(weather_path)
        df_weather = standardize_columns(df_weather)
        df_weather.to_sql("weather", con=engine, if_exists="replace", index=False)
        print(f"Created table 'weather': {df_weather.shape}")
        dataframes["weather"] = df_weather
    else:
        print("weather.csv not found. Skipping weather table for now.")

    return dataframes


def create_master_table(engine, dataframes: dict) -> pd.DataFrame:
    """
    Join all dataframes and create the master_data table.
    Uses outer joins to keep all data even if some datasets have gaps.
    """
    df_master = dataframes["worldbank"]

    # Join food prices
    df_master = pd.merge(
        df_master,
        dataframes["food_prices"],
        on=["country_code", "year_month"],
        how="outer",
    )

    # Join energy
    df_master = pd.merge(
        df_master,
        dataframes["energy"],
        on=["country_code", "year_month"],
        how="outer",
    )

    # Join weather if available
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


def drop_existing_tables(engine) -> None:
    """
    Drop existing tables before rebuilding the pipeline.
    """
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS master_data CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS worldbank CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS food_prices CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS energy CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS weather CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS countries CASCADE"))
        print("Dropped existing tables if they existed.")


def add_key_constraints(engine, include_weather: bool = False) -> None:
    """
    Add primary keys and foreign keys to tables.
    """
    with engine.begin() as conn:
        # Countries
        conn.execute(text("""
            ALTER TABLE countries
            ADD PRIMARY KEY (country_code)
        """))
        print("Added primary key to 'countries'")

        # World Bank
        conn.execute(text("""
            ALTER TABLE worldbank
            ADD PRIMARY KEY (country_code, year_month)
        """))
        conn.execute(text("""
            ALTER TABLE worldbank
            ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)
        """))
        print("Added primary key and foreign key to 'worldbank'")

        # Food Prices
        conn.execute(text("""
            ALTER TABLE food_prices
            ADD PRIMARY KEY (country_code, year_month)
        """))
        conn.execute(text("""
            ALTER TABLE food_prices
            ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)
        """))
        print("Added primary key and foreign key to 'food_prices'")

        # Energy
        conn.execute(text("""
            ALTER TABLE energy
            ADD PRIMARY KEY (country_code, year_month)
        """))
        conn.execute(text("""
            ALTER TABLE energy
            ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)
        """))
        print("Added primary key and foreign key to 'energy'")

        # Weather
        if include_weather:
            conn.execute(text("""
                ALTER TABLE weather
                ADD PRIMARY KEY (country_code, year_month)
            """))
            conn.execute(text("""
                ALTER TABLE weather
                ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)
            """))
            print("Added primary key and foreign key to 'weather'")

        # Master Data
        conn.execute(text("""
            ALTER TABLE master_data
            ADD PRIMARY KEY (country_code, year_month)
        """))
        conn.execute(text("""
            ALTER TABLE master_data
            ADD FOREIGN KEY (country_code) REFERENCES countries(country_code)
        """))
        print("Added primary key and foreign key to 'master_data'")

        print("-" * 40)
        print("All primary and foreign keys added successfully.")


def run() -> None:
    """
    Main function that orchestrates the entire pipeline.
    """
    print("=" * 50)
    print("MASTER PIPELINE")
    print("=" * 50)

    # Step 1: Run all individual pipelines to create CSVs
    run_pipelines()

    print("\nLoading to PostgreSQL...")
    print("-" * 40)

    try:
        engine = get_database_engine()

        # Step 2: Drop old tables
        drop_existing_tables(engine)

        # Step 3: Create countries lookup table
        create_countries_table(engine)

        # Step 4: Load CSVs to individual tables
        dataframes = load_individual_tables(engine)

        # Step 5: Create joined master table
        create_master_table(engine, dataframes)

        # Step 6: Add keys
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