#!/usr/bin/env python
# coding: utf-8

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
from sqlalchemy import create_engine

from config import DB_CONFIG


# -----------------------------
# Project configuration defaults
# -----------------------------

DEFAULT_COUNTRIES = ["USA", "BRA", "IND", "PHL", "NGA"]

DEFAULT_INDICATORS: Dict[str, str] = {
    "GDP": "NY.GDP.MKTP.CD",
    "Inflation": "FP.CPI.TOTL.ZG",
    "Population": "SP.POP.TOTL",
}

WB_ENDPOINT_TEMPLATE = (
    "https://api.worldbank.org/v2/country/{iso3}/indicator/{indicator}"
    "?format=json&date={start}:{end}&per_page=200"
)

YEAR_MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
BASE_DIR = Path(__file__).resolve().parent


# -----------------------------
# Helpers
# -----------------------------

def ensure_dirs(outdir: Path) -> Tuple[Path, Path]:
    raw_dir = outdir / "raw" / "world_bank"
    processed_dir = outdir / "processed"
    raw_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir, processed_dir


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def robust_get_json(url: str, timeout: int = 30, retries: int = 3, backoff_s: float = 1.5) -> List:
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff_s ** attempt)

    raise RuntimeError(
        f"Failed to fetch after {retries} attempts. Last error: {last_err}"
    ) from last_err


def cache_path(raw_dir: Path, iso3: str, indicator: str, start_year: int, end_year: int) -> Path:
    return raw_dir / f"{iso3}_{indicator}_{start_year}_{end_year}.json"


def load_cached_or_fetch(
    raw_dir: Path,
    iso3: str,
    indicator_code: str,
    start_year: int,
    end_year: int,
    use_cache_only: bool,
) -> List:
    cpath = cache_path(raw_dir, iso3, indicator_code, start_year, end_year)

    if cpath.exists():
        with cpath.open("r", encoding="utf-8") as f:
            return json.load(f)

    if use_cache_only:
        raise FileNotFoundError(f"Cache not found and use_cache_only=True: {cpath}")

    url = WB_ENDPOINT_TEMPLATE.format(
        iso3=iso3,
        indicator=indicator_code,
        start=start_year,
        end=end_year,
    )

    data = robust_get_json(url)

    with cpath.open("w", encoding="utf-8") as f:
        json.dump(data, f)

    return data


def expand_annual_to_monthly_rows(
    iso3: str,
    metric_name: str,
    annual_year: str,
    value: float,
    load_ts: str,
) -> List[Dict]:
    rows: List[Dict] = []

    for month in range(1, 13):
        rows.append(
            {
                "Country_Code": iso3,
                "Year_Month": f"{annual_year}-{month:02d}",
                "Metric": metric_name,
                "Value": value,
                "Source": "WorldBank",
                "Load_Timestamp": load_ts,
            }
        )

    return rows


def validate_contract(df: pd.DataFrame) -> None:
    required = ["Country_Code", "Year_Month"]

    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    bad_iso = df["Country_Code"].astype(str).str.len().ne(3)
    if bad_iso.any():
        examples = df.loc[bad_iso, "Country_Code"].head(5).tolist()
        raise ValueError(f"Invalid ISO3 codes found (len!=3). Examples: {examples}")

    bad_ym = ~df["Year_Month"].astype(str).str.match(YEAR_MONTH_RE)
    if bad_ym.any():
        examples = df.loc[bad_ym, "Year_Month"].head(5).tolist()
        raise ValueError(f"Invalid Year_Month format found (expected YYYY-MM). Examples: {examples}")


def log_counts(df: pd.DataFrame, label: str) -> None:
    print(f"\n[{label}] rows={len(df):,} cols={len(df.columns)}")

    if "Country_Code" in df.columns and "Year_Month" in df.columns:
        print("Countries:", sorted(df["Country_Code"].unique().tolist()))
        print("Year_Month range:", df["Year_Month"].min(), "to", df["Year_Month"].max())


# -----------------------------
# Main extraction logic
# -----------------------------

def build_world_bank_monthly(
    countries: List[str],
    indicators: Dict[str, str],
    start_year: int,
    end_year: int,
    outdir: Path,
    use_cache_only: bool,
) -> pd.DataFrame:
    raw_dir, processed_dir = ensure_dirs(outdir)
    load_ts = now_utc_iso()

    rows: List[Dict] = []

    for iso3 in countries:
        for metric_name, indicator_code in indicators.items():
            data = load_cached_or_fetch(
                raw_dir=raw_dir,
                iso3=iso3,
                indicator_code=indicator_code,
                start_year=start_year,
                end_year=end_year,
                use_cache_only=use_cache_only,
            )

            if not (isinstance(data, list) and len(data) > 1 and isinstance(data[1], list)):
                print(f"Warning: unexpected response format for {iso3} {indicator_code}. Skipping.")
                continue

            for entry in data[1]:
                year = entry.get("date")
                value = entry.get("value")

                if year is None or value is None:
                    continue

                rows.extend(
                    expand_annual_to_monthly_rows(
                        iso3=iso3,
                        metric_name=metric_name,
                        annual_year=year,
                        value=value,
                        load_ts=load_ts,
                    )
                )

    long_df = pd.DataFrame(rows)

    if long_df.empty:
        raise RuntimeError("No data extracted. Check connectivity, indicator IDs, and date range.")

    wide_df = (
        long_df.pivot_table(
            index=["Country_Code", "Year_Month"],
            columns="Metric",
            values="Value",
            aggfunc="first",
        )
        .reset_index()
    )

    wide_df.columns.name = None

    wide_df["Source"] = "WorldBank"
    wide_df["Load_Timestamp"] = load_ts

    ordered_cols = [
        "Country_Code",
        "Year_Month",
        "GDP",
        "Inflation",
        "Population",
        "Source",
        "Load_Timestamp",
    ]
    wide_df = wide_df[[c for c in ordered_cols if c in wide_df.columns]]

    wide_df = wide_df.sort_values(["Country_Code", "Year_Month"]).reset_index(drop=True)

    validate_contract(wide_df)

    assert (
        wide_df.duplicated(["Country_Code", "Year_Month"]).sum() == 0
    ), "Duplicate Country_Code + Year_Month rows found."

    full_path = processed_dir / "world_bank_monthly.csv"
    sample_path = processed_dir / "world_bank_sample10.csv"
    local_copy_path = BASE_DIR / "world_bank_monthly.csv"

    wide_df.to_csv(full_path, index=False)
    wide_df.head(10).to_csv(sample_path, index=False)
    wide_df.to_csv(local_copy_path, index=False)

    print(f"\nSaved: {full_path}")
    print(f"Saved: {sample_path}")
    print(f"Saved: {local_copy_path}")

    log_counts(wide_df, "WORLD BANK MONTHLY (processed)")

    return wide_df


def load_to_database(df: pd.DataFrame, table_name: str = "worldbank") -> None:
    engine_url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    engine = create_engine(engine_url)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Successfully loaded World Bank data into the database table '{table_name}'!")


def run() -> pd.DataFrame:
    print("Starting World Bank Pipeline...")

    df = build_world_bank_monthly(
        countries=DEFAULT_COUNTRIES,
        indicators=DEFAULT_INDICATORS,
        start_year=2022,
        end_year=2024,
        outdir=BASE_DIR / "data",
        use_cache_only=False,
    )

    load_to_database(df, table_name="worldbank")

    return df


if __name__ == "__main__":
    run()