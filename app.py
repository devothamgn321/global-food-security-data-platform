#!/usr/bin/env python
# coding: utf-8

from flask import Flask, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

from config import DB_CONFIG


app = Flask(__name__)


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_all(query: str):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


@app.route("/")
def home():
    return jsonify(
        {
            "message": "Team 9 Data Pipeline API is running",
            "endpoints": [
                "/api/health",
                "/api/get_countries",
                "/api/get_world_bank",
                "/api/get_food_prices",
                "/api/get_energy",
                "/api/get_weather",
                "/api/get_all",
            ],
        }
    )


@app.route("/api/health", methods=["GET"])
def health():
    try:
        rows = fetch_all("SELECT 1 AS status;")
        return jsonify({"status": "ok", "database": "connected", "check": rows[0]["status"]})
    except Exception as e:
        return jsonify({"status": "error", "database": "disconnected", "message": str(e)}), 500


@app.route("/api/get_countries", methods=["GET"])
def get_countries():
    query = """
        SELECT country_code, country_name
        FROM countries
        ORDER BY country_code;
    """
    return jsonify(fetch_all(query))


@app.route("/api/get_world_bank", methods=["GET"])
def get_world_bank():
    query = """
        SELECT country_code, year_month, gdp, inflation, population, source, load_timestamp
        FROM worldbank
        ORDER BY country_code, year_month;
    """
    return jsonify(fetch_all(query))


@app.route("/api/get_food_prices", methods=["GET"])
def get_food_prices():
    query = """
        SELECT
            country_code,
            year_month,
            rice_price_usd_per_kg,
            wheat_flour_price_usd_per_kg
        FROM food_prices
        ORDER BY country_code, year_month;
    """
    return jsonify(fetch_all(query))


@app.route("/api/get_energy", methods=["GET"])
def get_energy():
    query = """
        SELECT
            country_code,
            year_month,
            electricity_consumption,
            electricity_production,
            natural_gas_consumption,
            natural_gas_production,
            petroleum_consumption,
            petroleum_production
        FROM energy
        ORDER BY country_code, year_month;
    """
    return jsonify(fetch_all(query))


@app.route("/api/get_weather", methods=["GET"])
def get_weather():
    query = """
        SELECT
            country_code,
            year_month,
            avg_temperature,
            total_precipitation
        FROM weather
        ORDER BY country_code, year_month;
    """
    return jsonify(fetch_all(query))


@app.route("/api/get_all", methods=["GET"])
def get_all():
    query = """
        SELECT
            md.country_code,
            md.year_month,
            md.gdp,
            md.inflation,
            md.population,
            md.rice_price_usd_per_kg,
            md.wheat_flour_price_usd_per_kg,
            md.electricity_consumption,
            md.electricity_production,
            md.natural_gas_consumption,
            md.natural_gas_production,
            md.petroleum_consumption,
            md.petroleum_production,
            md.avg_temperature,
            md.total_precipitation
        FROM master_data md
        ORDER BY md.country_code, md.year_month;
    """
    return jsonify(fetch_all(query))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, debug=False)