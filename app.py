#!/usr/bin/env python
# coding: utf-8

from flask import Flask, jsonify, render_template_string
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


def fetch_one(query: str):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            row = cur.fetchone()
            return dict(row) if row else None
    finally:
        conn.close()


@app.route("/")
def home():
    stats = fetch_one("""
        SELECT COUNT(*) AS total_rows
        FROM master_data;
    """)

    countries = fetch_all("""
        SELECT country_code
        FROM countries
        ORDER BY country_code;
    """)

    sample_rows = fetch_all("""
        SELECT
            country_code,
            year_month,
            gdp,
            inflation,
            population,
            rice_price_usd_per_kg,
            wheat_flour_price_usd_per_kg,
            electricity_consumption,
            total_precipitation
        FROM master_data
        ORDER BY country_code, year_month
        LIMIT 12;
    """)

    chart_rows = fetch_all("""
        SELECT
            year_month,
            AVG(total_precipitation) AS avg_precipitation
        FROM master_data
        GROUP BY year_month
        ORDER BY year_month
        LIMIT 24;
    """)

    labels = [row["year_month"] for row in chart_rows]
    values = [float(row["avg_precipitation"] or 0) for row in chart_rows]

    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Global Food Security Data Platform</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            * { box-sizing: border-box; }
            body {
                margin: 0;
                font-family: Inter, Arial, sans-serif;
                background: #f3f6fb;
                color: #18212f;
            }
            .container {
                max-width: 1250px;
                margin: 28px auto;
                padding: 0 20px 30px;
            }
            .hero {
                background: linear-gradient(135deg, #172554, #1d4ed8);
                color: white;
                border-radius: 20px;
                padding: 28px 30px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.15);
                margin-bottom: 22px;
            }
            .hero h1 {
                margin: 0 0 10px 0;
                font-size: 36px;
                font-weight: 800;
            }
            .hero p {
                margin: 0;
                font-size: 16px;
                opacity: 0.95;
            }
            .grid {
                display: grid;
                grid-template-columns: 1.2fr 1.2fr 1fr 1fr;
                gap: 18px;
                margin-bottom: 22px;
            }
            .card {
                background: white;
                border-radius: 18px;
                padding: 20px;
                box-shadow: 0 8px 24px rgba(15,23,42,0.06);
            }
            .metric-label {
                font-size: 13px;
                color: #64748b;
                margin-bottom: 8px;
                text-transform: uppercase;
                letter-spacing: .04em;
                font-weight: 700;
            }
            .metric-value {
                font-size: 30px;
                font-weight: 800;
                color: #0f172a;
            }
            .metric-sub {
                margin-top: 8px;
                color: #475569;
                font-size: 14px;
            }
            .main {
                display: grid;
                grid-template-columns: 1.6fr 1fr;
                gap: 22px;
                margin-bottom: 22px;
            }
            .section-title {
                margin: 0 0 16px 0;
                font-size: 22px;
                font-weight: 800;
            }
            .endpoint-list a {
                display: inline-block;
                margin: 6px 8px 6px 0;
                padding: 10px 14px;
                background: #eff6ff;
                color: #1d4ed8;
                border-radius: 12px;
                text-decoration: none;
                font-weight: 600;
            }
            .endpoint-list a:hover {
                background: #dbeafe;
            }
            .table-card {
                background: white;
                border-radius: 18px;
                padding: 20px;
                box-shadow: 0 8px 24px rgba(15,23,42,0.06);
                overflow: hidden;
            }
            .table-wrap {
                overflow-x: auto;
                border-radius: 14px;
                border: 1px solid #e2e8f0;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                min-width: 900px;
                background: white;
            }
            th, td {
                padding: 12px 14px;
                text-align: left;
                border-bottom: 1px solid #e2e8f0;
                font-size: 14px;
            }
            th {
                background: #f8fafc;
                position: sticky;
                top: 0;
                z-index: 1;
            }
            tr:hover td {
                background: #fafcff;
            }
            .muted {
                color: #64748b;
                font-size: 13px;
                margin-top: 12px;
            }
            .badge {
                display: inline-block;
                padding: 7px 10px;
                border-radius: 999px;
                background: #dcfce7;
                color: #166534;
                font-size: 12px;
                font-weight: 700;
                margin-bottom: 10px;
            }
            .country-list {
                display: flex;
                flex-wrap: wrap;
                gap: 8px;
                margin-top: 10px;
            }
            .country-pill {
                background: #f1f5f9;
                border-radius: 999px;
                padding: 8px 12px;
                font-size: 13px;
                font-weight: 700;
                color: #334155;
            }
            canvas {
                max-height: 320px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="hero">
                <div class="badge">Live Warehouse Dashboard</div>
                <h1>Global Food Security Data Platform</h1>
                <p>Integrated World Bank, EIA, WFP, and weather data in PostgreSQL with API and automated ETL.</p>
            </div>

            <div class="grid">
                <div class="card">
                    <div class="metric-label">API Status</div>
                    <div class="metric-value">Running</div>
                    <div class="metric-sub">Flask service is available</div>
                </div>
                <div class="card">
                    <div class="metric-label">Database Status</div>
                    <div class="metric-value">Connected</div>
                    <div class="metric-sub">PostgreSQL warehouse online</div>
                </div>
                <div class="card">
                    <div class="metric-label">Master Rows</div>
                    <div class="metric-value">{{ total_rows }}</div>
                    <div class="metric-sub">Integrated records in <code>master_data</code></div>
                </div>
                <div class="card">
                    <div class="metric-label">Countries</div>
                    <div class="metric-value">{{ country_count }}</div>
                    <div class="metric-sub">Tracked country codes</div>
                </div>
            </div>

            <div class="main">
                <div class="card">
                    <h2 class="section-title">Average Total Precipitation Trend</h2>
                    <canvas id="precipChart"></canvas>
                </div>

                <div class="card">
                    <h2 class="section-title">API Endpoints</h2>
                    <div class="endpoint-list">
                        <a href="/api/health">Health</a>
                        <a href="/api/get_countries">Countries</a>
                        <a href="/api/get_world_bank">World Bank</a>
                        <a href="/api/get_food_prices">Food Prices</a>
                        <a href="/api/get_energy">Energy</a>
                        <a href="/api/get_weather">Weather</a>
                        <a href="/api/get_all">Master Data</a>
                    </div>
                    <h2 class="section-title" style="margin-top:22px;">Countries</h2>
                    <div class="country-list">
                        {% for c in countries %}
                        <div class="country-pill">{{ c.country_code }}</div>
                        {% endfor %}
                    </div>
                </div>
            </div>

            <div class="table-card">
                <h2 class="section-title">Sample Preview from master_data</h2>
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                {% for col in columns %}
                                <th>{{ col }}</th>
                                {% endfor %}
                            </tr>
                        </thead>
                        <tbody>
                            {% for row in sample_rows %}
                            <tr>
                                {% for col in columns %}
                                <td>{{ row[col] if row[col] is not none else "—" }}</td>
                                {% endfor %}
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
                <div class="muted">Showing the first 12 rows from the integrated warehouse table.</div>
            </div>
        </div>

        <script>
            const labels = {{ labels | tojson }};
            const values = {{ values | tojson }};

            new Chart(document.getElementById('precipChart'), {
                type: 'line',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Average precipitation',
                        data: values,
                        borderColor: '#2563eb',
                        backgroundColor: 'rgba(37, 99, 235, 0.15)',
                        tension: 0.25,
                        fill: true
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { display: true }
                    },
                    scales: {
                        y: { beginAtZero: true }
                    }
                }
            });
        </script>
    </body>
    </html>
    """

    columns = list(sample_rows[0].keys()) if sample_rows else []

    return render_template_string(
        html,
        total_rows=stats["total_rows"] if stats else 0,
        country_count=len(countries),
        countries=countries,
        sample_rows=sample_rows,
        columns=columns,
        labels=labels,
        values=values,
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
    return jsonify(fetch_all("""
        SELECT country_code, country_name
        FROM countries
        ORDER BY country_code;
    """))


@app.route("/api/get_world_bank", methods=["GET"])
def get_world_bank():
    return jsonify(fetch_all("""
        SELECT country_code, year_month, gdp, inflation, population, source, load_timestamp
        FROM worldbank
        ORDER BY country_code, year_month;
    """))


@app.route("/api/get_food_prices", methods=["GET"])
def get_food_prices():
    return jsonify(fetch_all("""
        SELECT
            country_code,
            year_month,
            rice_price_usd_per_kg,
            wheat_flour_price_usd_per_kg
        FROM food_prices
        ORDER BY country_code, year_month;
    """))


@app.route("/api/get_energy", methods=["GET"])
def get_energy():
    return jsonify(fetch_all("""
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
    """))


@app.route("/api/get_weather", methods=["GET"])
def get_weather():
    return jsonify(fetch_all("""
        SELECT
            country_code,
            year_month,
            avg_temperature,
            total_precipitation
        FROM weather
        ORDER BY country_code, year_month;
    """))


@app.route("/api/get_all", methods=["GET"])
def get_all():
    return jsonify(fetch_all("""
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
    """))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, debug=True)