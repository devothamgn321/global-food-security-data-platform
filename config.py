import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(ENV_PATH)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "t9pipe"),
    "user": os.getenv("DB_USER", "jhu"),
    "password": os.getenv("DB_PASSWORD", "jhu123"),
}

EIA_API_KEY = os.getenv("EIA_API_KEY", "")
WEATHER_BASE_URL = os.getenv("WEATHER_BASE_URL", "")