import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "t9pipe"),
    "user": os.getenv("DB_USER", "jhu"),
    "password": os.getenv("DB_PASSWORD", "jhu123"),
}

EIA_API_KEY = os.getenv("EIA_API_KEY")
WEATHER_BASE_URL = os.getenv("WEATHER_BASE_URL", "")