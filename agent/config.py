DB_PATH = "data/warehouse/nyc_taxi.duckdb"
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "qwen2.5:7b-instruct"

ALLOWED_TABLES = [
    "fct_trips",
    "dim_zones",
    "agg_daily_revenue",
    "agg_zone_performance",
]

MAX_ROWS = 200