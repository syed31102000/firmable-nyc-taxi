import duckdb
import os

DB_PATH = "data/warehouse/nyc_taxi.duckdb"
RAW_GLOB = "data/raw/yellow_tripdata_2023-*.parquet"

os.makedirs("data/warehouse", exist_ok=True)

con = duckdb.connect(DB_PATH)

con.execute(f"""
CREATE OR REPLACE TABLE yellow_tripdata_raw AS
SELECT *
FROM read_parquet('{RAW_GLOB}')
""")

row_count = con.execute("SELECT COUNT(*) FROM yellow_tripdata_raw").fetchone()[0]
print(f"Loaded yellow_tripdata_raw with {row_count} rows")