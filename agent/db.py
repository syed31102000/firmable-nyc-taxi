import duckdb
import pandas as pd
from agent.config import DB_PATH, MAX_ROWS

def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)

def run_query(sql: str) -> pd.DataFrame:
    con = get_connection()
    try:
        df = con.execute(sql).fetchdf()
        return df.head(MAX_ROWS)
    finally:
        con.close()

def list_tables():
    con = get_connection()
    try:
        df = con.execute("SHOW TABLES").fetchdf()
        return df
    finally:
        con.close()

def get_table_schema(table_name: str) -> pd.DataFrame:
    con = get_connection()
    try:
        return con.execute(f"DESCRIBE {table_name}").fetchdf()
    finally:
        con.close()