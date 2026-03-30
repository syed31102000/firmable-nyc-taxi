from agent.config import ALLOWED_TABLES
from agent.db import get_table_schema

def build_schema_context() -> str:
    lines = []
    for table in ALLOWED_TABLES:
        schema = get_table_schema(table)
        lines.append(f"Table: {table}")
        for _, row in schema.iterrows():
            col_name = row["column_name"]
            col_type = row["column_type"]
            lines.append(f"  - {col_name}: {col_type}")
        lines.append("")
    return "\n".join(lines)