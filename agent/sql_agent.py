import re
import pandas as pd
from agent.prompts import SYSTEM_PROMPT, SUMMARY_PROMPT
from agent.llm import call_ollama
from agent.schema_context import build_schema_context
from agent.db import run_query
from agent.config import ALLOWED_TABLES

FORBIDDEN_PATTERNS = [
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bDROP\b", r"\bALTER\b",
    r"\bCREATE\b", r"\bTRUNCATE\b", r"\bCOPY\b", r"\bATTACH\b", r"\bDETACH\b",
    r"\bPRAGMA\b", r"\bINSTALL\b", r"\bLOAD\b"
]

def validate_sql(sql: str) -> None:
    upper_sql = sql.upper()

    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, upper_sql):
            raise ValueError(f"Forbidden SQL detected: {pattern}")

    referenced_tables = set(re.findall(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)|\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE))
    flattened = set()
    for a, b in referenced_tables:
        if a:
            flattened.add(a)
        if b:
            flattened.add(b)

    invalid = [t for t in flattened if t not in ALLOWED_TABLES]
    if invalid:
        raise ValueError(f"SQL references disallowed tables: {invalid}")

def generate_sql(user_question: str) -> str:
    schema_context = build_schema_context()
    prompt = f"""
Schema:
{schema_context}

User question:
{user_question}
"""
    sql = call_ollama(prompt=prompt, system=SYSTEM_PROMPT)
    sql = sql.strip().strip("```").strip()
    validate_sql(sql)
    return sql

def summarize_answer(user_question: str, sql: str, df: pd.DataFrame) -> str:
    preview = df.head(30).to_markdown(index=False)
    prompt = f"""
User question:
{user_question}

SQL used:
{sql}

Result:
{preview}
"""
    return call_ollama(prompt=prompt, system=SUMMARY_PROMPT)

def answer_question(user_question: str):
    sql = generate_sql(user_question)
    df = run_query(sql)
    summary = summarize_answer(user_question, sql, df)
    return sql, df, summary