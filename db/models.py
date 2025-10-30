# models.py
from pathlib import Path
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import streamlit as st

DB_PATH = Path(__file__).resolve().parent / "data" / "olist.sqlite"

@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=900)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """읽기 전용 쿼리. 드라이버/락 이슈 시 sqlite3 폴백."""
    params = params or {}
    try:
        with get_engine().begin() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception:
        with sqlite3.connect(DB_PATH) as con:
            return pd.read_sql_query(sql, con, params=params)

@st.cache_data(ttl=3600)
def get_years_from(table: str, ts_col: str) -> list[str]:
    sql = f"""
    SELECT DISTINCT strftime('%Y', {ts_col}) AS y
    FROM {table}
    WHERE {ts_col} IS NOT NULL
    ORDER BY 1
    """
    df = q(sql)
    return df["y"].dropna().astype(str).tolist() if not df.empty else ["2016","2017","2018"]
