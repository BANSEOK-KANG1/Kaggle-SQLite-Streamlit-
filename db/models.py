# db/models.py
from __future__ import annotations
from pathlib import Path
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, text
import streamlit as st

# ✅ repo 루트 기준: <repo>/data/olist.sqlite
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "olist.sqlite"

@st.cache_resource
def get_engine():
    # DB가 아직 없으면 연결은 되지만 테이블이 없을 수 있음 → q()에서 체크
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

def _table_exists(table: str) -> bool:
    if not DB_PATH.exists():
        return False
    try:
        with sqlite3.connect(DB_PATH) as con:
            cur = con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            )
            return cur.fetchone() is not None
    except Exception:
        return False

@st.cache_data(ttl=900)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """읽기 전용 쿼리. 테이블 없으면 빈 DF 반환(페이지에서 안내)."""
    params = params or {}

    # 가장 흔히 조회하는 테이블명을 heuristic하게 추출해서 존재 확인
    # (FROM 다음 첫 토큰을 테이블명으로 가정)
    try:
        lower = " ".join(sql.lower().split())
        if " from " in lower:
            tkn = lower.split(" from ", 1)[1].split()[0]
            # view면 넘어가고 물리 테이블이면 존재 확인
            core = tkn.strip().strip(";")
            if not _table_exists(core) and not DB_PATH.exists():
                # DB 자체가 없으면 즉시 빈 DF
                return pd.DataFrame()
    except Exception:
        pass

    # 우선 SQLAlchemy
    try:
        with get_engine().begin() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception:
        # 폴백 sqlite3
        try:
            with sqlite3.connect(DB_PATH) as con:
                return pd.read_sql_query(sql, con, params=params)
        except Exception:
            # 최종 폴백: 빈 DF 반환 (페이지 측에서 안내)
            return pd.DataFrame()

@st.cache_data(ttl=900)
def get_years_from(table: str, ts_col: str) -> list[str]:
    """연도 리스트. 테이블/컬럼 없으면 기본값 반환."""
    if not _table_exists(table):
        return ["2016", "2017", "2018"]
    sql = f"""
    SELECT DISTINCT strftime('%Y', {ts_col}) AS y
    FROM {table}
    WHERE {ts_col} IS NOT NULL
    ORDER BY 1
    """
    df = q(sql)
    return df["y"].dropna().astype(str).tolist() if not df.empty else ["2016","2017","2018"]
