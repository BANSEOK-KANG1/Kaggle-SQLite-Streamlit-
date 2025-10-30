# pages/01_reviews.py  — 안정화 버전
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from pathlib import Path
import plotly.express as px
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "olist.sqlite"

@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=900)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """읽기 전용 쿼리. 드라이버 이슈 시 sqlite3 폴백."""
    params = params or {}
    try:
        with get_engine().begin() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception:
        with sqlite3.connect(DB_PATH) as con:
            return pd.read_sql_query(sql, con, params=params)

st.title("🔎 리뷰 분석")

# 1) 연도 목록(방어적)
years_df = q("""
    SELECT DISTINCT strftime('%Y', review_creation_date) AS y
    FROM olist_order_reviews_dataset
    WHERE review_creation_date IS NOT NULL
    ORDER BY 1;
""")
years = (years_df["y"].dropna().astype(str).tolist()) if not years_df.empty else ["2016","2017","2018"]
yf, yt = st.sidebar.select_slider("리뷰 연도 범위", options=years, value=(years[0], years[-1]))

# 2) 메인 집계 쿼리 (SQL 먼저 → q() 호출)
sql = """
SELECT strftime('%Y-%m', review_creation_date) AS ym,
       AVG(review_score)                         AS avg_score,
       COUNT(*)                                  AS reviews
FROM olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
  AND strftime('%Y', review_creation_date) BETWEEN :yf AND :yt
GROUP BY 1
ORDER BY 1;
"""
df = q(sql, {"yf": yf, "yt": yt})

# 3) 안전 가드 + Plotly 시각화
if df.empty or not {"ym", "avg_score", "reviews"}.issubset(df.columns):
    st.info("해당 구간 리뷰가 없습니다. 범위를 조정해 주세요.")
else:
    st.subheader("월별 평균 평점")
    st.plotly_chart(px.line(df, x="ym", y="avg_score"), use_container_width=True)

    st.subheader("월별 리뷰 수")
    st.plotly_chart(px.bar(df, x="ym", y="reviews"), use_container_width=True)

    with st.expander("원본 데이터 보기"):
        st.dataframe(df, use_container_width=True, height=360)
    st.download_button("CSV 다운로드", df.to_csv(index=False).encode("utf-8"), "reviews_by_month.csv")
