from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Review Analytics", page_icon="🔎", layout="wide")

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "olist.sqlite"

@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=1800)
def q(sql, params=None):
    with get_engine().begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

st.title("🔎 리뷰 분석")

years = q("""
    SELECT DISTINCT strftime('%Y', review_creation_date) AS y
    FROM olist_order_reviews_dataset
    WHERE review_creation_date IS NOT NULL
    ORDER BY 1
""")["y"].dropna().tolist() or ["2016","2017","2018"]

with st.sidebar:
    st.header("필터")
    yf, yt = st.select_slider("리뷰 연도 범위", options=years, value=(years[0], years[-1]))

df = q("""
SELECT strftime('%Y-%m', review_creation_date) AS ym,
       AVG(review_score) AS avg_score,
       COUNT(*) AS reviews
FROM olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
  AND strftime('%Y', review_creation_date) BETWEEN :yf AND :yt
GROUP BY 1 ORDER BY 1
""", {"yf": yf, "yt": yt})

c1, c2 = st.columns(2)
with c1:
    st.subheader("월별 평균 평점")
    st.plotly_chart(px.line(df, x="ym", y="avg_score"), use_container_width=True)
with c2:
    st.subheader("월별 리뷰 수")
    st.plotly_chart(px.bar(df, x="ym", y="reviews"), use_container_width=True)
