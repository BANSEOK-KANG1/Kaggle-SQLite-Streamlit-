import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "olist.sqlite"

@st.cache_resource
def eng():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=1800)
def q(sql, params=None):
    with eng().begin() as c:
        return pd.read_sql(text(sql), c, params or {})

st.title("🔎 Reviews")

years = q("SELECT DISTINCT strftime('%Y', review_creation_date) y FROM olist_order_reviews_dataset WHERE review_creation_date IS NOT NULL ORDER BY 1")["y"].dropna().tolist() or ["2016","2017","2018"]
yf, yt = st.sidebar.select_slider("Review year range", options=years, value=(years[0], years[-1]))

sql = """
SELECT strftime('%Y-%m', review_creation_date) ym,
       AVG(review_score) avg_score,
       COUNT(*) reviews
FROM olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
  AND strftime('%Y', review_creation_date) BETWEEN :yf AND :yt
GROUP BY 1 ORDER BY 1
"""
df = q(sql, {"yf": yf, "yt": yt})
st.line_chart(df.set_index("ym")["avg_score"])
st.bar_chart(df.set_index("ym")["reviews"])
