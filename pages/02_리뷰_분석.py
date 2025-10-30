import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
from pathlib import Path

st.set_page_config(layout="wide")
st.title("⭐ 리뷰 분석")

DB_PATH = Path("data/olist.sqlite")
engine = create_engine(f"sqlite:///{DB_PATH}", future=True)

with engine.begin() as conn:
    reviews = pd.read_sql(text("select * from olist_order_reviews_dataset"), conn)

st.write("리뷰 점수 분포")
fig = px.histogram(reviews, x="review_score", nbins=5)
st.plotly_chart(fig, use_container_width=True)

st.write("리뷰 길이 vs 점수")
reviews["length"] = reviews["review_comment_message"].fillna("").str.len()
fig2 = px.box(reviews, x="review_score", y="length")
st.plotly_chart(fig2, use_container_width=True)
