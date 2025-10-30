# pages/01_reviews.py
import plotly.express as px
import streamlit as st
import pandas as pd
from db.models import q, get_years_from

st.title("🔎 리뷰 분석")

# 필터
years = get_years_from("olist_order_reviews_dataset", "review_creation_date")
yf, yt = st.sidebar.select_slider("리뷰 연도 범위", options=years, value=(years[0], years[-1]))
min_len = st.sidebar.slider("최소 리뷰 글자수(요약용)", 0, 50, 0)

# 집계
sql = """
SELECT strftime('%Y-%m', review_creation_date) AS ym,
       AVG(review_score)                         AS avg_score,
       COUNT(*)                                  AS reviews
FROM olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
  AND strftime('%Y', review_creation_date) BETWEEN :yf AND :yt
GROUP BY 1
ORDER BY 1
"""
df = q(sql, {"yf": yf, "yt": yt})

if df.empty or not {"ym","avg_score","reviews"}.issubset(df.columns):
    st.info("해당 구간 리뷰가 없습니다. 범위를 조정해 주세요.")
else:
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("월별 평균 평점")
        st.plotly_chart(px.line(df, x="ym", y="avg_score"), use_container_width=True)
    with c2:
        st.subheader("월별 리뷰 수")
        st.plotly_chart(px.bar(df, x="ym", y="reviews"), use_container_width=True)

    st.subheader("데이터")
    st.dataframe(df, use_container_width=True, height=360)
    st.download_button("CSV 다운로드", df.to_csv(index=False).encode("utf-8"), "reviews_by_month.csv")

# (선택) 낮은 평점 리뷰 리스트(간단 요약)
low_sql = """
SELECT review_id, review_score, SUBSTR(review_comment_message,1,280) AS snippet,
       strftime('%Y-%m', review_creation_date) AS ym
FROM olist_order_reviews_dataset
WHERE review_creation_date IS NOT NULL
  AND strftime('%Y', review_creation_date) BETWEEN :yf AND :yt
  AND review_score <= 2
  AND (review_comment_message IS NULL OR length(review_comment_message) >= :min_len)
ORDER BY review_score ASC, review_creation_date DESC
LIMIT 200
"""
low = q(low_sql, {"yf": yf, "yt": yt, "min_len": min_len})
with st.expander("🧯 저평점 리뷰 빠른 스캔(최근 200개)"):
    st.dataframe(low, use_container_width=True, height=360)
