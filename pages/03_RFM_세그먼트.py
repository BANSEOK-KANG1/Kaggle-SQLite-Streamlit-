# pages/02_RFM_세그먼트.py
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="RFM 세그먼트", page_icon="👥", layout="wide")

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "olist.sqlite"

@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=1800)
def q(sql, params=None):
    with get_engine().begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

st.title("👥 RFM 세그먼트 (인터랙티브)")

# 필터
years = q("""
SELECT DISTINCT strftime('%Y', order_purchase_timestamp) AS y
FROM olist_orders_dataset WHERE order_purchase_timestamp IS NOT NULL ORDER BY 1
""")["y"].dropna().tolist() or ["2016","2017","2018"]

states = q("SELECT DISTINCT customer_state AS st FROM olist_customers_dataset ORDER BY 1")["st"].dropna().tolist()

with st.sidebar:
    st.header("필터")
    yf, yt = st.select_slider("배송 연도 범위", options=years, value=(years[0], years[-1]))
    sel_states = st.text_input("주(STATE) (예: SP, RJ) - 공란이면 전체", "")
    st.header("점수 설정")
    k = st.slider("분위수(등급) 개수", 3, 10, 5)
    r_pow = st.slider("R 가중치", 0.5, 2.0, 1.0, 0.1)
    f_pow = st.slider("F 가중치", 0.5, 2.0, 1.0, 0.1)
    m_pow = st.slider("M 가중치", 0.5, 2.0, 1.0, 0.1)

# RFM 원천 집계 (뷰 사용)
where = ["strftime('%Y', o.order_purchase_timestamp) BETWEEN :yf AND :yt"]
params = {"yf": yf, "yt": yt}

if sel_states.strip():
    tokens = [s.strip() for s in sel_states.split(",") if s.strip()]
    inlist = ",".join(f"'{t}'" for t in tokens)
    where.append(f""" o.customer_id IN (
        SELECT customer_id FROM olist_customers_dataset WHERE customer_state IN ({inlist})
    )""")

where_sql = "WHERE " + " AND ".join(where)

rfm_sql = f"""
WITH orders AS (
  SELECT o.order_id, o.customer_id, o.order_purchase_timestamp
  FROM olist_orders_dataset o
  {where_sql}
), pay AS (
  SELECT order_id, SUM(payment_value) AS monetary
  FROM olist_order_payments_dataset GROUP BY 1
), delivered AS (
  SELECT order_id, order_delivered_customer_date FROM olist_orders_dataset
  WHERE order_delivered_customer_date IS NOT NULL
)
SELECT
  o.customer_id,
  CAST(julianday(MAX(d.order_delivered_customer_date)) - julianday(MIN(o.order_purchase_timestamp)) AS INTEGER) AS recency_days,
  COUNT(DISTINCT o.order_id) AS frequency,
  COALESCE(SUM(p.monetary),0) AS monetary
FROM orders o
LEFT JOIN pay p USING(order_id)
LEFT JOIN delivered d USING(order_id)
GROUP BY o.customer_id
"""
rfm = q(rfm_sql, params)

if rfm.empty:
    st.info("조건에 맞는 데이터가 없습니다. 필터를 조정해 보세요.")
    st.stop()

# 점수화
def quantile_rank(s, k_bins, reverse=False):
    qtiles = pd.qcut(s.rank(method="first"), q=k_bins, labels=False, duplicates="drop")
    if reverse:
        qtiles = (k_bins - 1) - qtiles
    return qtiles + 1

rfm["R"] = quantile_rank(rfm["recency_days"], k, reverse=True) ** r_pow  # recency_days가 작을수록 좋음 → reverse
rfm["F"] = quantile_rank(rfm["frequency"], k, reverse=False) ** f_pow
rfm["M"] = quantile_rank(rfm["monetary"], k, reverse=False) ** m_pow
rfm["RFM"] = rfm["R"] + rfm["F"] + rfm["M"]

# 세그먼트 라벨링(예시)
def label_segment(row, k_bins):
    if row["R"] >= k_bins and row["F"] >= k_bins and row["M"] >= k_bins:
        return "Champions"
    if row["R"] >= k_bins-1 and row["F"] >= k_bins-1:
        return "Loyal"
    if row["R"] <= 2:
        return "At Risk"
    return "Regular"

rfm["segment"] = rfm.apply(label_segment, axis=1, args=(k,))

# 뷰
c1, c2 = st.columns([1.2, 1.0])
with c1:
    st.subheader("분포(산점)")
    fig = px.scatter(rfm, x="frequency", y="monetary", color="segment",
                     size="RFM", hover_data=["customer_id","recency_days"])
    st.plotly_chart(fig, use_container_width=True)
with c2:
    st.subheader("세그먼트 비중")
    pie = rfm["segment"].value_counts().rename_axis("segment").reset_index(name="cnt")
    st.plotly_chart(px.pie(pie, names="segment", values="cnt"), use_container_width=True)

st.subheader("상세 테이블")
st.dataframe(rfm.sort_values("RFM", ascending=False), use_container_width=True, height=420)
st.download_button("RFM CSV 다운로드", rfm.to_csv(index=False).encode("utf-8"), "rfm_segments.csv")
