# pages/02_rfm_segments.py
import plotly.express as px
import streamlit as st
import pandas as pd
from db.models import q, get_years_from

st.title("👥 RFM 세그먼트 (인터랙티브)")

# 필터
years = get_years_from("olist_orders_dataset", "order_purchase_timestamp")
yf, yt = st.sidebar.select_slider("구매 연도 범위", options=years, value=(years[0], years[-1]))
states_txt = st.sidebar.text_input("주(STATE) 필터, 콤마 구분 (예: SP,RJ)", "").strip()
k = st.sidebar.slider("분위수 개수(등급 수)", 3, 10, 5)

# WHERE 절 구성
where = ["strftime('%Y', o.order_purchase_timestamp) BETWEEN :yf AND :yt"]
params = {"yf": yf, "yt": yt}
if states_txt:
    inlist = ",".join(f"'{s.strip()}'" for s in states_txt.split(",") if s.strip())
    where.append(f"""o.customer_id IN (
        SELECT customer_id
        FROM olist_customers_dataset
        WHERE customer_state IN ({inlist})
    )""")
where_sql = "WHERE " + " AND ".join(where)

# RFM 원천 집계
rfm_sql = f"""
WITH orders AS (
  SELECT o.order_id, o.customer_id, o.order_purchase_timestamp
  FROM olist_orders_dataset o
  {where_sql}
),
pay AS (
  SELECT order_id, SUM(payment_value) AS monetary
  FROM olist_order_payments_dataset
  GROUP BY 1
),
deliv AS (
  SELECT order_id, order_delivered_customer_date
  FROM olist_orders_dataset
  WHERE order_delivered_customer_date IS NOT NULL
)
SELECT
  o.customer_id,
  CAST(julianday(MAX(d.order_delivered_customer_date))
       - julianday(MIN(o.order_purchase_timestamp)) AS INTEGER) AS recency_days,
  COUNT(DISTINCT o.order_id) AS frequency,
  COALESCE(SUM(p.monetary),0) AS monetary
FROM orders o
LEFT JOIN pay p USING(order_id)
LEFT JOIN deliv d USING(order_id)
GROUP BY o.customer_id
"""
rfm = q(rfm_sql, params)

if rfm.empty or not {"recency_days","frequency","monetary"}.issubset(rfm.columns):
    st.warning("조건에 맞는 데이터가 없거나 집계 컬럼이 누락됐습니다. 필터를 조정해 주세요.")
    st.stop()

# 분위수 스코어링
def qrank(s: pd.Series, bins: int, reverse=False) -> pd.Series:
    q = pd.qcut(s.rank(method="first"), q=bins, labels=False, duplicates="drop")
    if reverse:
        q = (bins-1) - q
    return (q + 1).astype(int)

rfm["R"] = qrank(rfm["recency_days"], k, reverse=True)
rfm["F"] = qrank(rfm["frequency"], k)
rfm["M"] = qrank(rfm["monetary"], k)
rfm["RFM"] = rfm[["R","F","M"]].sum(axis=1)

def label(row):
    if row["R"]>=k and row["F"]>=k and row["M"]>=k: return "Champions"
    if row["R"]>=k-1 and row["F"]>=k-1:            return "Loyal"
    if row["R"]<=2:                                return "At Risk"
    return "Regular"

rfm["segment"] = rfm.apply(label, axis=1)

# 시각화
c1, c2 = st.columns([1.2, 1.0])
with c1:
    st.subheader("빈도 × 매출(크기=RFM 점수)")
    fig = px.scatter(rfm, x="frequency", y="monetary", color="segment", size="RFM",
                     hover_data=["customer_id","recency_days"])
    st.plotly_chart(fig, use_container_width=True)
with c2:
    st.subheader("세그먼트 비중")
    pie = rfm["segment"].value_counts().rename_axis("segment").reset_index(name="cnt")
    st.plotly_chart(px.pie(pie, names="segment", values="cnt"), use_container_width=True)

# 표/다운로드
st.subheader("고객 리스트")
st.dataframe(rfm.sort_values("RFM", ascending=False), use_container_width=True, height=420)
st.download_button("CSV 다운로드", rfm.to_csv(index=False).encode("utf-8"), "rfm_segments.csv")
