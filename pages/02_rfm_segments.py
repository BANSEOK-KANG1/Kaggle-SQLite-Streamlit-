import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from pathlib import Path
import plotly.express as px

# --- 상단 import와 경로는 그대로 두고, q()를 아래처럼 교체 ---
@st.cache_resource
def eng():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=900)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    """읽기 전용 쿼리. 실패 시 sqlite3로 폴백."""
    params = params or {}
    try:
        with eng().begin() as c:
            return pd.read_sql(text(sql), c, params=params)
    except Exception as e:
        # 폴백: sqlite3로 동일 쿼리 시도
        import sqlite3
        with sqlite3.connect(DB_PATH) as con:
            df = pd.read_sql_query(sql, con, params=params)
        return df

# --- 연도 목록 생성도 방어적으로 ---
_years_df = q("""
    SELECT DISTINCT strftime('%Y', order_purchase_timestamp) AS y
    FROM olist_orders_dataset
    WHERE order_purchase_timestamp IS NOT NULL
    ORDER BY 1
""")
years = (_years_df["y"].dropna().astype(str).tolist()) if not _years_df.empty else ["2016","2017","2018"]
yf, yt = st.sidebar.select_slider("Delivered year range", options=years, value=(years[0], years[-1]))

states = st.sidebar.text_input("States (comma-separated, e.g., SP,RJ)", "").strip()

where = ["strftime('%Y', o.order_purchase_timestamp) BETWEEN :yf AND :yt"]
params = {"yf": yf, "yt": yt}
if states:
    inlist = ",".join(f"'{s.strip()}'" for s in states.split(",") if s.strip())
    where.append(f"""o.customer_id IN (SELECT customer_id FROM olist_customers_dataset WHERE customer_state IN ({inlist}))""")
where_sql = "WHERE " + " AND ".join(where)

rfm_sql = f"""
WITH orders AS (
  SELECT o.order_id, o.customer_id, o.order_purchase_timestamp
  FROM olist_orders_dataset o
  {where_sql}
),
pay AS (
  SELECT order_id, SUM(payment_value) monetary
  FROM olist_order_payments_dataset GROUP BY 1
),
deliv AS (
  SELECT order_id, order_delivered_customer_date
  FROM olist_orders_dataset WHERE order_delivered_customer_date IS NOT NULL
)
SELECT
  o.customer_id,
  CAST(julianday(MAX(d.order_delivered_customer_date)) - julianday(MIN(o.order_purchase_timestamp)) AS INTEGER) recency_days,
  COUNT(DISTINCT o.order_id) frequency,
  COALESCE(SUM(p.monetary),0) monetary
FROM orders o
LEFT JOIN pay p USING(order_id)
LEFT JOIN deliv d USING(order_id)
GROUP BY o.customer_id
"""
rfm = q(rfm_sql, params)
if rfm.empty:
    st.info("No data. Adjust filters.")
else:
    k = st.sidebar.slider("Quantile bins", 3, 10, 5)
    def qrank(s, k, rev=False):
        q = pd.qcut(s.rank(method="first"), q=k, labels=False, duplicates="drop")
        if rev: q = (k-1)-q
        return q+1
    rfm["R"] = qrank(rfm["recency_days"], k, True)
    rfm["F"] = qrank(rfm["frequency"], k, False)
    rfm["M"] = qrank(rfm["monetary"], k, False)
    rfm["RFM"] = rfm[["R","F","M"]].sum(axis=1)

    def label(row):
        if row["R"]>=k and row["F"]>=k and row["M"]>=k: return "Champions"
        if row["R"]>=k-1 and row["F"]>=k-1: return "Loyal"
        if row["R"]<=2: return "At Risk"
        return "Regular"
    rfm["segment"] = rfm.apply(label, axis=1)

    c1, c2 = st.columns([1.2,1.0])
    with c1:
        fig = px.scatter(rfm, x="frequency", y="monetary", color="segment", size="RFM",
                         hover_data=["customer_id","recency_days"])
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        pie = rfm["segment"].value_counts().rename_axis("segment").reset_index(name="cnt")
        st.plotly_chart(px.pie(pie, names="segment", values="cnt"), use_container_width=True)

    st.dataframe(rfm.sort_values("RFM", ascending=False), use_container_width=True, height=420)
