# pages/02_rfm_segments.py
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from db.models import q, get_years_from

st.title("👥 RFM 세그먼트 (인터랙티브)")

# ───────────────────────────── 필터 영역 ─────────────────────────────
years = get_years_from("olist_orders_dataset", "order_purchase_timestamp")
yf, yt = st.sidebar.select_slider("구매 연도 범위", options=years, value=(years[0], years[-1]))
states_txt = st.sidebar.text_input("주(STATE) 필터, 콤마 구분 (예: SP,RJ)", "").strip()

st.sidebar.markdown("---")
k = st.sidebar.slider("분위수 개수(등급 수)", 3, 10, 5)
wR = st.sidebar.slider("R 가중치", 1, 5, 1)
wF = st.sidebar.slider("F 가중치", 1, 5, 2)
wM = st.sidebar.slider("M 가중치", 1, 5, 2)

st.sidebar.markdown("---")
min_orders = st.sidebar.slider("최소 주문수(F) 필터", 0, 10, 0)
min_money  = st.sidebar.number_input("최소 매출(M) 필터", min_value=0.0, value=0.0, step=10.0, format="%.2f")
max_recency= st.sidebar.number_input("최대 Recency(일) 필터(0은 제한없음)", min_value=0, value=0, step=10)

log_money  = st.sidebar.checkbox("매출 로그스케일 사용(log10)", value=False)
top_n      = st.sidebar.slider("표·산점도 상위 N(총 RFM 점수 기준)", 100, 5000, 1000, step=100)

# ───────────────────────────── SQL 집계 ─────────────────────────────
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

rfm_sql = f"""
WITH filtered AS (
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
),
cust AS (
  SELECT
    f.customer_id,
    MAX(d.order_delivered_customer_date) AS last_delivered,
    COUNT(DISTINCT f.order_id)          AS frequency,
    COALESCE(SUM(p.monetary), 0)        AS monetary
  FROM filtered f
  LEFT JOIN deliv d USING(order_id)
  LEFT JOIN pay   p USING(order_id)
  GROUP BY f.customer_id
),
last_date AS (
  SELECT MAX(d.order_delivered_customer_date) AS max_delivered
  FROM filtered f
  JOIN deliv d USING(order_id)
)
SELECT
  c.customer_id,
  CAST(julianday(l.max_delivered) - julianday(c.last_delivered) AS REAL) AS recency_days,
  c.frequency,
  c.monetary
FROM cust c
CROSS JOIN last_date l
"""
rfm = q(rfm_sql, params)

# ───────────────────────────── 방어 로직 ─────────────────────────────
if rfm.empty or not {"recency_days","frequency","monetary"}.issubset(rfm.columns):
    st.info("조건에 맞는 데이터가 없습니다. (DB 미준비/필터 과도/배송일 부재)")
    st.stop()

# NaN/Inf 정리
rfm = rfm.replace([np.inf, -np.inf], np.nan)
rfm = rfm.dropna(subset=["frequency", "monetary"])  # 핵심 지표 결측 제거
# recency_days는 결측일 수 있으므로, 큰 값으로 대체하여 낮은 점수 받게 함
if rfm["recency_days"].isna().any():
    rfm["recency_days"] = rfm["recency_days"].fillna(rfm["recency_days"].max(skipna=True) + 1)

# 필터 적용
if min_orders > 0:
    rfm = rfm[rfm["frequency"] >= min_orders]
if min_money > 0:
    rfm = rfm[rfm["monetary"] >= min_money]
if max_recency > 0:
    rfm = rfm[rfm["recency_days"] <= max_recency]

if rfm.empty:
    st.info("필터 결과가 비어있습니다. 필터 범위를 완화하세요.")
    st.stop()

# ───────────────────── 안전한 스코어러(퍼센트랭크 기반) ────────────────────
def safe_rank_to_bins(s: pd.Series, bins: int, reverse: bool = False) -> pd.Series:
    """
    퍼센트랭크 기반 등분할 스코어(1..bins).
    - 유일값 부족/NaN이 있어도 항상 1..bins 정수 반환
    - reverse=True면 값이 작을수록 높은 점수(Recency용)
    """
    s = pd.to_numeric(s, errors="coerce")
    pct = s.rank(pct=True, method="first")  # 0~1
    if reverse:
        pct = 1 - pct
    score = np.ceil(pct * bins).astype("float")
    score = score.clip(1, bins)
    # NaN 발생 시 1로 대체(가장 낮은 점수)
    return score.fillna(1).astype(int)

rfm["R"] = safe_rank_to_bins(rfm["recency_days"], k, reverse=True)
rfm["F"] = safe_rank_to_bins(rfm["frequency"],     k, reverse=False)
rfm["M"] = safe_rank_to_bins(rfm["monetary"],      k, reverse=False)

# 가중합 RFM
rfm["RFM"] = (wR * rfm["R"] + wF * rfm["F"] + wM * rfm["M"]).astype(int)

# 기본 세그먼트 룰 (가볍게 조정 가능)
def label_row(r, k):
    if (r["R"] >= k-0) and (r["F"] >= k-1) and (r["M"] >= k-1):
        return "Champions"
    if (r["R"] >= k-1) and (r["F"] >= k-2):
        return "Loyal"
    if (r["R"] <= 2):
        return "At Risk"
    if (r["F"] >= k-2) and (r["M"] >= k-2):
        return "Potential"
    return "Regular"

rfm["segment"] = rfm.apply(lambda r: label_row(r, k), axis=1)

# 시각화용 변환
plot_df = rfm.copy()
if log_money:
    plot_df["monetary"] = np.log10(plot_df["monetary"].replace(0, np.nan)).fillna(0)

# 상위 N (총점) 제한
plot_df = plot_df.sort_values("RFM", ascending=False).head(top_n)

# ───────────────────────────── 시각화 ─────────────────────────────
c1, c2 = st.columns([1.2, 1.0])
with c1:
    st.subheader("빈도 × 매출 (버블=RFM)")
    fig = px.scatter(
        plot_df,
        x="frequency", y="monetary",
        size="RFM", color="segment",
        hover_data=["customer_id","recency_days","R","F","M","RFM"],
    )
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")
with c2:
    st.subheader("세그먼트 비중")
    pie = rfm["segment"].value_counts().rename_axis("segment").reset_index(name="cnt")
    st.plotly_chart(px.pie(pie, names="segment", values="cnt"), use_container_width=True)

st.subheader("고객 리스트 (정렬/필터 후 상위 N 표시)")
st.dataframe(
    rfm.sort_values("RFM", ascending=False).head(top_n),
    use_container_width=True, height=420
)
st.download_button(
    "CSV 다운로드",
    rfm.sort_values("RFM", ascending=False).to_csv(index=False).encode("utf-8"),
    "rfm_segments.csv",
)
