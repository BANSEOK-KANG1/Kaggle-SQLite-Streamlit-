# pages/03_RFM_세그먼트.py
from pathlib import Path
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

st.set_page_config(layout="wide")
st.title("👥 RFM 세그먼트 (인터랙티브)")

DB_PATH = Path("data/olist.sqlite")
if not DB_PATH.exists():
    st.error("DB가 없습니다. 먼저 ETL을 실행하세요.")
    st.stop()

@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=1800, show_spinner=False)
def read_df(sql, params=None):
    eng = get_engine()
    with eng.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# ─────────────────────────────────────────────────────────────────────────────
# 사이드바: 필터 + 점수/세그먼트 설정
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("필터")
    # 가용 연도 자동 탐색(배송연도 기준)
    years = read_df(
        """
        SELECT DISTINCT strftime('%Y', o.order_delivered_customer_date) AS y
        FROM olist_orders_dataset o
        WHERE o.order_delivered_customer_date IS NOT NULL
        ORDER BY 1
        """
    )["y"].dropna().tolist()
    if not years:
        years = ["2017","2018"]
    year_min, year_max = years[0], years[-1]
    y_from, y_to = st.select_slider("배송 연도 범위", options=years, value=(year_min, year_max))
    state = st.text_input("주(STATE) (예: SP, RJ) - 공란이면 전체", value="").strip().upper()

    st.divider()
    st.header("점수 설정")
    bins = st.slider("분위수(등급) 개수(기본 5분위)", min_value=3, max_value=10, value=5, step=1,
                     help="예: 5면 1~5점으로 나뉨")
    wR = st.slider("R 가중치", 0.0, 5.0, 1.0, 0.1)
    wF = st.slider("F 가중치", 0.0, 5.0, 1.0, 0.1)
    wM = st.slider("M 가중치", 0.0, 5.0, 1.0, 0.1)

    st.caption("※ R(Recency)은 값이 작을수록 최근 구매 → 높은 점수, F/M은 클수록 높은 점수.")

    st.divider()
    st.header("세그먼트 규칙(간단)")
    total_thr_champion = st.slider("Champions: 총점 이상", min_value=bins*0.6, max_value=bins*3.0,
                                   value=float(bins*2.6), step=0.1)
    total_thr_loyal = st.slider("Loyal: 총점 이상(Champions 미만)", min_value=bins*0.4, max_value=bins*2.6,
                                value=float(bins*2.2), step=0.1)
    at_risk_R_max = st.slider("At Risk: R 점수 이하", min_value=1.0, max_value=float(bins), value=2.0, step=0.1)
    potential_F_max = st.slider("Potential: F 점수 이하(최근은 높은데 빈도 낮음)", min_value=1.0, max_value=float(bins),
                                value=2.0, step=0.1)

    st.divider()
    st.header("추가 필터")
    min_freq = st.number_input("최소 구매 횟수(F) 필터", min_value=0, value=0, step=1)
    min_monetary = st.number_input("최소 누적 결제액(M) 필터", min_value=0.0, value=0.0, step=10.0)
    apply_btn = st.button("필터 적용 / 재계산")

# ─────────────────────────────────────────────────────────────────────────────
# 데이터 로딩: vw_rfm_base + 고객/주문 결합으로 조건 반영
# ─────────────────────────────────────────────────────────────────────────────
where = ["o.order_delivered_customer_date IS NOT NULL"]
params = {}
if state:
    where.append("c.customer_state = :s")
    params["s"] = state
if y_from and y_to:
    where.append("strftime('%Y', o.order_delivered_customer_date) BETWEEN :yf AND :yt")
    params["yf"], params["yt"] = y_from, y_to

where_sql = "WHERE " + " AND ".join(where)

sql = f"""
SELECT
  r.customer_id,
  r.recency_days,
  r.frequency,
  r.monetary,
  c.customer_state,
  MIN(o.order_purchase_timestamp) AS first_purchase_at,
  MAX(o.order_delivered_customer_date) AS last_delivered_at
FROM vw_rfm_base r
JOIN olist_customers_dataset c ON c.customer_id = r.customer_id
JOIN olist_orders_dataset o ON o.customer_id = r.customer_id
{where_sql}
GROUP BY r.customer_id, r.recency_days, r.frequency, r.monetary, c.customer_state
"""
rfm = read_df(sql, params)

if min_freq > 0:
    rfm = rfm[rfm["frequency"] >= min_freq]
if min_monetary > 0:
    rfm = rfm[rfm["monetary"] >= min_monetary]

if rfm.empty:
    st.warning("조건에 맞는 고객이 없습니다. 필터를 조정해보세요.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# R/F/M 스코어링(가변 분위수 + 가중 합산)
# ─────────────────────────────────────────────────────────────────────────────
def quantile_score(x: pd.Series, q_bins: int, higher_is_better: bool) -> pd.Series:
    # tie가 많아도 안정적으로 등분하려고 rank pct 사용
    pct = x.rank(method="average", pct=True).clip(0, 1)  # 0~1
    if higher_is_better:
        score = np.ceil(pct * q_bins).astype(int)
    else:
        score = (q_bins + 1 - np.ceil(pct * q_bins)).astype(int)
    score = score.clip(1, q_bins)
    return score

rfm["R_score"] = quantile_score(rfm["recency_days"], q_bins=bins, higher_is_better=False)
rfm["F_score"] = quantile_score(rfm["frequency"], q_bins=bins, higher_is_better=True)
rfm["M_score"] = quantile_score(rfm["monetary"],  q_bins=bins, higher_is_better=True)

# 가중 합계 점수(실수 허용)
rfm["RFM_weighted"] = rfm["R_score"] * wR + rfm["F_score"] * wF + rfm["M_score"] * wM

# ─────────────────────────────────────────────────────────────────────────────
# 세그먼트 라벨링(간단 규칙 편집 가능)
# ─────────────────────────────────────────────────────────────────────────────
def label_segment(row):
    total = row["RFM_weighted"]
    # champions > loyal > at risk > potential > others
    if total >= total_thr_champion:
        return "Champions"
    if total >= total_thr_loyal:
        return "Loyal"
    if row["R_score"] <= at_risk_R_max:
        return "At Risk"
    if row["R_score"] >= (bins - 1) and row["F_score"] <= potential_F_max:
        return "Potential"
    return "Others"

rfm["segment"] = rfm.apply(label_segment, axis=1)

# ─────────────────────────────────────────────────────────────────────────────
# KPI 카드
# ─────────────────────────────────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("고객 수", f"{len(rfm):,}")
with c2:
    st.metric("중앙값 Recency(일)", int(rfm["recency_days"].median()))
with c3:
    st.metric("평균 Frequency", f"{rfm['frequency'].mean():.2f}")
with c4:
    st.metric("평균 Monetary", f"{rfm['monetary'].mean():,.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 뷰 1: 세그먼트 분포 + 다운로드
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("세그먼트 분포")
seg_cnt = (rfm.groupby("segment").size()
           .reset_index(name="cnt")
           .sort_values("cnt", ascending=False))
fig_bar = px.bar(seg_cnt, x="segment", y="cnt", text="cnt")
fig_bar.update_traces(textposition="outside")
st.plotly_chart(fig_bar, use_container_width=True)

# 내보내기 버튼(전체/세그먼트 선택)
exp_cols = st.columns([2,2,2,2,2,2])
with exp_cols[0]:
    st.download_button("전체 CSV 다운로드", rfm.to_csv(index=False).encode("utf-8"), file_name="rfm_all.csv")
with exp_cols[1]:
    seg_pick = st.selectbox("세그먼트 선택(내보내기/드릴다운)", options=seg_cnt["segment"].tolist())
with exp_cols[2]:
    seg_df = rfm[rfm["segment"] == seg_pick].copy()
    st.download_button("선택 세그먼트 CSV", seg_df.to_csv(index=False).encode("utf-8"),
                       file_name=f"rfm_{seg_pick}.csv", disabled=seg_df.empty)

# ─────────────────────────────────────────────────────────────────────────────
# 뷰 2: R vs F 버블(색=세그먼트, 크기=M), 히트맵
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("R vs F (색: 세그먼트, 크기: M)")
fig_sc = px.scatter(
    rfm, x="recency_days", y="frequency", color="segment", size="monetary",
    hover_data=["R_score","F_score","M_score","RFM_weighted","customer_state"]
)
st.plotly_chart(fig_sc, use_container_width=True)

st.subheader("R_score × F_score 히트맵(고객 수)")
rfm["_R"] = rfm["R_score"].astype(int)
rfm["_F"] = rfm["F_score"].astype(int)
hm = rfm.pivot_table(index="_R", columns="_F", values="customer_id", aggfunc="count", fill_value=0)
hm = hm.sort_index(ascending=True)  # R은 낮을수록 최근이므로 낮은 점수=최근 → 해석 편의상 오름차순
fig_hm = px.imshow(hm, aspect="auto", labels=dict(x="F_score", y="R_score", color="고객 수"))
st.plotly_chart(fig_hm, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# 뷰 3: STATE별 분포/세그먼트 비중
# ─────────────────────────────────────────────────────────────────────────────
st.subheader("STATE별 RFM 요약")
top_states = rfm["customer_state"].value_counts().index[:12].tolist()
state_sel = st.multiselect("STATE 선택(최대 12)", options=top_states, default=top_states)
state_df = rfm[rfm["customer_state"].isin(state_sel)]
gp_state = (state_df.groupby(["customer_state","segment"]).size()
            .reset_index(name="cnt"))
fig_st = px.bar(gp_state, x="customer_state", y="cnt", color="segment", barmode="stack")
st.plotly_chart(fig_st, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# 뷰 4: 드릴다운 테이블(선택 세그먼트)
# ─────────────────────────────────────────────────────────────────────────────
st.subheader(f"세그먼트 드릴다운: {seg_pick}")
view_cols = [
    "customer_id","segment","R_score","F_score","M_score","RFM_weighted",
    "recency_days","frequency","monetary","customer_state","first_purchase_at","last_delivered_at"
]
st.dataframe(
    seg_df[view_cols].sort_values(["RFM_weighted","monetary","frequency"], ascending=[False, False, False]),
    use_container_width=True, height=420
)

# 적용 버튼 UX(선택): 클릭 시 토스트
if apply_btn:
    st.toast("필터/설정이 적용되었습니다.", icon="✅")
