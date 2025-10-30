# app.py 맨 위 import 아래쪽에 추가
import subprocess

DB_PATH = Path("data/olist.sqlite")
if not DB_PATH.exists():
    st.warning("⚙️ 데이터베이스가 없습니다. 자동 생성 중... (처음 1~2분 소요)")
    try:
        subprocess.run(
            ["python", "scripts/etl.py", "--download", "--load"],
            check=True
        )
        st.success("✅ 데이터베이스 생성 완료! 앱을 다시 실행하세요.")
        st.stop()
    except Exception as e:
        st.error(f"DB 생성 실패: {e}")
        st.stop()



# app.py (핸들링 확장판)
from pathlib import Path
import pandas as pd
from pathlib import Path
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Olist E-Commerce Explorer (Pro)", layout="wide")

DB_PATH = Path("data/olist.sqlite")
if not DB_PATH.exists():
    st.warning("데이터베이스가 없습니다. 먼저 `python scripts/etl.py --download --load` 를 실행하세요.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# 캐시: 엔진/쿼리
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(f"sqlite:///{DB_PATH}", future=True)

@st.cache_data(ttl=3600, show_spinner=False)
def q(sql: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engine()
    with eng.begin() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})

# ─────────────────────────────────────────────────────────────────────────────
# 사이드바: 글로벌 필터 폼
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("🔧 글로벌 필터")

years_df = q("""
    SELECT DISTINCT strftime('%Y', order_purchase_timestamp) AS y
    FROM olist_orders_dataset
    WHERE order_purchase_timestamp IS NOT NULL
    ORDER BY 1
""")
years = years_df["y"].dropna().tolist()
if not years:
    years = ["2016", "2017", "2018"]

states_df = q("""
    SELECT DISTINCT customer_state AS st
    FROM olist_customers_dataset
    WHERE customer_state IS NOT NULL
    ORDER BY 1
""")
state_all = states_df["st"].dropna().tolist()

with st.sidebar.form("filters", clear_on_submit=False):
    y_from, y_to = st.select_slider("구매 연도 범위", options=years, value=(years[0], years[-1]))
    pick_states = st.multiselect("STATE(여러 개 선택 가능)", options=state_all, default=[])
    topn = st.slider("Top N 카테고리", 5, 50, 15, 5)
    chart_type = st.selectbox("월별 추이 차트 타입", options=["line", "bar"], index=0)
    logscale = st.checkbox("Y축 로그 스케일", value=False, help="분포가 크면 로그 스케일이 보기 좋음")
    sample_rows = st.number_input("표시용 샘플링(행)", min_value=0, value=0, step=1000, help="0이면 전체")
    show_sections = st.multiselect(
        "표시할 섹션", 
        ["KPI", "월별 추이", "Top 카테고리", "원시데이터 미리보기", "커스텀 SQL"],
        default=["KPI", "월별 추이", "Top 카테고리", "커스텀 SQL"]
    )
    apply = st.form_submit_button("적용")

# 첫 진입 보정
if "applied" not in st.session_state:
    st.session_state.applied = True
    apply = True if not apply else apply

# 공통 WHERE/파라미터
base_where = ["o.order_purchase_timestamp IS NOT NULL"]
params = {"yf": y_from, "yt": y_to}
base_where.append("strftime('%Y', o.order_purchase_timestamp) BETWEEN :yf AND :yt")

if pick_states:
    base_where.append("""
        o.customer_id IN (
            SELECT customer_id FROM olist_customers_dataset
            WHERE customer_state IN (:states)
        )
    """)
    # SQLAlchemy 텍스트 파라미터 확장용
    # 아래에서 :states를 IN (...)로 바꾸지 않고 executemany 스타일로 처리
    # 간단히 문자열로 구현:
    states_str = ",".join(f"'{s}'" for s in pick_states)
    base_where[-1] = base_where[-1].replace(":states", states_str)  # 안전: 목록은 UI선택값

where_sql = "WHERE " + " AND ".join(base_where)

# ─────────────────────────────────────────────────────────────────────────────
# KPI 섹션
# ─────────────────────────────────────────────────────────────────────────────
if "KPI" in show_sections:
    st.title("🛍️ Olist E-Commerce Explorer (Pro)")
    st.caption("Kaggle → SQLite → Streamlit, 폼 기반 리런 최소화 · SQL 집계 · 내보내기/디버깅 강화")

    kpi_sql = f"""
    SELECT
      (SELECT count(*) FROM olist_orders_dataset o {where_sql}) AS orders_cnt,
      (SELECT sum(p.payment_value)
       FROM olist_order_payments_dataset p
       JOIN olist_orders_dataset o USING(order_id) {where_sql}) AS pay_sum,
      (SELECT avg(cnt) FROM (
          SELECT count(*) AS cnt
          FROM olist_order_items_dataset i
          JOIN olist_orders_dataset o USING(order_id) {where_sql}
          GROUP BY o.order_id
      )) AS avg_items
    """
    k = q(kpi_sql, params=params).iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("주문 수", f"{int(k['orders_cnt'] or 0):,}")
    with c2: st.metric("총 결제액(원화 환산 아님)", f"{float(k['pay_sum'] or 0):,.2f}")
    with c3: st.metric("주문당 아이템 수(평균)", f"{float(k['avg_items'] or 0):.2f}")
    with c4:
        # 간단히 카테고리 수(제품군 다양성)
        cat_sql = f"""
        SELECT count(DISTINCT p.product_category_name) AS cats
        FROM olist_order_items_dataset i
        JOIN olist_orders_dataset o USING(order_id)
        JOIN olist_products_dataset p USING(product_id)
        {where_sql}
        """
        cats = int(q(cat_sql, params=params).iloc[0]["cats"] or 0)
        st.metric("카테고리 수(판매기록)", f"{cats:,}")

# ─────────────────────────────────────────────────────────────────────────────
# 월별 추이 (주문 수)
# ─────────────────────────────────────────────────────────────────────────────
if "월별 추이" in show_sections:
    trend_sql = f"""
    SELECT strftime('%Y-%m', o.order_purchase_timestamp) AS ym, count(*) AS orders
    FROM olist_orders_dataset o
    {where_sql}
    GROUP BY 1 ORDER BY 1
    """
    trend = q(trend_sql, params=params)
    st.subheader("📈 월별 주문 추이")
    if chart_type == "line":
        fig = px.line(trend, x="ym", y="orders")
    else:
        fig = px.bar(trend, x="ym", y="orders")
    if logscale:
        fig.update_yaxes(type="log")
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("월별 주문 CSV 다운로드", trend.to_csv(index=False).encode("utf-8"), "monthly_orders.csv")

# ─────────────────────────────────────────────────────────────────────────────
# Top 카테고리 (판매건수)
# ─────────────────────────────────────────────────────────────────────────────
if "Top 카테고리" in show_sections:
    top_sql = f"""
    SELECT p.product_category_name AS category, count(*) AS cnt
    FROM olist_order_items_dataset i
    JOIN olist_orders_dataset o USING(order_id)
    JOIN olist_products_dataset p USING(product_id)
    {where_sql}
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT {int(topn)}
    """
    top_df = q(top_sql, params=params)
    st.subheader(f"🏷️ Top {topn} 상품 카테고리(판매건수)")
    fig2 = px.bar(top_df, x="category", y="cnt")
    if logscale:
        fig2.update_yaxes(type="log")
    st.plotly_chart(fig2, use_container_width=True)
    st.download_button("Top 카테고리 CSV 다운로드", top_df.to_csv(index=False).encode("utf-8"), "top_categories.csv")

# ─────────────────────────────────────────────────────────────────────────────
# 원시데이터 미리보기(옵션, 성능 보호용 샘플링)
# ─────────────────────────────────────────────────────────────────────────────
if "원시데이터 미리보기" in show_sections:
    st.subheader("🧾 원시데이터 미리보기 (orders)")
    raw_sql = f"""
    SELECT o.order_id, o.customer_id, o.order_status,
           o.order_purchase_timestamp, o.order_approved_at,
           o.order_delivered_customer_date
    FROM olist_orders_dataset o
    {where_sql}
    ORDER BY o.order_purchase_timestamp
    """
    raw = q(raw_sql, params=params)
    if sample_rows and sample_rows > 0 and len(raw) > sample_rows:
        raw = raw.sample(sample_rows, random_state=42).sort_values("order_purchase_timestamp")
        st.caption(f"※ 전체 {len(raw):,} 중 {sample_rows:,}행 샘플 표시")
    st.dataframe(raw, use_container_width=True, height=360)
    st.download_button("주문 원시데이터 CSV", raw.to_csv(index=False).encode("utf-8"), "orders_raw.csv")

# ─────────────────────────────────────────────────────────────────────────────
# 커스텀 SQL (템플릿+실행+쿼리플랜)
# ─────────────────────────────────────────────────────────────────────────────
if "커스텀 SQL" in show_sections:
    st.subheader("🧪 커스텀 SQL 실행기")
    templates = {
        "상태별 주문 수": "SELECT order_status, COUNT(*) cnt FROM olist_orders_dataset o GROUP BY 1 ORDER BY 2 DESC",
        "월별 매출(결제합)": """
            SELECT strftime('%Y-%m', o.order_purchase_timestamp) AS ym,
                   SUM(p.payment_value) AS revenue
            FROM olist_orders_dataset o
            JOIN olist_order_payments_dataset p USING(order_id)
            GROUP BY 1 ORDER BY 1
        """,
        "카테고리별 평균 주문당 아이템수": """
            SELECT p.product_category_name,
                   AVG(s.cnt) AS avg_items_per_order
            FROM (
                SELECT o.order_id, COUNT(*) AS cnt
                FROM olist_order_items_dataset i
                JOIN olist_orders_dataset o USING(order_id)
                GROUP BY o.order_id
            ) s
            JOIN olist_order_items_dataset i USING(order_id)
            JOIN olist_products_dataset p USING(product_id)
            GROUP BY 1 ORDER BY 2 DESC
        """,
    }
    tpl = st.selectbox("템플릿 선택", options=list(templates.keys()), index=0)
    default_sql = templates[tpl]
    sql = st.text_area("SQL 입력", default_sql, height=200)

    col_run, col_plan, col_dl = st.columns([1,1,2])
    with col_run:
        run = st.button("실행")
    with col_plan:
        show_plan = st.button("쿼리 플랜(EXPLAIN)")
    with col_dl:
        csv_name = st.text_input("CSV 파일명", value="query_result.csv")

    if run:
        try:
            df = q(sql)
            if sample_rows and sample_rows > 0 and len(df) > sample_rows:
                df_view = df.sample(sample_rows, random_state=42)
                st.caption(f"※ 전체 {len(df):,} 중 {sample_rows:,}행 샘플 표시")
            else:
                df_view = df
            st.dataframe(df_view, use_container_width=True, height=420)
            st.download_button("결과 CSV 다운로드", df.to_csv(index=False).encode("utf-8"), file_name=csv_name)
        except Exception as e:
            st.error(str(e))

    if show_plan:
        try:
            plan = q("EXPLAIN QUERY PLAN " + sql)
            st.code(plan.to_string(index=False), language="sql")
        except Exception as e:
            st.error(str(e))
