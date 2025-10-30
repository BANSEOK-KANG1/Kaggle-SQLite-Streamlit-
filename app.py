# app.py — All-in-one (Kaggle → SQLite → Streamlit)
# - 첫 실행 시: Kaggle에서 데이터 자동 다운로드 → SQLite 적재 → 인덱스/뷰 생성
# - 이후: 대시보드 렌더링
# 배포 전 필수: Streamlit Cloud Secrets에 아래 저장
# [kaggle]
# username = "YOUR_KAGGLE_USERNAME"
# key = "YOUR_KAGGLE_KEY"

from pathlib import Path
import os, json
import sqlite3
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from kaggle.api.kaggle_api_extended import KaggleApi

# ─────────────────────────────────────────────────────────────────────────────
# 0) 기본 설정
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Olist E-Commerce Explorer (All-in-One)", layout="wide")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "olist.sqlite"

DATASET_SLUG = "olistbr/brazilian-ecommerce"
CSV_FILES = [
    "olist_customers_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

# ─────────────────────────────────────────────────────────────────────────────
# 1) Kaggle 자격증명 로딩 (st.secrets → ENV → ~/.kaggle/kaggle.json)
# ─────────────────────────────────────────────────────────────────────────────
def load_kaggle_credentials() -> tuple[str, str]:
    user = key = ""
    try:
        user = st.secrets.get("kaggle", {}).get("username", "")
        key  = st.secrets.get("kaggle", {}).get("key", "")
    except Exception:
        pass
    user = user or os.getenv("KAGGLE_USERNAME", "")
    key  = key  or os.getenv("KAGGLE_KEY", "")
    if not (user and key):
        cfg = Path.home() / ".kaggle" / "kaggle.json"
        if cfg.exists():
            with cfg.open() as f:
                data = json.load(f)
                user = user or data.get("username", "")
                key  = key  or data.get("key", "")
    return user, key

# ─────────────────────────────────────────────────────────────────────────────
# 2) ETL 유틸
# ─────────────────────────────────────────────────────────────────────────────
def kaggle_download_unzip():
    """Kaggle Python API로 데이터셋 다운로드 및 자동 압축해제."""
    user, key = load_kaggle_credentials()
    if not (user and key):
        raise RuntimeError(
            "Kaggle API 자격증명이 없습니다. "
            "Streamlit Secrets([kaggle] username/key) 또는 환경변수/ ~/.kaggle/kaggle.json 설정이 필요합니다."
        )
    os.environ["KAGGLE_USERNAME"] = user
    os.environ["KAGGLE_KEY"] = key
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_SLUG, path=str(DATA_DIR), unzip=True)
    # 최소 CSV 몇 개가 실제로 생겼는지 점검
    has_any = any((DATA_DIR / name).exists() for name in CSV_FILES)
    if not has_any:
        raise FileNotFoundError("Kaggle 다운로드 후 CSV 파일을 찾지 못했습니다. 네트워크/권한을 확인하세요.")

def load_to_sqlite():
    """CSV → SQLite 적재(replace) + 성능 PRAGMA."""
    missing = [n for n in CSV_FILES if not (DATA_DIR / n).exists()]
    if missing:
        raise FileNotFoundError(
            "다음 CSV가 없습니다. Kaggle 다운로드가 실패했을 가능성이 큽니다:\n  - " + "\n  - ".join(missing)
        )

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA temp_store=MEMORY;
        """
    )
    con.commit()

    for name in CSV_FILES:
        df = pd.read_csv(DATA_DIR / name)
        table = name.replace(".csv", "")
        df.to_sql(table, con, if_exists="replace", index=False)
        print(f"{table}: {len(df):,} rows 적재")

    con.commit()
    con.close()

def _table_exists(con, name: str) -> bool:
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def create_indexes():
    """조회 성능 향상을 위한 인덱스."""
    con = sqlite3.connect(DB_PATH)
    required = [
        "olist_orders_dataset",
        "olist_order_items_dataset",
        "olist_order_payments_dataset",
        "olist_customers_dataset",
        "olist_products_dataset",
    ]
    missing = [t for t in required if not _table_exists(con, t)]
    if missing:
        con.close()
        raise RuntimeError("인덱스 생성 실패: 테이블이 없습니다 → " + ", ".join(missing))

    cur = con.cursor()
    cur.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_orders_ts
            ON olist_orders_dataset(order_purchase_timestamp);
        CREATE INDEX IF NOT EXISTS idx_orders_id
            ON olist_orders_dataset(order_id);
        CREATE INDEX IF NOT EXISTS idx_items_order
            ON olist_order_items_dataset(order_id);
        CREATE INDEX IF NOT EXISTS idx_items_product
            ON olist_order_items_dataset(product_id);
        CREATE INDEX IF NOT EXISTS idx_pay_order
            ON olist_order_payments_dataset(order_id);
        CREATE INDEX IF NOT EXISTS idx_cust_id_state
            ON olist_customers_dataset(customer_id, customer_state);
        ANALYZE;
        """
    )
    con.commit()
    con.close()

def create_views():
    """분석용 뷰 생성: 결제합·리드타임·RFM."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.executescript(
        """
        -- 주문별 결제 합계
        CREATE VIEW IF NOT EXISTS vw_order_payment_sum AS
        SELECT p.order_id, SUM(p.payment_value) AS payment_total
        FROM olist_order_payments_dataset p
        GROUP BY p.order_id;

        -- 구매~배송 리드타임(일)
        CREATE VIEW IF NOT EXISTS vw_order_lead_time AS
        SELECT
          o.order_id,
          o.customer_id,
          o.order_purchase_timestamp,
          o.order_delivered_customer_date,
          CAST(
            (julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp))
            AS REAL
          ) AS lead_time_days
        FROM olist_orders_dataset o
        WHERE o.order_delivered_customer_date IS NOT NULL
          AND o.order_purchase_timestamp IS NOT NULL;

        -- RFM 기본 집계(고객별 Recency/Frequency/Monetary)
        CREATE VIEW IF NOT EXISTS vw_rfm_base AS
        WITH last_date AS (
          SELECT MAX(order_delivered_customer_date) AS max_delivered
          FROM olist_orders_dataset
          WHERE order_delivered_customer_date IS NOT NULL
        ),
        order_money AS (
          SELECT
            o.order_id,
            o.customer_id,
            o.order_delivered_customer_date,
            COALESCE(s.payment_total, 0) AS monetary
          FROM olist_orders_dataset o
          LEFT JOIN vw_order_payment_sum s USING(order_id)
          WHERE o.order_delivered_customer_date IS NOT NULL
        )
        SELECT
          m.customer_id,
          CAST(julianday(l.max_delivered) - julianday(MAX(m.order_delivered_customer_date)) AS INTEGER) AS recency_days,
          COUNT(DISTINCT m.order_id) AS frequency,
          SUM(m.monetary) AS monetary
        FROM order_money m
        CROSS JOIN last_date l
        GROUP BY m.customer_id;
        """
    )
    con.commit()
    con.close()

# ─────────────────────────────────────────────────────────────────────────────
# 3) 최초 실행 시 자동 ETL(동일 프로세스에서 수행)
# ─────────────────────────────────────────────────────────────────────────────
if not DB_PATH.exists():
    with st.status("⚙️ 데이터베이스가 없습니다. 자동 생성 중… (최초 1~2분)", expanded=True) as s:
        try:
            st.write("1) Kaggle에서 데이터 다운로드 및 압축해제…")
            kaggle_download_unzip()
            st.write("2) CSV → SQLite 적재…")
            load_to_sqlite()
            st.write("3) 인덱스 생성…")
            create_indexes()
            st.write("4) 분석용 뷰 생성…")
            create_views()
            s.update(label="✅ 데이터베이스 생성 완료. 상단 Rerun 버튼으로 다시 실행하세요.", state="complete")
        except Exception as e:
            s.update(label="❌ DB 생성 실패", state="error")
            st.error(str(e))
        st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# 4) 쿼리 캐시/엔진
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
# 5) 사이드바 필터
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("🔧 글로벌 필터")
years_df = q("""
    SELECT DISTINCT strftime('%Y', order_purchase_timestamp) AS y
    FROM olist_orders_dataset
    WHERE order_purchase_timestamp IS NOT NULL
    ORDER BY 1
""")
years = years_df["y"].dropna().tolist() or ["2016", "2017", "2018"]

states_df = q("""
    SELECT DISTINCT customer_state AS st
    FROM olist_customers_dataset
    WHERE customer_state IS NOT NULL
    ORDER BY 1
""")
all_states = states_df["st"].dropna().tolist()

with st.sidebar.form("filters", clear_on_submit=False):
    y_from, y_to = st.select_slider("구매 연도 범위", options=years, value=(years[0], years[-1]))
    pick_states = st.multiselect("STATE(다중)", options=all_states, default=[])
    topn = st.slider("Top N 카테고리", 5, 50, 15, 5)
    chart_type = st.selectbox("월별 차트", options=["line", "bar"], index=0)
    logscale = st.checkbox("Y축 로그 스케일", value=False)
    sample_rows = st.number_input("표시 샘플링(행) — 0은 전체", min_value=0, value=0, step=1000)
    show_sections = st.multiselect(
        "표시 섹션",
        ["KPI", "월별 추이", "Top 카테고리", "원시데이터 미리보기", "커스텀 SQL"],
        default=["KPI", "월별 추이", "Top 카테고리", "커스텀 SQL"],
    )
    apply = st.form_submit_button("적용")

if "applied" not in st.session_state:
    st.session_state.applied = True
    apply = True if not apply else apply

base_where = ["o.order_purchase_timestamp IS NOT NULL",
              "strftime('%Y', o.order_purchase_timestamp) BETWEEN :yf AND :yt"]
params = {"yf": y_from, "yt": y_to}
if pick_states:
    states_str = ",".join(f"'{s}'" for s in pick_states)
    base_where.append(f"""
        o.customer_id IN (
            SELECT customer_id FROM olist_customers_dataset
            WHERE customer_state IN ({states_str})
        )
    """)
where_sql = "WHERE " + " AND ".join(base_where)

# ─────────────────────────────────────────────────────────────────────────────
# 6) 메인 화면
# ─────────────────────────────────────────────────────────────────────────────
st.title("🛍️ Olist E-Commerce Explorer (All-in-One)")
st.caption("Kaggle → SQLite → Streamlit | 최초 실행 자동 ETL · 캐시 · 커스텀 SQL · CSV 내보내기")

# KPI
if "KPI" in show_sections:
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
        cats_sql = f"""
        SELECT count(DISTINCT p.product_category_name) AS cats
        FROM olist_order_items_dataset i
        JOIN olist_orders_dataset o USING(order_id)
        JOIN olist_products_dataset p USING(product_id)
        {where_sql}
        """
        cats = int(q(cats_sql, params=params).iloc[0]["cats"] or 0)
        st.metric("카테고리 수(판매기록)", f"{cats:,}")

# 월별 추이
if "월별 추이" in show_sections:
    trend_sql = f"""
    SELECT strftime('%Y-%m', o.order_purchase_timestamp) AS ym, count(*) AS orders
    FROM olist_orders_dataset o
    {where_sql}
    GROUP BY 1 ORDER BY 1
    """
    trend = q(trend_sql, params=params)
    st.subheader("📈 월별 주문 추이")
    fig = px.line(trend, x="ym", y="orders") if chart_type == "line" else px.bar(trend, x="ym", y="orders")
    if logscale:
        fig.update_yaxes(type="log")
    st.plotly_chart(fig, use_container_width=True)
    st.download_button("월별 주문 CSV", trend.to_csv(index=False).encode("utf-8"), "monthly_orders.csv")

# Top 카테고리
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
    st.download_button("Top 카테고리 CSV", top_df.to_csv(index=False).encode("utf-8"), "top_categories.csv")

# 원시데이터 미리보기
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
    view = raw
    if sample_rows and sample_rows > 0 and len(raw) > sample_rows:
        view = raw.sample(sample_rows, random_state=42).sort_values("order_purchase_timestamp")
        st.caption(f"※ 전체 {len(raw):,}행 중 {sample_rows:,}행 샘플 표시")
    st.dataframe(view, use_container_width=True, height=360)
    st.download_button("주문 원시데이터 CSV", raw.to_csv(index=False).encode("utf-8"), "orders_raw.csv")

# 커스텀 SQL
if "커스텀 SQL" in show_sections:
    st.subheader("🧪 커스텀 SQL 실행기")
    templates = {
        "상태별 주문 수": "SELECT order_status, COUNT(*) cnt FROM olist_orders_dataset GROUP BY 1 ORDER BY 2 DESC",
        "월별 매출(결제합)": """
            SELECT strftime('%Y-%m', o.order_purchase_timestamp) AS ym,
                   SUM(p.payment_value) AS revenue
            FROM olist_orders_dataset o
            JOIN olist_order_payments_dataset p USING(order_id)
            GROUP BY 1 ORDER BY 1
        """,
        "카테고리별 주문당 평균 아이템수": """
            WITH per_order AS (
                SELECT o.order_id, COUNT(*) AS cnt
                FROM olist_order_items_dataset i
                JOIN olist_orders_dataset o USING(order_id)
                GROUP BY o.order_id
            )
            SELECT p.product_category_name, AVG(per_order.cnt) AS avg_items_per_order
            FROM per_order
            JOIN olist_order_items_dataset i USING(order_id)
            JOIN olist_products_dataset p USING(product_id)
            GROUP BY 1 ORDER BY 2 DESC
        """,
    }
    tpl = st.selectbox("템플릿 선택", options=list(templates.keys()), index=0)
    default_sql = templates[tpl]
    sql = st.text_area("SQL 입력", default_sql, height=200)

    col_run, col_plan, col_dl = st.columns([1, 1, 2])
    with col_run:
        run = st.button("실행")
    with col_plan:
        show_plan = st.button("쿼리 플랜(EXPLAIN)")
    with col_dl:
        csv_name = st.text_input("CSV 파일명", value="query_result.csv")

    if run:
        try:
            df = q(sql)
            df_view = df
            if sample_rows and sample_rows > 0 and len(df) > sample_rows:
                df_view = df.sample(sample_rows, random_state=42)
                st.caption(f"※ 전체 {len(df):,}행 중 {sample_rows:,}행 샘플 표시")
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

# 푸터
st.caption("© 2025 Olist Demo · Streamlit · SQLite · Kaggle · by Banseok")
