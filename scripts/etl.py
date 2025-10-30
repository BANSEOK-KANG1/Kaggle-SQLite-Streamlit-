# scripts/etl.py
import argparse
import os
import zipfile
from pathlib import Path
import pandas as pd

# ── Kaggle 자격증명 읽기(st.secrets → env fallback)
try:
    import streamlit as st
    KAGGLE_USER = st.secrets.get("kaggle", {}).get("username", os.getenv("KAGGLE_USERNAME", ""))
    KAGGLE_KEY = st.secrets.get("kaggle", {}).get("key", os.getenv("KAGGLE_KEY", ""))
except Exception:
    KAGGLE_USER = os.getenv("KAGGLE_USERNAME", "")
    KAGGLE_KEY = os.getenv("KAGGLE_KEY", "")

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True, parents=True)

DATASET_SLUG = "olistbr/brazilian-ecommerce"
ZIP_NAME = "olist.zip"
SQLITE_PATH = DATA_DIR / "olist.sqlite"

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

def kaggle_download():
    """Kaggle API로 데이터셋 다운로드 및 압축해제"""
    assert KAGGLE_USER and KAGGLE_KEY, "Kaggle API 자격증명이 없습니다. secrets.toml 또는 환경변수 설정 필요."
    os.environ["KAGGLE_USERNAME"] = KAGGLE_USER
    os.environ["KAGGLE_KEY"] = KAGGLE_KEY
    zip_path = DATA_DIR / ZIP_NAME
    if not zip_path.exists():
        os.system(f'kaggle datasets download -d {DATASET_SLUG} -p "{DATA_DIR}" -o')
    for zf in DATA_DIR.glob("*.zip"):
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(DATA_DIR)
    print("Kaggle 다운로드 및 압축해제 완료.")

def load_to_sqlite():
    """CSV → SQLite 적재(replace) + 성능 PRAGMA"""
    import sqlite3
    con = sqlite3.connect(SQLITE_PATH)
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
        csv_path = DATA_DIR / name
        if csv_path.exists():
            df = pd.read_csv(csv_path)
            table = name.replace(".csv", "")
            df.to_sql(table, con, if_exists="replace", index=False)
            print(f"{table}: {len(df):,} rows 적재")
        else:
            print(f"경고: {name} 없음")

    con.commit()
    con.close()
    print(f"SQLite 적재 완료 → {SQLITE_PATH}")

def create_indexes():
    """조회 성능 향상을 위한 인덱스 생성"""
    import sqlite3
    con = sqlite3.connect(SQLITE_PATH)
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
    print("인덱스 생성 완료.")

def create_views():
    """분석용 뷰 생성: 결제합·리드타임·RFM 집계 기반"""
    import sqlite3
    con = sqlite3.connect(SQLITE_PATH)
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

        -- RFM 기본 집계(고객별 Recency/Frequency/Monetary 원천)
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
    print("분석용 뷰(vw_*) 생성 완료.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true", help="Kaggle에서 데이터 다운로드")
    ap.add_argument("--load", action="store_true", help="CSV를 SQLite로 적재")
    ap.add_argument("--indexes-only", action="store_true", help="인덱스만 생성")
    ap.add_argument("--views-only", action="store_true", help="분석용 뷰만 생성")
    args = ap.parse_args()

    # 실행부: (정의보다 항상 아래에 위치)
    if args.download:
        kaggle_download()
    if args.load:
        load_to_sqlite()
        create_indexes()
        create_views()
    if args.indexes_only:
        create_indexes()
    if args.views_only:
        create_views()
