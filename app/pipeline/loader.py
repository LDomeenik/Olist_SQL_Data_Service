"""
원본 CSV 적재 파일

주요 역할:
- 원본 CSV 파일을 읽어 SQLite raw 테이블에 적재
- 파일별 대상 테이블 매핑 관리
- 컬럼명 예외 처리
- raw 레이어 전체 데이터 일괄 로드
"""

from pathlib import Path

import pandas as pd

from app.config.settings import RAW_DATA_DIR
from app.core.db import get_connection


# 원본 파일명과 raw 테이블명 매핑
FILE_TABLE_MAP = {
    "olist_orders_dataset.csv": "raw_orders",
    "olist_order_items_dataset.csv": "raw_order_items",
    "olist_order_payments_dataset.csv": "raw_order_payments",
    "olist_order_reviews_dataset.csv": "raw_order_reviews",
    "olist_customers_dataset.csv": "raw_customers",
    "olist_products_dataset.csv": "raw_products",
    "olist_sellers_dataset.csv": "raw_sellers",
    "olist_geolocation_dataset.csv": "raw_geolocation",
    "product_category_name_translation.csv": "raw_product_category_name_translation",
}

# 원본 CSV 컬럼명 예외 처리
COLUMN_RENAME_MAP = {
    "olist_products_dataset.csv": {
        "product_name_lenght": "product_name_length",
        "product_description_lenght": "product_description_length",
    }
}


def load_csv_to_table(csv_path: Path, table_name: str) -> None:
    """
    단일 CSV 파일을 읽어 SQLite raw 테이블에 적재합니다.
    """
    df = pd.read_csv(csv_path, encoding="utf-8", low_memory=False)

    if csv_path.name in COLUMN_RENAME_MAP:
        df = df.rename(columns=COLUMN_RENAME_MAP[csv_path.name])

    conn = get_connection()

    try:
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="replace",
            index=False,
        )
        conn.commit()
        print(f"Loaded {csv_path.name} -> {table_name} ({len(df):,} rows)")

    except Exception:
        conn.rollback()
        print(f"Failed to load {csv_path.name} -> {table_name}")
        raise

    finally:
        conn.close()


def load_all_raw_data() -> None:
    """
    RAW_DATA_DIR 내 원본 CSV를 순차적으로 읽어 SQLite raw 테이블에 적재합니다.
    """
    for file_name, table_name in FILE_TABLE_MAP.items():
        csv_path = RAW_DATA_DIR / file_name

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        print(f"\nStarting Load: {file_name}")
        load_csv_to_table(csv_path, table_name)