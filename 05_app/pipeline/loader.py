"""
원본 CSV 적재 파일

주요 역할:
- 원본 CSV 파일을 읽어 raw 테이블에 적재
- 파일별 대상 테이블 매핑 관리
- 컬럼명 예외 처리 및 NULL 값 변환
- raw 레이어 전체 데이터 일괄 로드
"""

import pandas as pd
from pathlib import Path
from core.db import get_connection
from config.settings import DATA_DIR

FILE_TABLE_MAP = {
    "olist_orders_dataset.csv" : "olist_raw.orders",
    "olist_order_items_dataset.csv" : "olist_raw.order_items",
    "olist_order_payments_dataset.csv" : "olist_raw.order_payments",
    "olist_order_reviews_dataset.csv" : "olist_raw.order_reviews",
    "olist_customers_dataset.csv" : "olist_raw.customers",
    "olist_products_dataset.csv" : "olist_raw.products",
    "olist_sellers_dataset.csv" : "olist_raw.sellers",
    "olist_geolocation_dataset.csv" : "olist_raw.geolocation",
    "product_category_name_translation.csv" : "olist_raw.product_category_name_translation"
}

COLUMN_RENAME_MAP = {
    "olist_products_dataset.csv": {
        "product_name_lenght" : "product_name_length",
        "product_description_lenght" : "product_description_length"
    }
}

CHUNK_SIZE = 5000

def load_csv_to_table(csv_path: Path, table_name: str) -> None:
    df = pd.read_csv(csv_path, encoding="utf-8")

    if csv_path.name in COLUMN_RENAME_MAP:
        df = df.rename(columns=COLUMN_RENAME_MAP[csv_path.name])

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        columns = list(df.columns)
        column_str = ", ".join(f"`{col}`" for col in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = f"""
            INSERT INTO {table_name} ({column_str})
            VALUES ({placeholders})
        """

        rows = [
            tuple(None if pd.isna(value) else value for value in row)
            for row in df.itertuples(index=False, name=None)
        ]

        total_rows = len(rows)

        for start in range(0, total_rows, CHUNK_SIZE):
            end = start + CHUNK_SIZE
            batch = rows[start:end]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            print(f"Loaded batch {start:,} ~ {min(end, total_rows):,} into {table_name}")

        print(f"Loaded {csv_path.name} -> {table_name} ({len(rows)} rows)")
    
    except Exception:
        conn.rollback()
        print(f"Failed to load {csv_path.name} -> {table_name}")
        raise

    finally:
        cursor.close()
        conn.close()

def load_all_raw_data() -> None:
    for file_name, table_name in FILE_TABLE_MAP.items():
        csv_path = DATA_DIR / file_name

        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        print(f"\nStarting Load: {file_name}")
        load_csv_to_table(csv_path, table_name)