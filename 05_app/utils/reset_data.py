"""
데이터 초기화 유틸리티 파일

주요 역할:
- Streamlit 앱에서 사용하는 DB 데이터 초기화
- Raw / Staging / Data Mart 계층 테이블 데이터 삭제
- 테이블 구조(스키마)는 유지하고 데이터만 TRUNCATE 수행
- 원본 CSV 파일 및 로컬 파일은 절대 삭제하지 않음

주의사항:
- FOREIGN KEY 제약 조건을 비활성화한 상태에서 TRUNCATE 수행
- 운영 환경에서는 사용 시 주의 필요
"""

from __future__ import annotations

from typing import Iterable
import sqlalchemy as sa


RAW_TABLES: list[str] = [
    "olist_raw.orders",
    "olist_raw.order_items",
    "olist_raw.order_payments",
    "olist_raw.order_reviews",
    "olist_raw.customers",
    "olist_raw.products",
    "olist_raw.sellers",
    "olist_raw.geolocation",
    "olist_raw.product_category_name_translation",
]

STG_TABLES: list[str] = [
    "olist_stg.stg_orders",
    "olist_stg.stg_order_items",
    "olist_stg.stg_order_payments",
    "olist_stg.stg_order_reviews",
    "olist_stg.stg_customers",
    "olist_stg.stg_products",
    "olist_stg.stg_sellers",
    "olist_stg.stg_geolocation",
]

DM_TABLES: list[str] = [
    "olist_dm.fact_orders",
    "olist_dm.fact_order_items",
    "olist_dm.dim_customer",
    "olist_dm.dim_product",
    "olist_dm.dim_seller",
    "olist_dm.dim_date",
    "olist_dm.dim_geolocation",
]


AM_TABLES: list[str] = []
BI_TABLES: list[str] = []


def get_reset_targets() -> list[str]:
    """
    초기화 대상 테이블 목록 반환.
    원본 파일(CSV 등)은 절대 포함하지 않음.
    """
    return RAW_TABLES + STG_TABLES + DM_TABLES + AM_TABLES + BI_TABLES


def reset_loaded_data(engine: sa.Engine, tables: Iterable[str] | None = None) -> dict:
    """
    Streamlit 앱에 연결된 DB 데이터만 초기화한다.
    - CSV / 원본 파일은 삭제하지 않음
    - 스키마/테이블 구조는 유지
    - 데이터만 비움

    Parameters
    ----------
    engine : sqlalchemy.Engine
        DB 연결 엔진
    tables : Iterable[str] | None
        초기화 대상 테이블 목록. None이면 기본 목록 사용.

    Returns
    -------
    dict
        실행 결과 요약
    """
    target_tables = list(tables) if tables is not None else get_reset_targets()

    if not target_tables:
        return {
            "success": True,
            "cleared_tables": [],
            "failed_tables": [],
            "message": "초기화 대상 테이블이 없습니다.",
        }

    cleared_tables: list[str] = []
    failed_tables: list[dict] = []

    with engine.begin() as conn:
        # MySQL 기준
        conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 0"))
        try:
            for table_name in target_tables:
                try:
                    conn.execute(sa.text(f"TRUNCATE TABLE {table_name}"))
                    cleared_tables.append(table_name)
                except Exception as exc:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "error": str(exc),
                        }
                    )
        finally:
            conn.execute(sa.text("SET FOREIGN_KEY_CHECKS = 1"))

    return {
        "success": len(failed_tables) == 0,
        "cleared_tables": cleared_tables,
        "failed_tables": failed_tables,
        "message": (
            "모든 연결 데이터가 초기화되었습니다."
            if len(failed_tables) == 0
            else "일부 테이블 초기화에 실패했습니다."
        ),
    }