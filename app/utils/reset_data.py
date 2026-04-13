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
    데이터 초기화 대상 테이블 목록을 반환하는 함수

    반환 범위:
    - Raw 레이어
    - Staging 레이어
    - Data Mart 레이어
    - 필요 시 Analysis Module / BI 레이어

    주의:
    - 원본 CSV 파일이나 로컬 파일 경로는 절대 포함하지 않음
    """
    return RAW_TABLES + STG_TABLES + DM_TABLES + AM_TABLES + BI_TABLES


def reset_loaded_data(
    engine: sa.Engine,
    tables: Iterable[str] | None = None,
) -> dict:
    """
    연결된 데이터베이스의 적재 데이터를 초기화하는 함수

    동작:
    - 지정된 테이블 목록에 대해 TRUNCATE TABLE 실행
    - 테이블 구조(스키마)는 유지하고 데이터만 삭제
    - FOREIGN KEY 제약 조건은 실행 중 일시적으로 비활성화

    입력:
    - engine: SQLAlchemy DB 연결 엔진
    - tables: 초기화 대상 테이블 목록
        - None이면 기본 초기화 대상(get_reset_targets) 사용

    반환:
    {
        "success": bool,
        "cleared_tables": list[str],
        "failed_tables": list[dict],
        "message": str,
    }

    주의:
    - CSV / 원본 파일은 삭제하지 않음
    - DB 내부 적재 데이터만 초기화함
    - MySQL 기준으로 작성된 로직임
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

    # 데이터 초기화 실행
    with engine.begin() as conn:
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

    # 실행 결과 반환
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