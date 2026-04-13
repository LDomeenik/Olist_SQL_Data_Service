"""
분석 결과 CSV 내보내기 파일

주요 역할:
- BI View 조회 결과를 pandas DataFrame으로 로드
- 결과 데이터를 CSV 파일로 저장
- 대시보드용 전체 결과 파일 일괄 export
"""


import pandas as pd

from app.config.settings import OUTPUT_DIR
from app.core.db import get_connection


# BI View 조회 쿼리 정의
EXPORT_QUERIES = {
    "growth_structure" : "SELECT * FROM vw_growth_structure",
    "growth_drill_down" : "SELECT * FROM vw_growth_drill_down",
    "customer_value_structure" : "SELECT * FROM vw_customer_value_structure",
    "operational_stability" : "SELECT * FROM vw_operational_stability"
}

# 단일 CSV export
def export_query_to_csv(file_name: str, query: str) -> None:
    """
    단일 BI View 조회 결과를 CSV 파일로 저장합니다.
    """
    output_path = OUTPUT_DIR / f"{file_name}.csv"
    conn = get_connection()

    try:
        df = pd.read_sql_query(query, conn)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Exported: {output_path.name} ({len(df):,} rows)")
    
    except Exception:
        print(f"Failed to export: {file_name}")
        raise

    finally:
        conn.close()

# 전체 BI 결과 export
def export_all_results() -> None:
    """
    전체 BI View 결과를 CSV 파일로 일괄 저장합니다.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for file_name, query in EXPORT_QUERIES.items():
        print(f"\nStarting Export: {file_name}")
        export_query_to_csv(file_name, query)
    
    print("\n모든 BI 결과 CSV export가 완료되었습니다.")