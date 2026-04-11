"""
분석 결과 CSV 내보내기 파일

주요 역할:
- BI View 조회 결과를 pandas DataFrame으로 로드
- 결과 데이터를 CSV 파일로 저장
- 대시보드용 전체 결과 파일 일괄 export
"""

import pandas as pd
from sqlalchemy import create_engine

from config.settings import DB_URL, OUTPUT_DIR

EXPORT_QUERIES = {
    "growth_structure" : "SELECT * FROM olist_bi.vw_growth_structure",
    "growth_drill_down" : "SELECT * FROM olist_bi.vw_growth_drill_down",
    "customer_value_structure" : "SELECT * FROM olist_bi.vw_customer_value_structure",
    "operational_stability" : "SELECT * FROM olist_bi.vw_operational_stability"
}

def export_query_to_csv(file_name: str, query: str) -> None:
    output_path = OUTPUT_DIR / f"{file_name}.csv"
    engine = create_engine(DB_URL)

    try:
        df = pd.read_sql(query, engine)
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"Exported: {output_path.name} ({len(df)} rows)")
    
    except Exception:
        print(f"Failed to export: {file_name}")
        raise
    
    finally:
        engine.dispose()

def export_all_results() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for file_name, query in EXPORT_QUERIES.items():
        export_query_to_csv(file_name, query)