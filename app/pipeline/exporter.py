"""
BI 결과 CSV export 모듈

주요 역할:
- SQLite BI 결과를 CSV 파일로 export
- Streamlit 앱에서 사용할 최종 출력 데이터 생성

설계 원칙:
- 원자적 파일 저장(Atomic Write)을 적용하여 파일 쓰기 중간 상태를 방지
- Streamlit rerun 시 CSV 읽기와 충돌하지 않도록 안정성 확보
"""


from pathlib import Path
import pandas as pd

from app.core.db import get_connection
from app.config.settings import OUTPUT_DIR



# 단일 쿼리 → CSV export
def export_query_to_csv(query: str, output_name: str) -> None:
    """
    SQL 쿼리 결과를 CSV 파일로 export합니다.

    Parameters:
    - query: 실행할 SQL 쿼리
    - output_name: 저장할 CSV 파일 이름 (확장자 제외)

    특징:
    - tmp 파일에 먼저 저장 후 replace()로 교체 (Atomic Write)
    - 파일 쓰기 중간 상태를 방지하여 Streamlit 충돌 방지
    """

    print(f"Exporting {output_name}.csv ...")

    conn = get_connection()

    try:
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    output_path = OUTPUT_DIR / f"{output_name}.csv"
    tmp_path = output_path.with_suffix(".tmp")
    
    df.to_csv(tmp_path, index=False, encoding="utf-8-sig")
    
    tmp_path.replace(output_path)
    
    if not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(f"Export failed or empty file: {output_path}")

    print(f"Exported → {output_path}")



# 전체 BI 결과 export 실행
def export_all_results() -> None:
    """
    BI 레이어 결과를 CSV로 export합니다.

    Export 대상:
    - growth_structure
    - growth_drill_down
    - customer_value_structure
    - operational_stability

    특징:
    - 각 결과는 독립적으로 export
    - 실패 시 해당 단계에서 중단
    """

    print("\n[Export] BI result CSV files")

    # Growth Structure
    export_query_to_csv(
        query="""
        SELECT *
        FROM vw_growth_structure
        """,
        output_name="growth_structure",
    )

    # Growth Drill Down    
    export_query_to_csv(
        query="""
        SELECT *
        FROM vw_growth_drill_down
        """,
        output_name="growth_drill_down",
    )

    # Customer Value Structure    
    export_query_to_csv(
        query="""
        SELECT *
        FROM vw_customer_value_structure
        """,
        output_name="customer_value_structure",
    )

    # Operational Stability    
    export_query_to_csv(
        query="""
        SELECT *
        FROM vw_operational_stability
        """,
        output_name="operational_stability",
    )

    print("\nAll BI result CSV files exported successfully.")