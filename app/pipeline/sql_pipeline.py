"""
SQL 파이프라인 실행 파일

주요 역할:
- SQLite 레이버려 SQL 파일을 순차 실행
- 데이터 레이어별 실행 순서 관리
- 전체 SQL 기반 데이터 파이프라인 orchestration
"""


from app.config.settings import SQLITE_SQL_DIR
from app.core.sql_runner import run_sql_file


# 전체 파이프라인 실행
def run_sql_pipeline() -> None:
    """
    SQLite 기반 전체 SQL 파이프라인을 실행합니다.

    실행 순서:
    1. Staging Layer
    2. Data Mart Layer
    3. Analysis Module Layer
    4. Analysis Layer
    5. BI Layer

    각 단계는 이전 단계의 결과를 의존하므로 반드시 순서를 유지해야 합니다.
    """
    sqlite_sql_files = [
        SQLITE_SQL_DIR / "02_staging.sql",
        SQLITE_SQL_DIR / "03_datamart.sql",
        SQLITE_SQL_DIR / "04_analysismodule.sql",
        SQLITE_SQL_DIR / "05_bi.sql"
    ]

    for sql_file in sqlite_sql_files:
        if not sql_file.exists():
            raise FileNotFoundError(f"SQL 파일이 없습니다: {sql_file}")
        
        run_sql_file(sql_file)
    
    print("모든 SQL 파이프라인이 정상적으로 실행되었습니다.")


