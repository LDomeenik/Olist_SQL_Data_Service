"""
SQL 파이프라인 실행 파일

주요 역할:
- staging / datamart / analysismodule / analysis / bi SQL 폴더를 순차 실행
- 데이터 레이어별 SQL 실행 순서 관리
- 전체 SQL 기반 데이터 파이프라인 orchestration
"""

from config.settings import SQL_DIR
from core.sql_runner import run_sql_folder

def run_sql_pipeline() -> None:
    sql_folders = [
        SQL_DIR / "02_staging",
        SQL_DIR / "03_datamart",
        SQL_DIR / "04_analysismodule",
        SQL_DIR / "05_analysis",
        SQL_DIR / "06_bi"
    ]

    for folder_path in sql_folders:
        if not folder_path.exists():
            raise FileNotFoundError(f"SQL folder not found: {folder_path}")
        
        run_sql_folder(folder_path)

    print("All SQL pipeline executed successfully.")