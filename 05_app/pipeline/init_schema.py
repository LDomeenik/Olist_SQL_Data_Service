"""
초기 SQL 스키마(데이터베이스) 생성 파일

주요 역할:
- 데이터베이스 기본 스키마 생성
- raw 레이어 테이블 구조 초기화
- 파이프라인 실행 전 DB 기본 환경 준비
"""

from core.sql_runner import run_sql_file
from config.settings import SQL_DIR

def init_schema() -> None:
    sql_path = SQL_DIR / "01_load" / "01_init_environment_schema_only.sql"
    run_sql_file(sql_path)
    print("Schema initialization completed successfully.")