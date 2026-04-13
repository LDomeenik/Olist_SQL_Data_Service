"""
SQL 실행 유틸리티 파일

주요 역할:
- 단일 SQL 파일 실행
- SQLite script 실행
- 실행 중 발생한 오류에 대해 rollback 처리
"""


from pathlib import Path

from app.core.db import get_connection


# SQL 파일 실행(단일)
def run_sql_file(sql_path: Path) -> None:
    """
    단일 SQL 파일을 실행합니다.
    """
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()
    
    conn = get_connection()

    try:
        conn.executescript(sql_script)
        conn.commit()
        print(f"Executed SQL file: {sql_path.name}")

    except Exception:
        conn.rollback()
        print(f"Failed while executing: {sql_path.name}")
        print("---- Failed Script Preview ----")
        print(sql_script[:1000])
        print("---- End Preview ----")
        raise

    finally:
        conn.close()