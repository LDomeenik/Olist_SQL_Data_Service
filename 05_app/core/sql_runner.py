"""
SQL 실행 유틸리티 파일

주요 역할:
- 단일 SQL 파일 실행
- 폴더 내 SQL 파일 순차 실행
- 다중 SQL 문 분리 및 실행
- 실행 중 발생한 오류에 대해 rollback 처리
"""

from pathlib import Path
import sqlparse
from core.db import get_connection

def run_sql_file(sql_path: Path):
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_script = f.read()

    statements = [
        stmt.strip()
        for stmt in sqlparse.split(sql_script)
        if stmt.strip()
    ]

    conn = get_connection()
    cursor = conn.cursor(buffered=True)
    current_stmt = None

    try:
        for stmt in statements:
            current_stmt = stmt
            cursor.execute(stmt)

            if cursor.with_rows:
                cursor.fetchall()
            
            while cursor.nextset():
                if cursor.with_rows:
                    cursor.fetchall()
        
        conn.commit()
        print(f"Executed SQL file: {sql_path.name}")
    
    except Exception:
        conn.rollback()
        print(f"Failed while executing: {sql_path.name}")
        print("---- Failed Statement Preview ----")
        if current_stmt:
            print(current_stmt[:1000])
        print("---- End Preview ----")
        raise
    
    finally:
        cursor.close()
        conn.close()

def run_sql_folder(folder_path: Path):
    sql_files = sorted(folder_path.glob("*.sql"))

    print(f"\nRunning Folder: {folder_path.name}")

    if not sql_files:
        print("No SQL files found.")
        return

    for sql_file in sql_files:
        run_sql_file(sql_file)