"""
데이터베이스 연결 유틸리티 파일

주요 역할:
- settings.py의 SQLite 경로를 사용해 연결 객체 생성
- SQLite DB 초기화 및 연결 상태 확인 지원
"""


import sqlite3
from pathlib import Path

from app.config.settings import SQLITE_DB_PATH


# SQLite 연결 생성
def get_connection() -> sqlite3.Connection:
    """
    SQLite 데이터베이스 연결 객체를 반환합니다.
    """
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

# 데이터베이스 초기화
def reset_database() -> None:
    """
    기존 SQLite DB 파일을 삭제하여 초기화합니다.
    """
    db_path = Path(SQLITE_DB_PATH)

    if db_path.exists():
        db_path.unlink()

# 연결 테스트
def test_connection() -> bool:
    """
    SQLite 연결이 정상적으로 되는지 확인합니다.
    """
    try:
        conn = get_connection()
        conn.execute("SELECT 1;")
        conn.close()
        return True
    
    except Exception as e:
        print(f"DB 연결 실패: {e}")
        return False