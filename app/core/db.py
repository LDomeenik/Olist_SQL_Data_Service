"""
SQLite DB 연결 및 관리 모듈

주요 역할:
- SQLite DB connection 생성
- 안정성 설정 (WAL, timeout 등)
- DB 초기화 (수동 실행 전용)
"""

import sqlite3

from app.config.settings import SQLITE_DB_PATH


def get_connection() -> sqlite3.Connection:
    """
    SQLite DB connection을 생성합니다.

    특징:
    - timeout 설정으로 lock 대기 가능
    - WAL 모드로 읽기/쓰기 동시 처리 가능
    - busy_timeout으로 lock 시 재시도
    """
    conn = sqlite3.connect(
        SQLITE_DB_PATH,
        timeout=30,
    )

    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")

    return conn


def reset_database() -> None:
    """
    SQLite DB 파일을 삭제하여 초기화합니다.

    주의:
    - 모든 connection이 닫힌 상태에서 실행해야 합니다.
    - 일반 pipeline 실행에서는 자동 호출하지 않습니다.
    """
    if SQLITE_DB_PATH.exists():
        SQLITE_DB_PATH.unlink()


def database_exists() -> bool:
    """
    SQLite DB 파일 존재 여부를 반환합니다.
    """
    return SQLITE_DB_PATH.exists()


def test_connection() -> bool:
    """
    SQLite 연결이 정상적으로 가능한지 확인합니다.
    """
    try:
        conn = get_connection()
        conn.execute("SELECT 1;")
        conn.close()
        return True
    except Exception as e:
        print(f"DB 연결 실패: {e}")
        return False