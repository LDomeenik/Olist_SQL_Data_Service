"""
SQL 실행 유틸리티

주요 역할:
- SQL 파일 실행
- SQLite 파이프라인 SQL 실행 관리

안정성 개선:
- executescript()와 수동 BEGIN/COMMIT 충돌 방지
- rollback 가능할 때만 rollback 수행
- 원래 SQL 에러를 가리지 않도록 처리
"""

from pathlib import Path
import time

from app.core.db import get_connection


def run_sql_file(file_path: Path) -> None:
    """
    단일 SQL 파일을 실행합니다.
    """

    sql_script = file_path.read_text(encoding="utf-8")
    conn = get_connection()

    start_time = time.time()

    try:
        # executescript()는 자체적으로 여러 SQL 문을 처리하므로
        # 여기서 수동 BEGIN/COMMIT을 강제로 감싸지 않는다.
        conn.executescript(sql_script)

        elapsed = time.time() - start_time
        print(f"Executed: {file_path.name} ({elapsed:.2f}s)")

    except Exception as e:
        # rollback은 실제 트랜잭션이 살아 있을 때만 시도
        try:
            if conn.in_transaction:
                conn.rollback()
        except Exception as rollback_error:
            print(f"⚠️ Rollback failed in {file_path.name}: {rollback_error}")

        print(f"❌ Error in {file_path.name}: {e}")
        raise

    finally:
        conn.close()


def run_sql_folder(folder_path: Path) -> None:
    """
    폴더 내 SQL 파일들을 순차 실행합니다.
    """

    sql_files = sorted(folder_path.glob("*.sql"))

    if not sql_files:
        print(f"No SQL files found in {folder_path}")
        return

    print(f"\nRunning SQL folder: {folder_path.name}")

    for file in sql_files:
        run_sql_file(file)