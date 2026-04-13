"""
SQLite 실행 환경 초기화 파일

주요 역할:
- DB가 없는 경우 초기화
- 연결 상태 확인
"""

from app.core.db import reset_database, test_connection, database_exists


def init_environment(force_reset: bool = False) -> None:
    """
    SQLite 환경을 초기화합니다.

    Parameters:
    - force_reset: True일 경우 DB를 강제 초기화
    """

    # DB가 없을 때만 초기화
    if force_reset or not database_exists():
        print("SQLite DB 초기화 중...")
        reset_database()
    else:
        print("SQLite DB 이미 존재 → 초기화 생략")

    # 연결 테스트
    if not test_connection():
        raise RuntimeError("SQLite initialization failed.")

    print("SQLite 환경이 정상적으로 준비되었습니다.")