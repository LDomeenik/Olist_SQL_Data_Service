"""
SQLite 실행 환경 초기화 파일

주요 역할:
- 기존 SQLite DB 초기화
- 파이프라인 실행 전 기본 실행 환경 준비
"""


from app.core.db import reset_database, test_connection


# SQLite DB 초기화
def init_environment() -> None:
    """
    SQLite DB를 초기화하고 연결 가능 상태를 확인합니다.
    """
    reset_database()

    if not test_connection():
        raise RuntimeError("SQLite initialization failed.")
    
    print("SQLite 환경이 정상적으로 초기화되었습니다.")