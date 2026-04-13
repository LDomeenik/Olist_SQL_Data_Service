"""
프로젝트 전역 설정 파일

주요 역할:
- 프로젝트 루트 및 주요 디렉토리 경로 정의
- SQLite 기반 실행 환경 설정
- 데이터 및 결과 경로 관리
"""


from pathlib import Path


# 프로젝트 기본 경로
BASE_DIR = Path(__file__).resolve().parent.parent.parent
# app 폴더 경로
APP_DIR = BASE_DIR / "app"
# 입력 데이터 경로
DATA_ROOT_DIR = BASE_DIR / "00_data"
# SQL 파일 경로
SQL_DIR = BASE_DIR / "04_sql"
SQLITE_SQL_DIR = SQL_DIR / "07_sqlite"
# 출력 데이터 저장 경로
OUTPUT_DIR = BASE_DIR / "outputs"

# 데이터 디렉토리
RAW_DATA_DIR = DATA_ROOT_DIR / "01_raw"
STAGING_DATA_DIR = DATA_ROOT_DIR / "02_staging_data"
DASHBOARD_DATA_DIR = DATA_ROOT_DIR / "03_dashboard_data"

# SQLite 설정
SQLITE_DB_PATH = OUTPUT_DIR / "olist.sqlite"

# 디렉토리 자동 생성(디렉토리가 없을 시 자동 생성되게 설정)
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
STAGING_DATA_DIR.mkdir(parents=True, exist_ok=True)
DASHBOARD_DATA_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)