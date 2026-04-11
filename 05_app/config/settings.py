"""
프로젝트 전역 설정 파일

주요 역할:
- 프로젝트 루트 및 주요 디렉토리 경로 정의
- .env 환경 변수 로드
- 데이터베이스 연결 설정 관리
"""

import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.engine import URL


BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "00_data" / "01_raw"
SQL_DIR = BASE_DIR / "04_sql"
OUTPUT_DIR = BASE_DIR / "outputs"
APP_DIR = BASE_DIR / "05_app"

load_dotenv(BASE_DIR / ".env")

DB_DRIVER = os.getenv("DB_DRIVER", "mysql+pymysql")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_CHARSET = os.getenv("DB_CHARSET", "utf8mb4")

DB_URL = URL.create(
    drivername=DB_DRIVER,
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    query={"charset": DB_CHARSET}
)