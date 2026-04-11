"""
데이터베이스 연결 유틸리티 파일

주요 역할:
- settings.py의 DB 설정값을 사용해 MySQL 연결 객체 생성
"""

import mysql.connector
from config.settings import DB_HOST, DB_USER, DB_PASSWORD, DB_PORT

def get_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
        allow_local_infile=True,
        use_pure=True
    )