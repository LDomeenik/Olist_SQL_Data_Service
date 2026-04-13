"""
대시보드 출력 데이터 로드 파일

주요 역할:
- output 폴더의 CSV 파일을 DataFrame으로 로드
- Streamlit 캐시를 통해 반복 로드 비용을 줄임
- 대시보드에서 사용하는 전체 데이터셋을 딕셔너리 형태로 제공
"""


import pandas as pd
import streamlit as st

from app.config.settings import OUTPUT_DIR


# 캐시 지정
@st.cache_data

# 단일 CSV 로드
def load_csv(file_name: str) -> pd.DataFrame:
    """
    단일 CSV 파일을 DataFrame으로 로드하는 함수

    - OUTPUT_DIR 기준으로 파일 경로 생성
    - 파일이 없을 경우 빈 DataFrame 반환
    - Streamlit 캐시를 통해 반복 로딩 비용 최소화
    """
    file_path = OUTPUT_DIR / f"{file_name}.csv"

    if not file_path.exists():
        return pd.DataFrame()
    
    return pd.read_csv(file_path)

# 전체 데이터셋 로드
def load_all_datasets() -> dict[str, pd.DataFrame]:
    """
    대시보드에서 사용하는 전체 데이터셋을 로드하는 함수

    - 각 CSV 파일을 개별적으로 로드하여 하나의 dict로 반환
    - app.py에서 전체 데이터 흐름 제어에 사용
    """
    return {
        "growth_structure" : load_csv("growth_structure"),
        "growth_drill_down" : load_csv("growth_drill_down"),
        "customer_value_structure" : load_csv("customer_value_structure"),
        "operational_stability" : load_csv("operational_stability")
    }