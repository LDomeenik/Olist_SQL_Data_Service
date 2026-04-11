"""
대시보드 출력 데이터 로드 파일

주요 역할:
- output 폴더의 CSV 파일을 DataFrame으로 로드
- Streamlit 캐시를 통해 반복 로드 비용을 줄임
- 대시보드에서 사용하는 전체 데이터셋을 딕셔너리 형태로 제공
"""

import pandas as pd
import streamlit as st

from config.settings import OUTPUT_DIR


@st.cache_data
def load_csv(file_name: str) -> pd.DataFrame:
    file_path = OUTPUT_DIR / f"{file_name}.csv"

    if not file_path.exists():
        return pd.DataFrame()

    return pd.read_csv(file_path)


def load_all_datasets() -> dict[str, pd.DataFrame]:
    return {
        "growth_structure": load_csv("growth_structure"),
        "growth_drill_down": load_csv("growth_drill_down"),
        "customer_value_structure": load_csv("customer_value_structure"),
        "operational_stability": load_csv("operational_stability"),
    }