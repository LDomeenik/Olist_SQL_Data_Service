"""
대시보드 출력 데이터 로드 파일

주요 역할:
- output 폴더의 CSV 파일을 DataFrame으로 로드
- Streamlit 캐시를 통해 반복 로드 비용을 줄임
- 대시보드에서 사용하는 전체 데이터셋을 딕셔너리 형태로 제공
"""

import time
import pandas as pd
import streamlit as st

from app.config.settings import OUTPUT_DIR


def _get_file_mtime(file_path):
    if not file_path.exists():
        return None
    return file_path.stat().st_mtime


@st.cache_data
def load_csv(file_name: str, mtime: float | None) -> pd.DataFrame:
    """
    CSV 파일을 안전하게 로드하는 함수

    Parameters:
    - file_name: 파일 이름
    - mtime: 파일 수정 시간 (캐시 무효화용)
    """
    file_path = OUTPUT_DIR / f"{file_name}.csv"

    if not file_path.exists():
        return pd.DataFrame()

    last_error = None

    for _ in range(3):
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            last_error = e
            time.sleep(0.2)

    raise RuntimeError(f"CSV 로드 실패: {file_path} | {last_error}")


def load_all_datasets() -> dict[str, pd.DataFrame]:
    """
    전체 CSV 데이터를 로드하는 함수
    """

    def load(name):
        path = OUTPUT_DIR / f"{name}.csv"
        mtime = _get_file_mtime(path)
        return load_csv(name, mtime)

    return {
        "growth_structure": load("growth_structure"),
        "growth_drill_down": load("growth_drill_down"),
        "customer_value_structure": load("customer_value_structure"),
        "operational_stability": load("operational_stability"),
    }