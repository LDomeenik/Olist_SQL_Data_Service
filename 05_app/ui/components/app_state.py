"""
앱 상태 및 빈 화면 표시 파일

주요 역할:
- 대시보드 데이터셋 준비 여부를 확인
- 필요한 데이터가 없을 때 초기 안내 화면을 표시
- 사용자가 앱 실행 흐름을 이해할 수 있도록 시작 가이드를 제공
"""

import streamlit as st

REQUIRED_DATASET_KEYS = [
    "growth_structure",
    "growth_drill_down",
    "customer_value_structure",
    "operational_stability",
]


def validate_datasets(datasets: dict) -> tuple[bool, list[str]]:
    missing = []

    for key in REQUIRED_DATASET_KEYS:
        if key not in datasets:
            missing.append(key)
            continue

        df = datasets[key]
        if df is None or getattr(df, "empty", False):
            missing.append(key)

    return len(missing) == 0, missing


def render_empty_state(missing_keys: list[str]) -> None:
    st.info("아직 대시보드에 필요한 출력 데이터가 준비되지 않았습니다.")

    with st.container(border=True):
        st.markdown("#### 시작 방법")
        st.markdown(
            """
1. 사이드바의 **Data Import**에서 Raw CSV 파일을 업로드합니다.  
2. **Import Raw Data** 버튼으로 데이터를 저장합니다.  
3. **Run Data Pipeline** 버튼을 눌러 출력 데이터를 생성합니다.  
4. 완료 후 대시보드가 자동으로 다시 로드됩니다.
"""
        )

    if missing_keys:
        with st.expander("누락된 데이터셋 보기", expanded=False):
            for key in missing_keys:
                st.write(f"❌ {key}")