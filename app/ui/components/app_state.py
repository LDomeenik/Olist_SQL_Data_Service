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
    """
    대시보드 필수 데이터셋이 모두 준비되었는지 확인하는 함수

    반환:
    - bool: 모든 데이터셋 준비 여부
    - list[str]: 누락되었거나 비어 있는 데이터셋 키 목록

    검증 기준:
    - dict에 필수 key가 존재해야 함
    - 해당 value가 None이 아니어야 함
    - DataFrame이 비어 있지 않아야 함
    """
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
    """
    대시보드 출력 데이터가 아직 준비되지 않았을 때 안내 화면을 렌더링하는 함수

    표시 내용:
    - 현재 데이터가 준비되지 않았다는 안내 메시지
    - 앱 사용 시작 순서
    - 누락된 데이터셋 목록
    """
    st.info("아직 대시보드에 필요한 출력 데이터가 준비되지 않았습니다.")

    with st.container(border=True):
        st.markdown("#### 시작 방법")
        st.markdown(
            """
1. 사이드바의 **Data Import**에서 Raw CSV 파일을 업로드합니다.  
2. **Import Raw Data** 버튼으로 데이터를 저장합니다.  
3. **Run Data Pipeline** 버튼을 눌러 출력 데이터를 생성합니다.  
4. 완료 후 Refresh 버튼을 눌럭 대시보드를 로드됩니다.
"""
        )

    if missing_keys:
        with st.expander("누락된 데이터셋 보기", expanded=False):
            for key in missing_keys:
                st.write(f"❌ {key}")