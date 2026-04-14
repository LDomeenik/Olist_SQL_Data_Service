"""
메인 애플리케이션 엔트리 파일

주요 역할:
- Streamlit 기반 데이터 분석 대시보드의 전체 실행 흐름을 제어
- 데이터 로딩 → 상태 검증 → 페이지 라우팅까지 전 과정 관리
- 상단 네비게이션과 페이지 전환 로직 담당

페이지 구성:
- Overview: 전체 KPI 및 구조 요약
- Growth Structure: 매출 변화 구조 분석
- Growth Drill Down: 매출 급락 원인 분해
- Customer Value Structure: 고객 가치 및 리텐션 분석
- Operational Stability: 운영 안정성 진단
"""


import streamlit as st

from app.ui.data_loader import load_all_datasets
from app.ui.components.sidebar_controls import render_sidebar_controls
from app.ui.components.app_state import validate_datasets, render_empty_state


# Page Config
st.set_page_config(
    page_title="Olist Analytics Data Service",
    layout="wide",
)


# App Header
st.title("Olist Analytics Data Service")
st.caption(
        "이커머스 데이터 기반으로 성장, 고객 가치, 운영 안정성을 종합 분석하는 데이터 서비스\n\n"
        "추천 탐색 순서: Overview → Growth Structure → Growth Drill Down → "
        "Customer Value Structure → Operational Stability"
    )


# Sidebar
render_sidebar_controls()


# Data Load
datasets = load_all_datasets()

if not datasets:
    st.info("좌측 사이드바에서 Raw 데이터를 업로드하고 파이프라인을 실행하세요.")
    st.stop()

is_ready, missing_keys = validate_datasets(datasets)

if not is_ready:
    render_empty_state(missing_keys)
    st.stop()


# Data Mapping
growth_df = datasets.get("growth_structure")
drill_df = datasets.get("growth_drill_down")
customer_df = datasets.get("customer_value_structure")
ops_df = datasets.get("operational_stability")


# Page Registry
PAGE_CONFIG = {
    "Overview": {
        "description": "핵심 KPI와 전체 구조를 빠르게 요약해서 확인합니다.",
    },
    "Growth Structure": {
        "description": "매출 변화가 거래량 요인인지 고객 가치 요인인지 구조적으로 확인합니다.",
    },
    "Growth Drill Down": {
        "description": "매출 급락 구간의 원인을 신규/재구매, 카테고리, 지역 관점에서 분해합니다.",
    },
    "Customer Value Structure": {
        "description": "고객 가치, 재구매 구조, 리텐션 관점에서 매출 구조를 진단합니다.",
    },
    "Operational Stability": {
        "description": "취소율, unavailable 비율, 실패율을 통해 운영 안정성을 진단합니다.",
    },
}


# Session State
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Overview"


def set_current_page(page_name: str) -> None:
    st.session_state["current_page"] = page_name


current_page = st.session_state["current_page"]


# Top Navigation Buttons
page_names = list(PAGE_CONFIG.keys())
nav_cols = st.columns([0.8, 1.2, 1.4, 1.6, 1.4])

for col, page_name in zip(nav_cols, page_names):
    button_type = "primary" if st.session_state["current_page"] == page_name else "secondary"

    with col:
        st.button(
            page_name,
            use_container_width=True,
            type=button_type,
            on_click=set_current_page,
            args=(page_name,),
        )

st.divider()


# Page Router
page = st.session_state["current_page"]

if page == "Overview":
    from app.ui.overview import render_overview
    render_overview(growth_df, drill_df, customer_df, ops_df)
    st.stop()

if page == "Growth Structure":
    from app.ui.growth_structure import render_growth_structure
    render_growth_structure(growth_df)
    st.stop()

if page == "Growth Drill Down":
    from app.ui.growth_drill_down import render_growth_drill_down
    render_growth_drill_down(drill_df)
    st.stop()

if page == "Customer Value Structure":
    from app.ui.customer_value_structure import render_customer_value
    render_customer_value(customer_df)
    st.stop()

if page == "Operational Stability":
    from app.ui.operational_stability import render_operational_stability
    render_operational_stability(ops_df)
    st.stop()

st.warning("페이지를 불러올 수 없습니다.")