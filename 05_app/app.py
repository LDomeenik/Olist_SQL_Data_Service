"""
Streamlit 메인 애플리케이션 파일

주요 역할:
- 전체 대시보드 UI 구성 및 페이지 라우팅
- 사이드바 제어 패널 렌더링
- 데이터 로딩 및 상태 검증
- 각 분석 모듈(Overview, Growth, Customer, Ops) 연결
"""

import streamlit as st

from ui.data_loader import load_all_datasets
from ui.overview import render_overview
from ui.growth_structure import render_growth_structure
from ui.growth_drill_down import render_growth_drill_down
from ui.customer_value_structure import render_customer_value
from ui.operational_stability import render_operational_stability
from ui.components.sidebar_controls import render_sidebar_controls
from ui.components.app_state import validate_datasets, render_empty_state


# 페이지 설정
st.set_page_config(
    page_title="Olist Analytics Data Service",
    layout="wide"
)

# 페이지 제목
st.title("Olist Analytics Data Service")
st.caption("이커머스 데이터 기반으로 성장, 고객 가치, 운영 안정성을 종합 분석하는 데이터 서비스")

# Sidebar
render_sidebar_controls()

# pipeline 결과가 준비된 경우에만 데이터 로드
if not st.session_state.get("pipeline_ready", False):
    datasets = {}
else:
    try:
        datasets = load_all_datasets()
    except Exception as e:
        st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {e}")
        datasets = {}

is_ready, missing_keys = validate_datasets(datasets)

if not is_ready:
    render_empty_state(missing_keys)
    st.stop()

growth_df = datasets["growth_structure"]
drill_df = datasets["growth_drill_down"]
customer_df = datasets["customer_value_structure"]
ops_df = datasets["operational_stability"]

# tab 구성
tab0, tab1, tab2, tab3, tab4 = st.tabs([
    "Overview",
    "Growth Structure",
    "Growth Drill Down",
    "Customer Value Structure",
    "Operational Stability"
])

with tab0:
    render_overview(growth_df, drill_df, customer_df, ops_df)

with tab1:
    render_growth_structure(growth_df)

with tab2:
    render_growth_drill_down(drill_df)

with tab3:
    render_customer_value(customer_df)

with tab4:
    render_operational_stability(ops_df)