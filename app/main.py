import streamlit as st

from app.ui.data_loader import load_all_datasets
from app.ui.components.sidebar_controls import render_sidebar_controls
from app.ui.components.app_state import validate_datasets, render_empty_state

st.set_page_config(
    page_title="Olist Analytics Data Service",
    layout="wide"
)

st.title("Olist Analytics Data Service")
st.caption("이커머스 데이터 기반으로 성장, 고객 가치, 운영 안정성을 종합 분석하는 데이터 서비스")

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

# Page State
if "current_page" not in st.session_state:
    st.session_state["current_page"] = "Overview"

nav1, nav2, nav3, nav4, nav5 = st.columns(5)

with nav1:
    if st.button("Overview", use_container_width=True):
        st.session_state["current_page"] = "Overview"

with nav2:
    if st.button("Growth Structure", use_container_width=True):
        st.session_state["current_page"] = "Growth Structure"

with nav3:
    if st.button("Growth Drill Down", use_container_width=True):
        st.session_state["current_page"] = "Growth Drill Down"

with nav4:
    if st.button("Customer Value Structure", use_container_width=True):
        st.session_state["current_page"] = "Customer Value Structure"

with nav5:
    if st.button("Operational Stability", use_container_width=True):
        st.session_state["current_page"] = "Operational Stability"

st.divider()

# Page Router
page = st.session_state["current_page"]

if page == "Overview":
    from app.ui.overview import render_overview
    render_overview(growth_df, drill_df, customer_df, ops_df)
    st.stop()

elif page == "Growth Structure":
    from app.ui.growth_structure import render_growth_structure
    render_growth_structure(growth_df)
    st.stop()

elif page == "Growth Drill Down":
    from app.ui.growth_drill_down import render_growth_drill_down
    render_growth_drill_down(drill_df)
    st.stop()

elif page == "Customer Value Structure":
    from app.ui.customer_value_structure import render_customer_value
    render_customer_value(customer_df)
    st.stop()

elif page == "Operational Stability":
    from app.ui.operational_stability import render_operational_stability
    render_operational_stability(ops_df)
    st.stop()