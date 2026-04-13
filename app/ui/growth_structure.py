"""
성장 구조 페이지 파일

주요 역할:
- 월별 핵심 KPI를 요약 카드 형태로 표시
- 매출 추이와 주요 드라이버(Order, Buyers, AOV, ARPB)를 시각화
- MoM 증감률을 통해 성장 및 급락 구간을 진단
"""


import pandas as pd
import streamlit as st

from app.ui.components.kpi_cards import render_kpi_cards
from app.ui.components.insight_box import render_insight_box


def _safe_sum(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None
    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None
    return s.sum()


def _safe_ratio(numerator, denominator):
    if numerator is None or denominator in [None, 0]:
        return None
    return numerator / denominator


def _prepare_chart_df(df: pd.DataFrame, x_col: str, y_cols: list[str]) -> pd.DataFrame | None:
    required = {x_col, *y_cols}
    if df is None or df.empty or not required.issubset(df.columns):
        return None

    chart_df = df[[x_col] + y_cols].copy()

    for col in y_cols:
        chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce")
        chart_df[col] = chart_df[col].replace([float("inf"), float("-inf")], pd.NA).fillna(0)

    chart_df = chart_df.set_index(x_col)
    chart_df.index = chart_df.index.astype(str)
    return chart_df


def render_growth_structure(growth_df: pd.DataFrame) -> None:
    st.subheader("Growth Structure")
    st.caption(
        "월별 핵심 KPI 추이를 통해 매출 성장과 급락 구간을 식별하고, "
        "매출 변화 상태를 확인합니다."
    )

    if growth_df is None or growth_df.empty:
        st.warning("growth_structure.csv 파일이 없습니다.")
        return

    total_revenue = _safe_sum(growth_df, "gross_revenue")
    total_orders = _safe_sum(growth_df, "order_cnt")
    total_buyers = _safe_sum(growth_df, "active_buyers")

    aov = _safe_ratio(total_revenue, total_orders)
    arpb = _safe_ratio(total_revenue, total_buyers)

    metrics = [
        {"label": "Revenue", "value": total_revenue, "type": "currency"},
        {"label": "Orders", "value": total_orders, "type": "integer"},
        {"label": "Active Buyers", "value": total_buyers, "type": "integer"},
        {"label": "AOV", "value": aov, "type": "currency"},
        {"label": "ARPB", "value": arpb, "type": "currency"},
    ]

    render_kpi_cards(metrics)

    render_insight_box(
        title="Key Insight",
        message=(
            "월별 매출 흐름은 AOV보다 주문 수와 활성 구매자 수 변화와 더 강하게 동행합니다.\n\n "
            "2017년의 고성장은 고객 1인당 가치 상승이 아니라 구매자 기반 확장에 의해 만들어졌으며,\n\n "
            "2017-12 급락 역시 가격 하락이 아닌 거래량 감소가 직접적인 원인입니다.\n\n "
            "즉, 본 플랫폼은 value-driven 구조가 아닌 volume-driven 성장 구조를 보입니다."
        ),
        level="info",
    )

    st.divider()

    revenue_chart_df = _prepare_chart_df(
        growth_df,
        x_col="year_month",
        y_cols=["gross_revenue"],
    )
    if revenue_chart_df is not None:
        st.markdown("#### Gross Revenue Trend")
        st.caption(
            "월별 매출 추이를 통해 성장, 정체, 급락 구간이 언제 발생하는지 확인합니다."
        )
        st.line_chart(revenue_chart_df, use_container_width=True)


    driver_cols = [
        col for col in ["order_cnt", "active_buyers", "aov", "arpb"]
        if col in growth_df.columns
    ]
    if driver_cols:
        driver_chart_df = _prepare_chart_df(
            growth_df,
            x_col="year_month",
            y_cols=driver_cols,
        )
        if driver_chart_df is not None:
            st.markdown("#### Revenue Driver Trend")
            st.caption(
                "주문 수, 활성 구매자 수, AOV, ARPB를 함께 비교하여 "
                "매출 변화가 거래량 요인인지 고객 가치 요인인지 확인합니다."
            )
            st.line_chart(driver_chart_df, use_container_width=True)

    mom_cols = [
        col for col in [
            "mom_gross_revenue",
            "mom_order_cnt",
            "mom_active_buyers",
            "mom_aov",
            "mom_arpb",
        ]
        if col in growth_df.columns
    ]
    if mom_cols:
        mom_chart_df = _prepare_chart_df(
            growth_df,
            x_col="year_month",
            y_cols=mom_cols,
        )
        if mom_chart_df is not None:
            st.markdown("#### MoM Growth Trend")
            st.caption(
                "월별 증감률(MoM)을 통해 어떤 KPI가 매출 급등과 급락 구간을 직접 설명하는지 확인합니다."
            )
            st.line_chart(mom_chart_df, use_container_width=True)

    with st.expander("원본 데이터 보기"):
        st.dataframe(growth_df.head(100), use_container_width=True)