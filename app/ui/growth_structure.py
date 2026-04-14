"""
성장 구조 페이지 파일

주요 역할:
- 월별 핵심 KPI를 요약 카드 형태로 표시
- 매출 추이와 주요 드라이버(Order, Buyers, AOV, ARPB)를 시각화
- MoM 증감률을 통해 성장 및 급락 구간을 진단
"""

import pandas as pd
import streamlit as st

from app.ui.components.kpi_cards import render_kpi_cards, apply_kpi_metric_style
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


def _prepare_chart_df(
    df: pd.DataFrame,
    x_col: str,
    y_cols: list[str],
) -> pd.DataFrame | None:
    """
    차트용 DataFrame 최소 전처리
    """
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
        "매출 변화가 거래량 요인인지 고객 가치 요인인지 구조적으로 확인합니다."
    )

    apply_kpi_metric_style()

    if growth_df is None or growth_df.empty:
        st.warning("growth_structure.csv 파일이 없습니다.")
        return

    # KPI 계산
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

    st.caption(
        "요약 지표 기준으로 보면, 매출은 AOV보다 주문 수와 활성 구매자 수 변화에 더 크게 반응하며\n\n"
        "현재 성장 구조는 고객 가치 확장보다 거래량 확대에 더 의존합니다."
    )

    render_insight_box(
        title="Key Insight",
        message=(
            "매출 변화는 고객 가치(AOV)보다 주문 수와 활성 구매자 수 변화에 더 강하게 반응합니다.\n\n"
            "특히 특정 구간에서는 AOV가 안정적인 상태에서도 매출이 크게 감소하는 현상이 관찰되며,\n\n"
            "이는 매출 구조가 고객 소비 수준이 아닌 고객 수(트래픽)에 의해 결정되는 구조임을 의미합니다.\n\n"
            "즉, 현재 플랫폼의 성장 구조는 고객 가치 확장보다 유입 기반의 거래량 증가에 의존하고 있습니다."
        ),
        level="info",
    )

    st.divider()

    # Gross Revenue Trend
    revenue_chart_df = _prepare_chart_df(
        growth_df,
        x_col="year_month",
        y_cols=["gross_revenue"],
    )

    if revenue_chart_df is not None:
        st.markdown("#### Gross Revenue Trend")
        st.caption(
            "월별 매출 추이를 통해 성장, 정체, 급락 구간이 언제 발생하는지 확인합니다.\n\n"
            "이 차트는 전체 성장 흐름의 방향성과 변곡점을 빠르게 파악하기 위한 기준 차트입니다."
        )
        st.line_chart(revenue_chart_df, use_container_width=True)

    # Revenue Driver Trend
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
                "매출 변화가 거래량 감소에 의해 발생했는지, 고객 가치 변화에 의해 발생했는지 구분합니다."
            )
            st.line_chart(driver_chart_df, use_container_width=True)

    # MoM Growth Trend
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
                "월별 증감률(MoM)을 통해 어떤 KPI 변화가 매출 급등과 급락 구간을 직접적으로 설명하는지 확인합니다.\n\n"
                "특히 급락 구간에서 어떤 지표가 먼저, 크게 흔들리는지를 보는 데 유용합니다."
            )
            st.line_chart(mom_chart_df, use_container_width=True)

    # Raw Data
    with st.expander("원본 데이터 보기"):
        st.dataframe(growth_df.head(100), use_container_width=True)