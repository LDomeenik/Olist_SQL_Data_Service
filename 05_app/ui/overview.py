"""
대시보드 개요 페이지 파일

주요 역할:
- 핵심 KPI를 요약 카드 형태로 표시
- Growth / Operational Stability 핵심 추이를 한 화면에서 제공
- 주요 분석 모듈의 핵심 인사이트를 요약하여 보여줌
"""

import streamlit as st
import pandas as pd

from ui.components.kpi_cards import render_kpi_cards
from ui.components.insight_box import render_insight_box


def render_overview(
    growth_df: pd.DataFrame,
    drill_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    ops_df: pd.DataFrame,
) -> None:
    st.subheader("Overview")

    if growth_df.empty and drill_df.empty and customer_df.empty and ops_df.empty:
        st.warning("표시할 데이터가 없습니다. 먼저 파이프라인을 실행해 주세요.")
        return

    growth_df = growth_df.copy()
    customer_df = customer_df.copy()
    ops_df = ops_df.copy()

    if "year_month" in growth_df.columns:
        growth_df = growth_df.sort_values("year_month").reset_index(drop=True)

    if "year_month" in customer_df.columns:
        customer_df = customer_df.sort_values("year_month").reset_index(drop=True)

    monthly_ops_df = ops_df.copy()
    if "row_type" in ops_df.columns:
        monthly_ops_df = ops_df[ops_df["row_type"] == "monthly_kpi"].copy()

    if "year_month" in monthly_ops_df.columns:
        monthly_ops_df = monthly_ops_df.sort_values("year_month").reset_index(drop=True)

    # Customer monthly_value 분리
    monthly_customer_df = pd.DataFrame()
    if not customer_df.empty and "section_type" in customer_df.columns:
        monthly_customer_df = customer_df[customer_df["section_type"] == "monthly_value"].copy()
        if not monthly_customer_df.empty and "year_month" in monthly_customer_df.columns:
            monthly_customer_df = monthly_customer_df.sort_values("year_month").reset_index(drop=True)

    # Growth 기반 KPI
    total_revenue = (
        growth_df["gross_revenue"].sum()
        if "gross_revenue" in growth_df.columns else None
    )
    total_orders = (
        growth_df["order_cnt"].sum()
        if "order_cnt" in growth_df.columns else None
    )

    # Customer 기반 KPI
    total_buyers = (
        monthly_customer_df["buyers"].sum()
        if not monthly_customer_df.empty and "buyers" in monthly_customer_df.columns
        else None
    )

    # 파생 KPI
    aov = (
        total_revenue / total_orders
        if total_revenue not in [0, None] and total_orders not in [0, None]
        else None
    )
    arpb = (
        total_revenue / total_buyers
        if total_revenue not in [0, None] and total_buyers not in [0, None]
        else None
    )

    # Repeat Rate
    repeat_rate = None
    if not customer_df.empty and "section_type" in customer_df.columns:
        new_repeat_df = customer_df[customer_df["section_type"] == "new_repeat_share"].copy()

        if not new_repeat_df.empty and {"sub_type", "buyers"}.issubset(new_repeat_df.columns):
            repeat_buyers = new_repeat_df.loc[
                new_repeat_df["sub_type"] == "repeat", "buyers"
            ].sum()

            total_repeat_base_buyers = new_repeat_df["buyers"].sum()

            repeat_rate = (
                repeat_buyers / total_repeat_base_buyers
                if total_repeat_base_buyers not in [0, None] else None
            )

    # Failed Rate
    avg_failed_rate = None
    if not monthly_ops_df.empty and "failed_rate" in monthly_ops_df.columns:
        avg_failed_rate = monthly_ops_df["failed_rate"].mean()

    metrics = [
        {
            "label": "Revenue",
            "value": total_revenue,
            "type": "currency",
        },
        {
            "label": "Orders",
            "value": total_orders,
            "type": "integer",
        },
        {
            "label": "Buyers",
            "value": total_buyers,
            "type": "integer",
        },
        {
            "label": "AOV",
            "value": aov,
            "type": "currency",
        },
        {
            "label": "ARPB",
            "value": arpb,
            "type": "currency",
        },
        {
            "label": "Repeat Rate",
            "value": repeat_rate * 100 if repeat_rate is not None else None,
            "type": "percent",
        },
        {
            "label": "Avg Failed Rate",
            "value": avg_failed_rate * 100 if avg_failed_rate is not None else None,
            "type": "percent",
        },
    ]

    render_kpi_cards(metrics)

    render_insight_box(
        title="Executive Summary",
        message=(
            "플랫폼 매출은 고객 가치 상승보다 주문 수와 구매자 수 증가에 더 크게 반응합니다. "
            "재구매율은 낮은 수준으로, 매출 구조는 신규 고객 유입에 크게 의존하고 있습니다. "
            "또한 매출 급락 구간에서도 운영 지표 악화는 관찰되지 않아, 매출 감소의 주요 원인은 수요 감소로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Growth Structure")
        st.caption("거래량 중심 성장 여부와 주요 변동 구간을 확인합니다.")

        if not growth_df.empty and {"year_month", "gross_revenue"}.issubset(growth_df.columns):
            growth_chart_df = growth_df[["year_month", "gross_revenue"]].copy().set_index("year_month")
            st.line_chart(growth_chart_df, height=260)

    with col2:
        st.markdown("### Operational Stability")
        st.caption("취소율·실패율 추이로 운영 이슈 여부를 확인합니다.")

        if {"year_month", "failed_rate"}.issubset(monthly_ops_df.columns):
            ops_chart_df = monthly_ops_df[["year_month", "failed_rate"]].copy().set_index("year_month")
            st.line_chart(ops_chart_df, height=260)

    st.markdown("### Module Summary")

    col3, col4 = st.columns(2)

    with col3:
        render_insight_box(
            title="Growth Drill Down",
            message="급락 구간은 특정 카테고리 붕괴보다 신규 수요 축소의 영향이 더 큰 것으로 해석됩니다.",
            level="success",
        )

        render_insight_box(
            title="Customer Value Structure",
            message="ARPB와 구매 빈도는 비교적 안정적이며, 매출 성장은 신규 고객 유입 규모에 더 의존합니다.",
            level="success",
        )

    with col4:
        render_insight_box(
            title="Action Plan",
            message=(
                "단기적으로는 신규 유입 감소 원인을 파악하고, "
                "중장기적으로는 재구매율과 초기 리텐션을 높이는 방향이 중요합니다."
            ),
            level="warning",
        )