"""
고객 가치 구조 페이지 파일

주요 역할:
- 고객 가치 관련 핵심 KPI를 요약 카드 형태로 표시
- 고객 가치 추이, 신규/재구매 구조, 매출 집중도, 코호트 리텐션을 시각화
- 매출이 고객 가치 상승보다 신규 고객 유입에 의존하는지 진단
"""

import altair as alt
import pandas as pd
import numpy as np
import streamlit as st

from app.ui.components.kpi_cards import render_kpi_cards
from app.ui.components.insight_box import render_insight_box


def render_customer_value(customer_df: pd.DataFrame) -> None:
    """
    Customer Value Structure 페이지 전체를 렌더링하는 함수
    """
    st.subheader("Customer Value Structure")
    st.caption(
        "매출이 고객 가치 상승보다 신규 고객 유입에 의해 발생하는지 확인하기 위해 "
        "고객 가치 수준, 신규/재구매 구조, 고객 분포, 코호트 리텐션을 종합적으로 분석합니다."
    )

    if customer_df.empty:
        st.warning("customer_value_structure.csv 파일이 없습니다.")
        return

    # 데이터 복사 및 정렬
    df = customer_df.copy()

    if "year_month" in df.columns:
        df = df.sort_values("year_month").reset_index(drop=True)

    # 전체 데이터 안정화
    df = df.replace([np.inf, -np.inf], None)

    # 섹션 분리
    monthly_df = (
        df[df["section_type"] == "monthly_value"].copy()
        if "section_type" in df.columns
        else pd.DataFrame()
    )
    new_repeat_df = (
        df[df["section_type"] == "new_repeat_share"].copy()
        if "section_type" in df.columns
        else pd.DataFrame()
    )
    decile_df = (
        df[df["section_type"] == "decile_share"].copy()
        if "section_type" in df.columns
        else pd.DataFrame()
    )
    cohort_df = (
        df[df["section_type"] == "cohort_retention"].copy()
        if "section_type" in df.columns
        else pd.DataFrame()
    )

    if monthly_df.empty:
        st.warning("monthly_value 데이터가 없습니다.")
        return

    if "year_month" in monthly_df.columns:
        monthly_df = monthly_df.sort_values("year_month").reset_index(drop=True)

    # 핵심 KPI 계산
    total_revenue = (
        monthly_df["gross_revenue"].sum()
        if "gross_revenue" in monthly_df.columns
        else None
    )
    total_buyers = (
        monthly_df["buyers"].sum()
        if "buyers" in monthly_df.columns
        else None
    )
    total_orders = (
        monthly_df["order_cnt"].sum()
        if "order_cnt" in monthly_df.columns
        else None
    )

    aov = (
        total_revenue / total_orders
        if total_orders not in [0, None]
        else None
    )
    arpb = (
        total_revenue / total_buyers
        if total_buyers not in [0, None]
        else None
    )

    # Repeat Rate 계산
    repeat_rate = None
    if not new_repeat_df.empty and {"sub_type", "buyers"}.issubset(new_repeat_df.columns):
        repeat_buyers = new_repeat_df.loc[
            new_repeat_df["sub_type"] == "repeat",
            "buyers",
        ].sum()

        total_repeat_base_buyers = new_repeat_df["buyers"].sum()

        repeat_rate = (
            repeat_buyers / total_repeat_base_buyers
            if total_repeat_base_buyers not in [0, None]
            else None
        )

    # Month 1 Retention 계산
    month1_retention = None
    if not cohort_df.empty and {"month_n", "retention_rate"}.issubset(cohort_df.columns):
        m1 = cohort_df[cohort_df["month_n"] == 1]
        if not m1.empty:
            month1_retention = m1["retention_rate"].mean()

    # Top 10% Revenue Share 계산
    top10_share = None
    if not decile_df.empty and {"sub_type", "revenue_share"}.issubset(decile_df.columns):
        top10_df = decile_df[decile_df["sub_type"] == "decile_1"]
        if not top10_df.empty:
            top10_share = top10_df["revenue_share"].mean()

    # KPI 카드
    metrics = [
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
            "label": "Month 1 Retention",
            "value": month1_retention * 100 if month1_retention is not None else None,
            "type": "percent",
        },
        {
            "label": "Top 10% Share",
            "value": top10_share * 100 if top10_share is not None else None,
            "type": "percent",
        },
    ]

    render_kpi_cards(metrics)

    # 핵심 인사이트
    render_insight_box(
        title="Key Insight",
        message=(
            "고객 1인당 가치(ARPB), 구매 빈도, AOV는 전 기간 동안 큰 변화 없이 안정적으로 유지됩니다.\n\n "
            "또한 매출은 특정 상위 고객군에 집중되지 않고 전체 고객에 분산된 구조를 보입니다.\n\n "
            "코호트 리텐션 역시 매우 낮은 수준으로 나타나,\n\n 매출은 기존 고객의 반복 구매보다는 "
            "신규 고객 유입에 의해 발생하는 구조로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    # Customer Value Trend
    trend_cols = [
        col for col in ["arpb", "orders_per_buyer", "aov"]
        if col in monthly_df.columns
    ]
    if "year_month" in monthly_df.columns and trend_cols:
        st.markdown("#### Customer Value Trend")
        st.caption(
            "고객 1인당 가치(ARPB), 구매 빈도, AOV 추이를 통해 "
            "매출이 고객 가치 상승에 의해 발생하는지 확인합니다."
        )

        chart_df = monthly_df[["year_month"] + trend_cols].copy()
        chart_df = chart_df.set_index("year_month")

        chart_df = chart_df.replace([np.inf, -np.inf], None).fillna(0)
        chart_df.index = chart_df.index.astype(str)

        try:
            st.line_chart(chart_df, use_container_width=True)
        except Exception:
            st.warning("고객 가치 추이 차트를 렌더링할 수 없습니다.")

    # New vs Repeat Structure
    if not new_repeat_df.empty:
        st.markdown("#### New vs Repeat Structure")
        st.caption(
            "매출과 구매자가 신규 고객과 재구매 고객 중 어디에서 발생하는지 비교하여 성장 구조를 확인합니다. (상단: Revenue / 하단: Buyers)"
        )

        if {"year_month", "sub_type", "gross_revenue"}.issubset(new_repeat_df.columns):
            revenue_pivot = (
                new_repeat_df.pivot(
                    index="year_month",
                    columns="sub_type",
                    values="gross_revenue",
                ).fillna(0)
            )

            revenue_pivot = revenue_pivot.replace([np.inf, -np.inf], None).fillna(0)
            revenue_pivot.index = revenue_pivot.index.astype(str)

            try:
                st.bar_chart(revenue_pivot, use_container_width=True)
            except Exception:
                st.warning("신규/재구매 매출 차트를 렌더링할 수 없습니다.")

        if {"year_month", "sub_type", "buyers"}.issubset(new_repeat_df.columns):
            buyers_pivot = (
                new_repeat_df.pivot(
                    index="year_month",
                    columns="sub_type",
                    values="buyers",
                ).fillna(0)
            )

            buyers_pivot = buyers_pivot.replace([np.inf, -np.inf], None).fillna(0)
            buyers_pivot.index = buyers_pivot.index.astype(str)

            try:
                st.bar_chart(buyers_pivot, use_container_width=True)
            except Exception:
                st.warning("신규/재구매 구매자 차트를 렌더링할 수 없습니다.")

    # Revenue Concentration
    if not decile_df.empty:
        st.markdown("#### Revenue Concentration")
        st.caption(
            "매출이 소수 고객에 집중되어 있는지 확인하여 특정 고객군 의존 구조 여부를 판단합니다."
        )

        if {"year_month", "sub_type", "gross_revenue"}.issubset(decile_df.columns):
            decile_pivot = (
                decile_df.pivot(
                    index="year_month",
                    columns="sub_type",
                    values="gross_revenue",
                ).fillna(0)
            )

            decile_pivot = decile_pivot.replace([np.inf, -np.inf], None).fillna(0)
            decile_pivot.index = decile_pivot.index.astype(str)

            try:
                st.area_chart(decile_pivot, use_container_width=True)
            except Exception:
                st.warning("매출 집중도 차트를 렌더링할 수 없습니다.")

    # Cohort Retention Heatmap
    if not cohort_df.empty:
        st.markdown("#### Cohort Retention Heatmap")
        st.caption(
            "코호트별 리텐션을 통해 고객 유지 구조가 실제로 존재하는지, "
            "그리고 매출에 기여 가능한 수준인지 확인합니다."
        )

        required_cols = {"cohort_year_month", "month_n", "retention_rate"}
        if required_cols.issubset(cohort_df.columns):
            heatmap_source = cohort_df.copy()
            heatmap_source["month_n"] = heatmap_source["month_n"].astype(str)
            heatmap_source = heatmap_source.replace([np.inf, -np.inf], None).fillna(0)

            chart = (
                alt.Chart(heatmap_source)
                .mark_rect()
                .encode(
                    x=alt.X("month_n:O", title="Month N"),
                    y=alt.Y(
                        "cohort_year_month:O",
                        title="Cohort Month",
                        sort="ascending",
                    ),
                    color=alt.Color("retention_rate:Q", title="Retention Rate"),
                    tooltip=[
                        alt.Tooltip("cohort_year_month:O", title="Cohort"),
                        alt.Tooltip("month_n:O", title="Month N"),
                        alt.Tooltip("retention_rate:Q", title="Retention", format=".1%"),
                    ],
                )
                .properties(height=500)
            )

            try:
                st.altair_chart(chart, use_container_width=True)
            except Exception:
                st.warning("코호트 리텐션 히트맵을 렌더링할 수 없습니다.")

    # 원본 데이터
    with st.expander("원본 데이터 보기"):
        st.dataframe(df.head(100), use_container_width=True)