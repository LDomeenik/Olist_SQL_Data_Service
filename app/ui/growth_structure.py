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


def render_growth_structure(growth_df: pd.DataFrame) -> None:
    """
    Growth Structure 페이지 전체를 렌더링하는 함수

    입력 데이터:
    - growth_df: 월별 핵심 KPI 및 MoM 지표가 포함된 DataFrame

    주요 동작:
    - 핵심 KPI 카드 표시
    - Gross Revenue 추이 시각화
    - Revenue Driver 추이 시각화
    - MoM Growth 추이 시각화
    - 원본 데이터 확인용 테이블 제공
    """
    st.subheader("Growth Structure")
    st.caption(
        "월별 핵심 KPI 추이를 통해 매출 성장과 급락 구간을 식별하고, "
        "매출 변화 상태에 대해 확인합니다."
    )

    if growth_df.empty:
        st.warning("growth_structure.csv 파일이 없습니다.")
        return

    # 데이터 복사 및 정렬
    df = growth_df.copy()

    if "year_month" in df.columns:
        df = df.sort_values("year_month").reset_index(drop=True)

    # 핵심 KPI 계산
    total_revenue = (
        df["gross_revenue"].sum()
        if "gross_revenue" in df.columns
        else None
    )
    total_orders = (
        df["order_cnt"].sum()
        if "order_cnt" in df.columns
        else None
    )
    total_buyers = (
        df["active_buyers"].sum()
        if "active_buyers" in df.columns
        else None
    )

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

    # KPI 카드
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
            "label": "Active Buyers",
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
    ]

    render_kpi_cards(metrics)

    # 핵심 인사이트
    render_insight_box(
        title="Key Insight",
        message=(
            "매출 변화는 AOV보다 주문 수와 활성 구매자 수 변화에 더 강하게 반응합니다. "
            "즉, 현재 플랫폼은 고객 가치 상승보다 거래량(Volume) 확장에 의해 성장하는 구조입니다."
        ),
        level="info",
    )

    st.divider()

    # Gross Revenue Trend
    if {"year_month", "gross_revenue"}.issubset(df.columns):
        st.markdown("#### Gross Revenue Trend")
        st.caption(
            "월별 매출 추이를 통해 성장, 정체, 급락 구간이 언제 발생하는지 확인합니다."
        )

        revenue_df = df[["year_month", "gross_revenue"]].copy()
        revenue_df = revenue_df.set_index("year_month")
        st.line_chart(revenue_df, use_container_width=True)

    # Revenue Driver Trend
    driver_cols = [
        col for col in ["order_cnt", "active_buyers", "aov", "arpb"]
        if col in df.columns
    ]

    if "year_month" in df.columns and driver_cols:
        st.markdown("#### Revenue Driver Trend")
        st.caption(
            "주문 수, 활성 구매자 수, AOV, ARPB를 함께 비교하여 "
            "매출 변화가 거래량 요인인지 고객 가치 요인인지 확인합니다."
        )

        driver_df = df[["year_month"] + driver_cols].copy()
        driver_df = driver_df.set_index("year_month")
        st.line_chart(driver_df, use_container_width=True)

    # MoM Growth Trend
    mom_cols = [
        col
        for col in [
            "mom_gross_revenue",
            "mom_order_cnt",
            "mom_active_buyers",
            "mom_aov",
            "mom_arpb",
        ]
        if col in df.columns
    ]

    if "year_month" in df.columns and mom_cols:
        st.markdown("#### MoM Growth Trend")
        st.caption(
            "월별 증감률(MoM)을 통해 어떤 KPI가 매출 급등과 급락 구간을 직접 설명하는지 확인합니다."
        )

        mom_df = df[["year_month"] + mom_cols].copy()
        mom_df = mom_df.set_index("year_month")
        st.line_chart(mom_df, use_container_width=True)

    # 원본 데이터
    with st.expander("원본 데이터 보기"):
        st.dataframe(df, use_container_width=True)