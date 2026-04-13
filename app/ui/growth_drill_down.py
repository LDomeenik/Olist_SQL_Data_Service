"""
성장 드릴다운 페이지 파일

주요 역할:
- 매출 급락 구간의 비교 월을 자동 식별
- 신규/재구매, 카테고리, 지역 관점에서 매출 감소 원인을 분해
- 비교 구간 KPI와 세부 기여 구조를 시각화
"""

import altair as alt
import pandas as pd
import numpy as np
import streamlit as st

from app.ui.components.insight_box import render_insight_box
from app.ui.components.kpi_cards import apply_kpi_metric_style


def _format_number(value, value_type: str = "number") -> str | None:
    """
    숫자 값을 화면 표시용 문자열로 변환하는 함수
    """
    if value is None or pd.isna(value):
        return None

    if value_type == "currency":
        if abs(value) >= 1_000_000:
            return f"BRL {value / 1_000_000:.2f}M"
        if abs(value) >= 1_000:
            return f"BRL {value / 1_000:.1f}K"
        return f"BRL {value:,.0f}"

    if value_type == "percent":
        return f"{value:.2f}%"

    return f"{value:,.0f}"


def _get_month_pair(df: pd.DataFrame) -> tuple[str | None, str | None]:
    """
    비교할 두 개의 연월 값을 추출하는 함수
    """
    if "year_month" not in df.columns or df.empty:
        return None, None

    months = sorted(df["year_month"].dropna().unique().tolist())
    if len(months) < 2:
        return None, None

    return months[-2], months[-1]


def _safe_pct_change(before, after) -> float | None:
    """
    두 값의 증감률(%)을 안전하게 계산하는 함수
    """
    if before in [None, 0] or pd.isna(before) or pd.isna(after):
        return None

    return ((after - before) / before) * 100


def render_growth_drill_down(drill_df: pd.DataFrame) -> None:
    """
    Growth Drill Down 페이지 전체를 렌더링하는 함수
    """
    st.subheader("Growth Drill Down")
    st.caption("매출 급락 구간의 원인을 신규/재구매, 카테고리, 지역 관점에서 분해합니다.")

    apply_kpi_metric_style()

    if drill_df.empty:
        st.warning("growth_drill_down.csv 파일이 없습니다.")
        return

    if "year_month" not in drill_df.columns or "section_type" not in drill_df.columns:
        st.error("Growth Drill Down 데이터 구조가 올바르지 않습니다.")
        return

    df = drill_df.sort_values("year_month").reset_index(drop=True)
    df = df.replace([np.inf, -np.inf], None)

    growth_month, drop_month = _get_month_pair(df)

    if not growth_month or not drop_month:
        st.warning("비교할 수 있는 월 데이터가 부족합니다.")
        st.dataframe(df.head(30), use_container_width=True)
        return

    # 비교 구간
    st.markdown("#### 비교 구간")

    col1, col2, col3 = st.columns([1, 0.3, 1])
    with col1:
        st.metric("기준 월", growth_month)
    with col2:
        st.markdown("### →")
    with col3:
        st.metric("비교 월", drop_month)

    # KPI 비교
    buyer_df = df[df["section_type"] == "buyer_type"].copy()

    if not buyer_df.empty:
        monthly_summary = (
            buyer_df.groupby("year_month", as_index=False)
            .agg(
                gross_revenue=("gross_revenue", "sum"),
                buyers=("buyers", "sum"),
                order_cnt=("order_cnt", "sum"),
            )
        )

        monthly_summary["aov"] = (
            monthly_summary["gross_revenue"]
            / monthly_summary["order_cnt"].replace(0, pd.NA)
        )

        base_row = monthly_summary[monthly_summary["year_month"] == growth_month]
        comp_row = monthly_summary[monthly_summary["year_month"] == drop_month]

        if not base_row.empty and not comp_row.empty:
            base_row = base_row.iloc[0]
            comp_row = comp_row.iloc[0]

            rev_pct = _safe_pct_change(base_row["gross_revenue"], comp_row["gross_revenue"])
            order_pct = _safe_pct_change(base_row["order_cnt"], comp_row["order_cnt"])
            buyer_pct = _safe_pct_change(base_row["buyers"], comp_row["buyers"])
            aov_pct = _safe_pct_change(base_row["aov"], comp_row["aov"])

            k1, k2, k3, k4 = st.columns(4)

            with k1:
                st.metric(
                    "Revenue",
                    f"{_format_number(base_row['gross_revenue'],'currency')} → {_format_number(comp_row['gross_revenue'],'currency')}",
                    delta=f"{rev_pct:.2f}%" if rev_pct is not None else None,
                )
            with k2:
                st.metric(
                    "Orders",
                    f"{_format_number(base_row['order_cnt'])} → {_format_number(comp_row['order_cnt'])}",
                    delta=f"{order_pct:.2f}%" if order_pct is not None else None,
                )
            with k3:
                st.metric(
                    "Active Buyers",
                    f"{_format_number(base_row['buyers'])} → {_format_number(comp_row['buyers'])}",
                    delta=f"{buyer_pct:.2f}%" if buyer_pct is not None else None,
                )
            with k4:
                st.metric(
                    "AOV",
                    f"{_format_number(base_row['aov'],'currency')} → {_format_number(comp_row['aov'],'currency')}",
                    delta=f"{aov_pct:.2f}%" if aov_pct is not None else None,
                )

            render_insight_box(
                title="Key Insight",
                message=(
                    f"{growth_month} → {drop_month} 구간의 매출 급락은 특정 카테고리나 특정 지역의 붕괴가 아닌,\n\n "
                    "신규 구매자 유입 감소에 따른 전반적 거래량 축소로 해석됩니다.\n\n "
                    "카테고리와 지역 모두 상위 구조는 유지된 상태에서 동반 하락했으며,\n\n "
                    "신규 매출 감소가 전체 매출 감소를 사실상 대부분 설명합니다.\n\n "
                    "즉, 이 하락은 상품 믹스 변화가 아닌 platform-wide demand 감소의 신호입니다."
                ),
                level="info",
            )

    st.divider()

    # 신규 vs 재구매
    st.markdown("#### New vs Repeat Revenue Structure")
    st.caption("신규 고객과 재구매 고객이 매출 감소에 얼마나 기여했는지 비교합니다.")

    if not buyer_df.empty:
        buyer_compare = buyer_df[
            buyer_df["year_month"].isin([growth_month, drop_month])
        ].copy()

        chart_source = (
            buyer_compare.groupby(["year_month", "dimension_value"], as_index=False)
            .agg(gross_revenue=("gross_revenue", "sum"))
        )

        chart_source = chart_source.replace([np.inf, -np.inf], None).fillna(0)

        try:
            st.altair_chart(
                alt.Chart(chart_source).mark_bar().encode(
                    x="year_month:N",
                    xOffset="dimension_value:N",
                    y="gross_revenue:Q",
                    color="dimension_value:N",
                ),
                use_container_width=True,
            )
        except Exception:
            st.warning("차트를 렌더링할 수 없습니다.")

    st.divider()

    # 카테고리
    st.markdown("#### Category Contribution Change")
    st.caption("카테고리별 매출 변화량을 통해 특정 카테고리 영향 여부를 확인합니다.")
    category_df = df[df["section_type"] == "category"].copy()

    if not category_df.empty:
        category_compare = category_df[
            category_df["year_month"].isin([growth_month, drop_month])
        ]

        category_pivot = (
            category_compare.pivot_table(
                index="dimension_value",
                columns="year_month",
                values="gross_revenue",
                aggfunc="sum",
            )
            .fillna(0)
            .reset_index()
        )

        if growth_month in category_pivot.columns and drop_month in category_pivot.columns:
            category_pivot["rev_diff"] = category_pivot[drop_month] - category_pivot[growth_month]

            chart_df = category_pivot.set_index("dimension_value")["rev_diff"]
            chart_df = chart_df.replace([np.inf, -np.inf], 0).fillna(0)

            try:
                st.bar_chart(chart_df, use_container_width=True)
            except Exception:
                st.warning("차트를 렌더링할 수 없습니다.")

    st.divider()

    # 지역
    st.markdown("#### Regional Contribution Change")
    st.caption("지역별 매출 변화량을 통해 특정 지역의 수요 감소 여부를 확인합니다.")
    city_df = df[df["section_type"] == "city_state"].copy()

    if not city_df.empty:
        city_compare = city_df[
            city_df["year_month"].isin([growth_month, drop_month])
        ]

        city_pivot = (
            city_compare.pivot_table(
                index="dimension_value",
                columns="year_month",
                values="gross_revenue",
                aggfunc="sum",
            )
            .fillna(0)
            .reset_index()
        )

        if growth_month in city_pivot.columns and drop_month in city_pivot.columns:
            city_pivot["rev_diff"] = city_pivot[drop_month] - city_pivot[growth_month]

            chart_df = city_pivot.set_index("dimension_value")["rev_diff"]
            chart_df = chart_df.replace([np.inf, -np.inf], 0).fillna(0)

            try:
                st.bar_chart(chart_df, use_container_width=True)
            except Exception:
                st.warning("차트를 렌더링할 수 없습니다.")

    # 원본 데이터
    with st.expander("원본 데이터 보기"):
        st.dataframe(df.head(100), use_container_width=True)