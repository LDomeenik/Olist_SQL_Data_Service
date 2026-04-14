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

from app.ui.components.kpi_cards import render_kpi_cards, apply_kpi_metric_style
from app.ui.components.insight_box import render_insight_box


def _safe_sum(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None

    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None

    return s.sum()


def _safe_mean(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None

    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None

    return s.mean()


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
    차트용 최소 전처리
    """
    required = {x_col, *y_cols}

    if df is None or df.empty or not required.issubset(df.columns):
        return None

    chart_df = df[[x_col] + y_cols].copy()

    for col in y_cols:
        chart_df[col] = pd.to_numeric(chart_df[col], errors="coerce")
        chart_df[col] = chart_df[col].replace([np.inf, -np.inf], pd.NA).fillna(0)

    chart_df = chart_df.set_index(x_col)
    chart_df.index = chart_df.index.astype(str)

    return chart_df


def render_customer_value(customer_df: pd.DataFrame) -> None:
    """
    Customer Value Structure 페이지 전체를 렌더링하는 함수
    """
    st.subheader("Customer Value Structure")
    st.caption(
        "매출이 고객 가치 상승보다 신규 고객 유입에 의해 발생하는지 확인하기 위해\n\n"
        "고객 가치 수준, 신규/재구매 구조, 매출 집중도, 코호트 리텐션을 종합적으로 분석합니다."
    )

    apply_kpi_metric_style()

    if customer_df is None or customer_df.empty:
        st.warning("customer_value_structure.csv 파일이 없습니다.")
        return

    # -------------------------
    # 데이터 준비
    # -------------------------
    df = customer_df.copy()

    if "year_month" in df.columns:
        df = df.sort_values("year_month").reset_index(drop=True)

    df = df.replace([np.inf, -np.inf], None)

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

    # -------------------------
    # KPI 계산
    # -------------------------
    total_revenue = _safe_sum(monthly_df, "gross_revenue")
    total_buyers = _safe_sum(monthly_df, "buyers")
    total_orders = _safe_sum(monthly_df, "order_cnt")

    aov = _safe_ratio(total_revenue, total_orders)
    arpb = _safe_ratio(total_revenue, total_buyers)

    repeat_rate = None
    if not new_repeat_df.empty and {"sub_type", "buyers"}.issubset(new_repeat_df.columns):
        buyers_series = pd.to_numeric(new_repeat_df["buyers"], errors="coerce")
        repeat_buyers = pd.to_numeric(
            new_repeat_df.loc[new_repeat_df["sub_type"] == "repeat", "buyers"],
            errors="coerce",
        ).sum()
        total_repeat_base_buyers = buyers_series.sum()

        if pd.notna(total_repeat_base_buyers) and total_repeat_base_buyers != 0:
            repeat_rate = repeat_buyers / total_repeat_base_buyers

    month1_retention = None
    if not cohort_df.empty and {"month_n", "retention_rate"}.issubset(cohort_df.columns):
        cohort_df["retention_rate"] = pd.to_numeric(cohort_df["retention_rate"], errors="coerce")
        m1 = cohort_df[pd.to_numeric(cohort_df["month_n"], errors="coerce") == 1]
        if not m1.empty:
            month1_retention = _safe_mean(m1, "retention_rate")

    top10_share = None
    if not decile_df.empty and {"sub_type", "revenue_share"}.issubset(decile_df.columns):
        decile_df["revenue_share"] = pd.to_numeric(decile_df["revenue_share"], errors="coerce")
        top10_df = decile_df[decile_df["sub_type"] == "decile_1"]
        if not top10_df.empty:
            top10_share = _safe_mean(top10_df, "revenue_share")

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

    st.caption(
        "요약 지표 기준으로 보면, 고객 1인당 가치와 구매 빈도는 비교적 안정적이며\n\n"
        "재구매율과 초기 리텐션은 낮은 수준을 보여 현재 매출은 신규 고객 유입에 더 크게 의존합니다."
    )

    render_insight_box(
        title="Key Insight",
        message=(
            "고객 1인당 가치(ARPB), 구매 빈도, AOV는 전 기간 동안 큰 변화 없이 안정적으로 유지됩니다.\n\n"
            "또한 매출은 특정 상위 고객군에 과도하게 집중되지 않고 전체 고객에 비교적 분산된 구조를 보입니다.\n\n"
            "반면 코호트 리텐션은 낮은 수준으로 나타나,\n\n"
            "현재 매출은 기존 고객의 반복 구매보다는 신규 고객 유입에 의해 발생하는 구조로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    # -------------------------
    # Customer Value Trend
    # -------------------------
    trend_cols = [
        col for col in ["arpb", "orders_per_buyer", "aov"]
        if col in monthly_df.columns
    ]

    if "year_month" in monthly_df.columns and trend_cols:
        st.markdown("#### Customer Value Trend")
        st.caption(
            "고객 1인당 가치(ARPB), 구매 빈도, AOV 추이를 통해 "
            "매출이 고객 가치 상승에 의해 발생하는지 확인합니다.\n\n"
            "이 지표들이 안정적이라면 매출 변화는 고객 가치보다 고객 수 변화의 영향이 더 크다고 해석할 수 있습니다."
        )

        chart_df = _prepare_chart_df(
            monthly_df,
            x_col="year_month",
            y_cols=trend_cols,
        )

        if chart_df is not None:
            try:
                st.line_chart(chart_df, use_container_width=True)
            except Exception:
                st.warning("고객 가치 추이 차트를 렌더링할 수 없습니다.")

    # -------------------------
    # New vs Repeat Structure
    # -------------------------
    if not new_repeat_df.empty:
        st.markdown("#### New vs Repeat Structure")
        st.caption(
            "매출과 구매자가 신규 고객과 재구매 고객 중 어디에서 발생하는지 비교하여 "
            "성장 구조가 신규 유입 기반인지, 기존 고객 유지 기반인지 확인합니다."
        )

        if {"year_month", "sub_type", "gross_revenue"}.issubset(new_repeat_df.columns):
            revenue_source = new_repeat_df[["year_month", "sub_type", "gross_revenue"]].copy()
            revenue_source["gross_revenue"] = pd.to_numeric(
                revenue_source["gross_revenue"], errors="coerce"
            ).fillna(0)

            try:
                revenue_chart = (
                    alt.Chart(revenue_source)
                    .mark_bar(size=18)
                    .encode(
                        x=alt.X("year_month:N", title="Year Month"),
                        y=alt.Y("gross_revenue:Q", title="Gross Revenue"),
                        color=alt.Color(
                            "sub_type:N",
                            title="Type",
                            scale=alt.Scale(
                                domain=["new", "repeat"],
                                range=["#4C78A8", "#F58518"],
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("year_month:N", title="Month"),
                            alt.Tooltip("sub_type:N", title="Type"),
                            alt.Tooltip("gross_revenue:Q", title="Revenue", format=",.0f"),
                        ],
                    )
                    .properties(height=320)
                )
                st.altair_chart(revenue_chart, use_container_width=True)
            except Exception:
                st.warning("신규/재구매 매출 차트를 렌더링할 수 없습니다.")

        if {"year_month", "sub_type", "buyers"}.issubset(new_repeat_df.columns):
            buyers_source = new_repeat_df[["year_month", "sub_type", "buyers"]].copy()
            buyers_source["buyers"] = pd.to_numeric(
                buyers_source["buyers"], errors="coerce"
            ).fillna(0)

            try:
                buyers_chart = (
                    alt.Chart(buyers_source)
                    .mark_bar(size=18)
                    .encode(
                        x=alt.X("year_month:N", title="Year Month"),
                        y=alt.Y("buyers:Q", title="Buyers"),
                        color=alt.Color(
                            "sub_type:N",
                            title="Type",
                            scale=alt.Scale(
                                domain=["new", "repeat"],
                                range=["#4C78A8", "#F58518"],
                            ),
                        ),
                        tooltip=[
                            alt.Tooltip("year_month:N", title="Month"),
                            alt.Tooltip("sub_type:N", title="Type"),
                            alt.Tooltip("buyers:Q", title="Buyers", format=",.0f"),
                        ],
                    )
                    .properties(height=320)
                )
                st.altair_chart(buyers_chart, use_container_width=True)
            except Exception:
                st.warning("신규/재구매 구매자 차트를 렌더링할 수 없습니다.")

    # -------------------------
    # Revenue Concentration
    # -------------------------
    if not decile_df.empty:
        st.markdown("#### Revenue Concentration")
        st.caption(
            "매출이 소수 고객군에 집중되어 있는지 확인합니다.\n\n"
            "Top 10% Share가 높을수록 특정 고객군 의존도가 높고, 낮을수록 보다 분산된 매출 구조로 해석할 수 있습니다."
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

    # -------------------------
    # Cohort Retention Heatmap
    # -------------------------
    if not cohort_df.empty:
        st.markdown("#### Cohort Retention Heatmap")
        st.caption(
            "코호트별 리텐션을 통해 고객 유지 구조가 실제로 존재하는지, "
            "그리고 신규 고객이 장기 매출에 기여할 수 있는 수준인지 확인합니다."
        )

        required_cols = {"cohort_year_month", "month_n", "retention_rate"}
        if required_cols.issubset(cohort_df.columns):
            heatmap_source = cohort_df.copy()
            heatmap_source["month_n"] = heatmap_source["month_n"].astype(str)
            heatmap_source["retention_rate"] = pd.to_numeric(
                heatmap_source["retention_rate"], errors="coerce"
            ).fillna(0)
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

    # -------------------------
    # 원본 데이터
    # -------------------------
    with st.expander("원본 데이터 보기"):
        st.dataframe(df.head(100), use_container_width=True)