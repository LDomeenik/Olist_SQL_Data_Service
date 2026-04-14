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


def _to_numeric_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    특정 컬럼을 안전하게 숫자형으로 변환
    """
    if col not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[col], errors="coerce")


def _build_top_drop_df(
    source_df: pd.DataFrame,
    growth_month: str,
    drop_month: str,
) -> pd.DataFrame:
    """
    비교 월 기준 감소 기여도 상위 10개를 계산
    """
    pivot_df = (
        source_df.pivot_table(
            index="dimension_value",
            columns="year_month",
            values="gross_revenue",
            aggfunc="sum",
        )
        .fillna(0)
        .reset_index()
    )

    if growth_month not in pivot_df.columns or drop_month not in pivot_df.columns:
        return pd.DataFrame()

    pivot_df[growth_month] = pd.to_numeric(pivot_df[growth_month], errors="coerce").fillna(0)
    pivot_df[drop_month] = pd.to_numeric(pivot_df[drop_month], errors="coerce").fillna(0)

    pivot_df["rev_diff"] = pivot_df[drop_month] - pivot_df[growth_month]

    top_drop_df = (
        pivot_df.sort_values("rev_diff", ascending=True)
        .head(10)
        .copy()
    )

    return top_drop_df


def render_growth_drill_down(drill_df: pd.DataFrame) -> None:
    """
    Growth Drill Down 페이지 전체를 렌더링하는 함수
    """
    st.subheader("Growth Drill Down")
    st.caption("매출 급락 구간의 원인을 신규/재구매, 카테고리, 지역 관점에서 분해합니다.")

    apply_kpi_metric_style()

    if drill_df is None or drill_df.empty:
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

    # -------------------------
    # 비교 구간
    # -------------------------
    st.markdown("#### 비교 구간")
    st.caption("가장 최근 두 개 월을 기준으로 매출 변화 원인을 비교합니다.")

    col1, col2, col3 = st.columns([1, 0.25, 1])

    with col1:
        st.metric("Base Month", growth_month)

    with col2:
        st.markdown(
            "<div style='text-align:center; font-size:1.6rem; padding-top:1.4rem;'>→</div>",
            unsafe_allow_html=True,
        )

    with col3:
        st.metric("Compare Month", drop_month)

    # -------------------------
    # KPI 비교
    # -------------------------
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

        monthly_summary["gross_revenue"] = pd.to_numeric(
            monthly_summary["gross_revenue"], errors="coerce"
        )
        monthly_summary["buyers"] = pd.to_numeric(
            monthly_summary["buyers"], errors="coerce"
        )
        monthly_summary["order_cnt"] = pd.to_numeric(
            monthly_summary["order_cnt"], errors="coerce"
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
                    f"{_format_number(base_row['gross_revenue'], 'currency')} → {_format_number(comp_row['gross_revenue'], 'currency')}",
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
                    f"{_format_number(base_row['aov'], 'currency')} → {_format_number(comp_row['aov'], 'currency')}",
                    delta=f"{aov_pct:.2f}%" if aov_pct is not None else None,
                )

            st.caption(
                "비교 구간에서 Revenue, Orders, Active Buyers가 함께 감소했으며, "
                "AOV 변화는 상대적으로 제한적입니다.\n\n"
                "즉, 매출 하락은 고객 가치 하락보다 거래량 축소의 영향이 더 큽니다."
            )

            render_insight_box(
                title="Key Insight",
                message=(
                    f"{growth_month} → {drop_month} 구간의 매출 급락은 특정 카테고리나 특정 지역의 붕괴가 아닌,\n\n"
                    "신규 구매자 유입 감소에 따른 전반적 거래량 축소로 해석됩니다.\n\n"
                    "카테고리와 지역 모두 상위 구조는 유지된 상태에서 동반 하락했으며,\n\n"
                    "신규 매출 감소가 전체 매출 감소를 사실상 대부분 설명합니다.\n\n"
                    "즉, 이 하락은 상품 믹스 변화가 아닌 platform-wide demand 감소의 신호입니다."
                ),
                level="info",
            )

    st.divider()

    # -------------------------
    # 신규 vs 재구매
    # -------------------------
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

        chart_source["gross_revenue"] = pd.to_numeric(
            chart_source["gross_revenue"], errors="coerce"
        ).fillna(0)

        try:
            chart = (
                alt.Chart(chart_source)
                .mark_bar(size=42)
                .encode(
                    x=alt.X("year_month:N", title="Year Month"),
                    xOffset=alt.XOffset("dimension_value:N"),
                    y=alt.Y("gross_revenue:Q", title="Gross Revenue"),
                    color=alt.Color(
                        "dimension_value:N",
                        title="Buyer Type",
                        scale=alt.Scale(
                            domain=["new", "repeat"],
                            range=["#4C78A8", "#F58518"],
                        ),
                    ),
                    tooltip=[
                        alt.Tooltip("year_month:N", title="Month"),
                        alt.Tooltip("dimension_value:N", title="Type"),
                        alt.Tooltip("gross_revenue:Q", title="Revenue", format=",.0f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(chart, use_container_width=True)
        except Exception:
            st.warning("차트를 렌더링할 수 없습니다.")

    st.divider()

    # -------------------------
    # 카테고리 감소 기여도
    # -------------------------
    st.markdown("#### Category Contribution Change")
    st.caption("비교 구간에서 매출 감소에 가장 크게 기여한 카테고리 상위 10개를 확인합니다.")

    category_df = df[df["section_type"] == "category"].copy()

    if not category_df.empty:
        category_compare = category_df[
            category_df["year_month"].isin([growth_month, drop_month])
        ].copy()

        top_drop_category = _build_top_drop_df(
            source_df=category_compare,
            growth_month=growth_month,
            drop_month=drop_month,
        )

        if not top_drop_category.empty:
            top_row = top_drop_category.iloc[0]
            st.info(
                f"가장 큰 카테고리 감소 요인은 **{top_row['dimension_value']}**이며, "
                f"비교 구간 매출 차이는 **{top_row['rev_diff']:,.0f}** 입니다."
            )

            try:
                chart = (
                    alt.Chart(top_drop_category)
                    .mark_bar()
                    .encode(
                        x=alt.X("rev_diff:Q", title="Revenue Difference"),
                        y=alt.Y(
                            "dimension_value:N",
                            sort=alt.SortField(field="rev_diff", order="ascending"),
                            title="Category",
                        ),
                        color=alt.condition(
                            alt.datum.rev_diff < 0,
                            alt.value("#E45756"),
                            alt.value("#72B7B2"),
                        ),
                        tooltip=[
                            alt.Tooltip("dimension_value:N", title="Category"),
                            alt.Tooltip("rev_diff:Q", title="Diff", format=",.0f"),
                            alt.Tooltip(f"{growth_month}:Q", title=f"{growth_month}", format=",.0f"),
                            alt.Tooltip(f"{drop_month}:Q", title=f"{drop_month}", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)
            except Exception:
                st.warning("차트를 렌더링할 수 없습니다.")

    st.divider()

    # -------------------------
    # 지역 감소 기여도
    # -------------------------
    st.markdown("#### Regional Contribution Change")
    st.caption("비교 구간에서 매출 감소에 가장 크게 기여한 지역 상위 10개를 확인합니다.")

    city_df = df[df["section_type"] == "city_state"].copy()

    if not city_df.empty:
        city_compare = city_df[
            city_df["year_month"].isin([growth_month, drop_month])
        ].copy()

        top_drop_city = _build_top_drop_df(
            source_df=city_compare,
            growth_month=growth_month,
            drop_month=drop_month,
        )

        if not top_drop_city.empty:
            top_row = top_drop_city.iloc[0]
            st.info(
                f"가장 큰 지역 감소 요인은 **{top_row['dimension_value']}**이며, "
                f"비교 구간 매출 차이는 **{top_row['rev_diff']:,.0f}** 입니다."
            )

            try:
                chart = (
                    alt.Chart(top_drop_city)
                    .mark_bar()
                    .encode(
                        x=alt.X("rev_diff:Q", title="Revenue Difference"),
                        y=alt.Y(
                            "dimension_value:N",
                            sort=alt.SortField(field="rev_diff", order="ascending"),
                            title="Region",
                        ),
                        color=alt.condition(
                            alt.datum.rev_diff < 0,
                            alt.value("#E45756"),
                            alt.value("#72B7B2"),
                        ),
                        tooltip=[
                            alt.Tooltip("dimension_value:N", title="Region"),
                            alt.Tooltip("rev_diff:Q", title="Diff", format=",.0f"),
                            alt.Tooltip(f"{growth_month}:Q", title=f"{growth_month}", format=",.0f"),
                            alt.Tooltip(f"{drop_month}:Q", title=f"{drop_month}", format=",.0f"),
                        ],
                    )
                    .properties(height=360)
                )
                st.altair_chart(chart, use_container_width=True)
            except Exception:
                st.warning("차트를 렌더링할 수 없습니다.")

    # -------------------------
    # 원본 데이터
    # -------------------------
    with st.expander("원본 데이터 보기"):
        st.dataframe(df.head(100), use_container_width=True)