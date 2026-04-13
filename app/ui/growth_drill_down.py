"""
성장 드릴다운 페이지 파일

주요 역할:
- 매출 급락 구간의 비교 월을 자동 식별
- 신규/재구매, 카테고리, 지역 관점에서 매출 감소 원인을 분해
- 비교 구간 KPI와 세부 기여 구조를 시각화
"""

import altair as alt
import pandas as pd
import streamlit as st

from app.ui.components.insight_box import render_insight_box


def _format_number(value, value_type: str = "number") -> str | None:
    """
    숫자 값을 화면 표시용 문자열로 변환하는 함수

    지원 형식:
    - currency: BRL 통화 형식
    - percent: 퍼센트 형식
    - number: 일반 숫자 형식

    None 또는 NaN은 None으로 반환하여 st.metric 등에 그대로 활용할 수 있게 함
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

    동작:
    - year_month 컬럼의 고유값을 정렬
    - 가장 최근 두 개의 월을 반환

    반환:
    - (기준 월, 비교 월)
    - 데이터가 부족하면 (None, None)
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

    계산식:
    ((after - before) / before) * 100

    예외 처리:
    - before가 None, 0, NaN이면 None 반환
    - after가 NaN이면 None 반환
    """
    if before in [None, 0] or pd.isna(before) or pd.isna(after):
        return None

    return ((after - before) / before) * 100


def render_growth_drill_down(drill_df: pd.DataFrame) -> None:
    """
    Growth Drill Down 페이지 전체를 렌더링하는 함수

    입력 데이터:
    - drill_df: 비교 구간의 buyer_type / category / city_state 데이터가 포함된 DataFrame

    주요 동작:
    - 비교 월 자동 식별
    - 기준 월 vs 비교 월 KPI 요약 카드 표시
    - 신규/재구매 구조 분해
    - 카테고리별 매출 감소 기여도 표시
    - 지역별 매출 감소 기여도 표시
    - 원본 데이터 확인용 테이블 제공
    """
    st.subheader("Growth Drill Down")
    st.caption("매출 급락 구간의 원인을 신규/재구매, 카테고리, 지역 관점에서 분해합니다.")

    if drill_df.empty:
        st.warning("growth_drill_down.csv 파일이 없습니다.")
        return

    if "year_month" not in drill_df.columns or "section_type" not in drill_df.columns:
        st.error("Growth Drill Down 데이터 구조가 올바르지 않습니다.")
        return

    # 데이터 정렬
    df = drill_df.sort_values("year_month").reset_index(drop=True)

    # 비교 월 추출
    growth_month, drop_month = _get_month_pair(df)

    if not growth_month or not drop_month:
        st.warning("비교할 수 있는 월 데이터가 부족합니다.")
        st.dataframe(df.head(30), use_container_width=True)
        return

    # 비교 구간 표시
    st.markdown("#### 비교 구간")

    col1, col2, col3 = st.columns([1, 0.3, 1])
    with col1:
        st.metric("기준 월", growth_month)
    with col2:
        st.markdown("### →")
    with col3:
        st.metric("비교 월", drop_month)

    # buyer_type 데이터 분리
    buyer_df = df[df["section_type"] == "buyer_type"].copy()

    # KPI 요약
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
                    label="Revenue",
                    value=(
                        f"{_format_number(base_row['gross_revenue'], 'currency')} → "
                        f"{_format_number(comp_row['gross_revenue'], 'currency')}"
                    ),
                    delta=f"{rev_pct:.2f}%" if rev_pct is not None else None,
                )

            with k2:
                st.metric(
                    label="Orders",
                    value=(
                        f"{_format_number(base_row['order_cnt'])} → "
                        f"{_format_number(comp_row['order_cnt'])}"
                    ),
                    delta=f"{order_pct:.2f}%" if order_pct is not None else None,
                )

            with k3:
                st.metric(
                    label="Active Buyers",
                    value=(
                        f"{_format_number(base_row['buyers'])} → "
                        f"{_format_number(comp_row['buyers'])}"
                    ),
                    delta=f"{buyer_pct:.2f}%" if buyer_pct is not None else None,
                )

            with k4:
                st.metric(
                    label="AOV",
                    value=(
                        f"{_format_number(base_row['aov'], 'currency')} → "
                        f"{_format_number(comp_row['aov'], 'currency')}"
                    ),
                    delta=f"{aov_pct:.2f}%" if aov_pct is not None else None,
                )

            render_insight_box(
                title="Key Insight",
                message=(
                    f"{growth_month} → {drop_month} 구간의 매출 감소는 "
                    "고객가치(AOV) 하락보다 신규 구매자 감소에 따른 거래량 축소 영향이 더 크게 나타납니다."
                ),
                level="info",
            )

    st.divider()

    # 신규 vs 재구매 구조 분해
    st.markdown("#### 신규 vs 재구매 구조 분해")
    st.caption("매출 감소가 신규 고객 감소인지, 기존 고객 이탈인지 구분합니다.")

    if not buyer_df.empty:
        buyer_compare = buyer_df[
            buyer_df["year_month"].isin([growth_month, drop_month])
        ].copy()

        chart_source = (
            buyer_compare.groupby(["year_month", "dimension_value"], as_index=False)
            .agg(gross_revenue=("gross_revenue", "sum"))
        )

        if not chart_source.empty:
            bar_chart = (
                alt.Chart(chart_source)
                .mark_bar()
                .encode(
                    x=alt.X("year_month:N", title="Year Month"),
                    xOffset=alt.XOffset("dimension_value:N"),
                    y=alt.Y("gross_revenue:Q", title="Gross Revenue"),
                    color=alt.Color("dimension_value:N", title="Buyer Type"),
                    tooltip=[
                        alt.Tooltip("year_month:N", title="Year Month"),
                        alt.Tooltip("dimension_value:N", title="Buyer Type"),
                        alt.Tooltip("gross_revenue:Q", title="Revenue", format=",.2f"),
                    ],
                )
                .properties(height=380)
            )

            st.altair_chart(bar_chart, use_container_width=True)

        buyer_display_df = buyer_compare.copy()
        buyer_display_df["aov"] = (
            buyer_display_df["gross_revenue"]
            / buyer_display_df["order_cnt"].replace(0, pd.NA)
        )

        display_cols = [
            "year_month",
            "dimension_value",
            "buyers",
            "order_cnt",
            "item_cnt",
            "gross_revenue",
            "aov",
        ]
        available_cols = [col for col in display_cols if col in buyer_display_df.columns]

        st.dataframe(buyer_display_df[available_cols], use_container_width=True)

    st.divider()

    # 카테고리별 매출 기여도
    st.markdown("#### 카테고리별 매출 기여도")
    st.caption("특정 카테고리 붕괴인지, 전반적인 수요 감소인지 확인합니다.")

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
            category_pivot["rev_diff"] = (
                category_pivot[drop_month] - category_pivot[growth_month]
            )
            category_pivot = category_pivot.sort_values("rev_diff").head(10)

            chart_df = category_pivot.set_index("dimension_value")["rev_diff"]
            st.bar_chart(chart_df, use_container_width=True)

            st.dataframe(category_pivot, use_container_width=True)

    st.divider()

    # 지역별 매출 기여도
    st.markdown("#### 지역별 매출 기여도")
    st.caption("일부 지역 집중 하락인지, 전체 지역 동반 하락인지 확인합니다.")

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
            city_pivot = city_pivot.sort_values("rev_diff").head(10)

            chart_df = city_pivot.set_index("dimension_value")["rev_diff"]
            st.bar_chart(chart_df, use_container_width=True)

            st.dataframe(city_pivot, use_container_width=True)

    # 원본 데이터
    with st.expander("원본 데이터 보기"):
        st.dataframe(df, use_container_width=True)