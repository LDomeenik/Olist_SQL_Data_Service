"""
대시보드 개요 페이지 파일

주요 역할:
- 핵심 KPI를 요약 카드 형태로 표시
- Growth / Operational 핵심 추이를 한 화면에서 제공
- 주요 분석 모듈의 핵심 인사이트를 요약하여 보여줌

안정성 개선 버전:
- 전체 DataFrame copy/replace 최소화
- 필요한 컬럼만 명시적으로 numeric 변환
- 차트용 데이터만 따로 안전하게 전처리
- 레이아웃 복잡도 축소
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


def render_overview(
    growth_df: pd.DataFrame,
    drill_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    ops_df: pd.DataFrame,
) -> None:
    st.subheader("Overview")

    if growth_df is None:
        growth_df = pd.DataFrame()
    if drill_df is None:
        drill_df = pd.DataFrame()
    if customer_df is None:
        customer_df = pd.DataFrame()
    if ops_df is None:
        ops_df = pd.DataFrame()

    if growth_df.empty and drill_df.empty and customer_df.empty and ops_df.empty:
        st.warning("표시할 데이터가 없습니다. 먼저 파이프라인을 실행해 주세요.")
        return

    # KPI 계산
    total_revenue = _safe_sum(growth_df, "gross_revenue")
    total_orders = _safe_sum(growth_df, "order_cnt")

    total_buyers = None
    repeat_rate = None

    if not customer_df.empty and "section_type" in customer_df.columns:
        monthly_customer_df = customer_df[customer_df["section_type"] == "monthly_value"]
        total_buyers = _safe_sum(monthly_customer_df, "buyers")

        new_repeat_df = customer_df[customer_df["section_type"] == "new_repeat_share"]

        if not new_repeat_df.empty and {"sub_type", "buyers"}.issubset(new_repeat_df.columns):
            buyers_series = pd.to_numeric(new_repeat_df["buyers"], errors="coerce")
            repeat_buyers = pd.to_numeric(
                new_repeat_df.loc[new_repeat_df["sub_type"] == "repeat", "buyers"],
                errors="coerce",
            ).sum()
            total_repeat_base_buyers = buyers_series.sum()

            if pd.notna(total_repeat_base_buyers) and total_repeat_base_buyers != 0:
                repeat_rate = repeat_buyers / total_repeat_base_buyers

    aov = _safe_ratio(total_revenue, total_orders)
    arpb = _safe_ratio(total_revenue, total_buyers)

    avg_failed_rate = None
    monthly_ops_df = ops_df

    if not ops_df.empty:
        if "row_type" in ops_df.columns:
            monthly_ops_df = ops_df[ops_df["row_type"] == "monthly_kpi"]

        avg_failed_rate = _safe_mean(monthly_ops_df, "failed_rate")

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
            "전체 분석 결과, 플랫폼 매출은 고객 가치 상승보다 신규 고객 유입 규모에 의해 결정되는 구조로 나타납니다.\n\n "
            "매출 급락 구간에서도 가격(AOV)이나 운영 안정성 변화는 제한적이었으며,\n\n "
            "신규 구매자 수 감소가 매출 하락을 직접적으로 설명합니다.\n\n "
            "즉, 현재 매출 변동은 공급/운영 문제가 아닌 acquisition 기반 수요 변화에 의해 발생합니다."
        ),
        level="info",
    )

    st.divider()

    # 차트 1: Growth Structure
    if (
        growth_df is not None
        and not growth_df.empty
        and {"year_month", "gross_revenue"}.issubset(growth_df.columns)
    ):
        st.markdown("### Growth Structure")
        st.caption("거래량 중심 성장 여부와 주요 변동 구간을 확인합니다.")

        growth_chart_df = growth_df[["year_month", "gross_revenue"]].copy()
        growth_chart_df["gross_revenue"] = pd.to_numeric(
            growth_chart_df["gross_revenue"], errors="coerce"
        ).replace([float("inf"), float("-inf")], pd.NA).fillna(0)

        growth_chart_df = growth_chart_df.set_index("year_month")
        growth_chart_df.index = growth_chart_df.index.astype(str)

        st.line_chart(growth_chart_df, height=260)

    # 차트 2: Operational Stability
    if (
        monthly_ops_df is not None
        and not monthly_ops_df.empty
        and {"year_month", "failed_rate"}.issubset(monthly_ops_df.columns)
    ):
        st.markdown("### Operational Stability")
        st.caption("취소율·실패율 추이로 운영 이슈 여부를 확인합니다.")

        ops_chart_df = monthly_ops_df[["year_month", "failed_rate"]].copy()
        ops_chart_df["failed_rate"] = pd.to_numeric(
            ops_chart_df["failed_rate"], errors="coerce"
        ).replace([float("inf"), float("-inf")], pd.NA).fillna(0)

        ops_chart_df = ops_chart_df.set_index("year_month")
        ops_chart_df.index = ops_chart_df.index.astype(str)

        st.line_chart(ops_chart_df, height=260)

    st.divider()

    # Module Summary
    st.markdown("### Module Summary")

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

    render_insight_box(
        title="Action Plan",
        message=(
            "분석 결과, 매출 감소는 운영 문제나 상품 믹스 변화가 아닌 "
            "신규 고객 유입 감소에 따른 수요 축소로 확인되었습니다.\n\n "

            "이에 따라 단기적으로는 acquisition funnel을 채널 → 유입 → 전환 단계로 분해하여\n\n "
            "유입 감소 원인 및 전환 병목 구간을 식별하고, "
            "마케팅 채널 최적화 및 랜딩/결제 UX 개선을 통해 전환율을 회복해야 합니다. \n\n"

            "중기적으로는 CRM, 리타겟팅, 프로모션을 활용한 재구매 유도 전략을 통해 "
            "신규 고객 의존도를 완화하고,\n\n "

            "장기적으로는 코호트 리텐션 개선을 통해 retention 기반 매출 구조로 전환하여\n\n "
            "매출 변동성을 낮추는 방향으로 전략을 설계해야 합니다."
        ),
        level="info",
    )