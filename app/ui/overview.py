# """
# 대시보드 개요 페이지 파일

# 주요 역할:
# - 핵심 KPI를 요약 카드 형태로 표시
# - Growth / Operational 핵심 추이를 한 화면에서 제공
# - 주요 분석 모듈의 핵심 인사이트를 요약하여 보여줌

# 안정성 개선 버전:
# - 전체 DataFrame copy/replace 최소화
# - 필요한 컬럼만 명시적으로 numeric 변환
# - 차트용 데이터만 따로 안전하게 전처리
# - 레이아웃 복잡도 축소
# """

# import pandas as pd
# import streamlit as st

# from app.ui.components.kpi_cards import render_kpi_cards
# from app.ui.components.insight_box import render_insight_box


# def _safe_sum(df: pd.DataFrame, col: str):
#     if df is None or df.empty or col not in df.columns:
#         return None

#     s = pd.to_numeric(df[col], errors="coerce")
#     if s.dropna().empty:
#         return None

#     return s.sum()


# def _safe_mean(df: pd.DataFrame, col: str):
#     if df is None or df.empty or col not in df.columns:
#         return None

#     s = pd.to_numeric(df[col], errors="coerce")
#     if s.dropna().empty:
#         return None

#     return s.mean()


# def _safe_ratio(numerator, denominator):
#     if numerator is None or denominator in [None, 0]:
#         return None
#     return numerator / denominator


# def render_overview(
#     growth_df: pd.DataFrame,
#     drill_df: pd.DataFrame,
#     customer_df: pd.DataFrame,
#     ops_df: pd.DataFrame,
# ) -> None:
#     st.subheader("Overview")

#     if growth_df is None:
#         growth_df = pd.DataFrame()
#     if drill_df is None:
#         drill_df = pd.DataFrame()
#     if customer_df is None:
#         customer_df = pd.DataFrame()
#     if ops_df is None:
#         ops_df = pd.DataFrame()

#     if growth_df.empty and drill_df.empty and customer_df.empty and ops_df.empty:
#         st.warning("표시할 데이터가 없습니다. 먼저 파이프라인을 실행해 주세요.")
#         return

#     # KPI 계산
#     total_revenue = _safe_sum(growth_df, "gross_revenue")
#     total_orders = _safe_sum(growth_df, "order_cnt")

#     total_buyers = None
#     repeat_rate = None

#     if not customer_df.empty and "section_type" in customer_df.columns:
#         monthly_customer_df = customer_df[customer_df["section_type"] == "monthly_value"]
#         total_buyers = _safe_sum(monthly_customer_df, "buyers")

#         new_repeat_df = customer_df[customer_df["section_type"] == "new_repeat_share"]

#         if not new_repeat_df.empty and {"sub_type", "buyers"}.issubset(new_repeat_df.columns):
#             buyers_series = pd.to_numeric(new_repeat_df["buyers"], errors="coerce")
#             repeat_buyers = pd.to_numeric(
#                 new_repeat_df.loc[new_repeat_df["sub_type"] == "repeat", "buyers"],
#                 errors="coerce",
#             ).sum()
#             total_repeat_base_buyers = buyers_series.sum()

#             if pd.notna(total_repeat_base_buyers) and total_repeat_base_buyers != 0:
#                 repeat_rate = repeat_buyers / total_repeat_base_buyers

#     aov = _safe_ratio(total_revenue, total_orders)
#     arpb = _safe_ratio(total_revenue, total_buyers)

#     avg_failed_rate = None
#     monthly_ops_df = ops_df

#     if not ops_df.empty:
#         if "row_type" in ops_df.columns:
#             monthly_ops_df = ops_df[ops_df["row_type"] == "monthly_kpi"]

#         avg_failed_rate = _safe_mean(monthly_ops_df, "failed_rate")

#     metrics = [
#         {
#             "label": "Revenue",
#             "value": total_revenue,
#             "type": "currency",
#         },
#         {
#             "label": "Orders",
#             "value": total_orders,
#             "type": "integer",
#         },
#         {
#             "label": "Buyers",
#             "value": total_buyers,
#             "type": "integer",
#         },
#         {
#             "label": "AOV",
#             "value": aov,
#             "type": "currency",
#         },
#         {
#             "label": "ARPB",
#             "value": arpb,
#             "type": "currency",
#         },
#         {
#             "label": "Repeat Rate",
#             "value": repeat_rate * 100 if repeat_rate is not None else None,
#             "type": "percent",
#         },
#         {
#             "label": "Avg Failed Rate",
#             "value": avg_failed_rate * 100 if avg_failed_rate is not None else None,
#             "type": "percent",
#         },
#     ]

#     render_kpi_cards(metrics)

#     render_insight_box(
#         title="Executive Summary",
#         message=(
#             "전체 분석 결과, 플랫폼 매출은 고객 가치 상승보다 신규 고객 유입 규모에 의해 결정되는 구조로 나타납니다.\n\n "
#             "툭하 매출 급락 구간에서도 가격(AOV)과 운영 안정성 지표는 큰 변화 없이 유지되었으며,\n\n "
#             "신규 구매자 수 감소가 매출 하락을 직접적으로 설명합니다.\n\n "
#             "이는 현재 매출 변동이 공급 또는 운영 이슈가 아닌, "
#             "외부 수요 변화(acquisition 감소)에 의해 발생했음을 의미합니다.\n\n "
#             "결과적으로 현재 플랫폼은 신규 유입 의존도가 높은 구조를 가지며,\n\n "
#             "유입 감소 시 매출이 직접적으로 영향을 받는 구조로 해석됩니다."
#         ),
#         level="info",
#     )

#     st.divider()

#     # 차트 1: Growth Structure
#     if (
#         growth_df is not None
#         and not growth_df.empty
#         and {"year_month", "gross_revenue"}.issubset(growth_df.columns)
#     ):
#         st.markdown("### Growth Structure")
#         st.caption("거래량 중심 성장 여부와 주요 변동 구간을 확인합니다.")

#         growth_chart_df = growth_df[["year_month", "gross_revenue"]].copy()
#         growth_chart_df["gross_revenue"] = pd.to_numeric(
#             growth_chart_df["gross_revenue"], errors="coerce"
#         ).replace([float("inf"), float("-inf")], pd.NA).fillna(0)

#         growth_chart_df = growth_chart_df.set_index("year_month")
#         growth_chart_df.index = growth_chart_df.index.astype(str)

#         st.line_chart(growth_chart_df, height=260)

#     # 차트 2: Operational Stability
#     if (
#         monthly_ops_df is not None
#         and not monthly_ops_df.empty
#         and {"year_month", "failed_rate"}.issubset(monthly_ops_df.columns)
#     ):
#         st.markdown("### Operational Stability")
#         st.caption("취소율·실패율 추이로 운영 이슈 여부를 확인합니다.")

#         ops_chart_df = monthly_ops_df[["year_month", "failed_rate"]].copy()
#         ops_chart_df["failed_rate"] = pd.to_numeric(
#             ops_chart_df["failed_rate"], errors="coerce"
#         ).replace([float("inf"), float("-inf")], pd.NA).fillna(0)

#         ops_chart_df = ops_chart_df.set_index("year_month")
#         ops_chart_df.index = ops_chart_df.index.astype(str)

#         st.line_chart(ops_chart_df, height=260)

#     st.divider()

#     # Module Summary
#     st.markdown("### Module Summary")

#     render_insight_box(
#         title="Growth Drill Down",
#         message=(
#             "매출 감소 구간을 분해한 결과, 특정 카테고리 또는 지역의 문제가 아닌\n\n"
#             "전체적인 거래량 감소가 주요 원인으로 확인됩니다.\n\n"
#             "특히 신규 고객 유입 감소가 매출 하락에 가장 크게 기여하고 있으며,\n\n"
#             "재구매 고객의 소비 패턴은 비교적 안정적으로 유지되고 있습니다."
#         ),
#         level="success",
#     )

#     render_insight_box(
#         title="Customer Value Structure",
#         message=(
#             "고객 1인당 가치(ARPB), 구매 빈도, AOV는 큰 변동 없이 안정적인 흐름을 보이며,\n\n"
#             "매출 변화는 고객 가치 상승이 아닌 고객 수 변화에 의해 발생하는 구조입니다.\n\n"
#             "즉, 현재 비즈니스는 고객의 질(Quality)보다 양(Volume)에 더 의존하는 성장 구조를 가지고 있습니다."
#         ),
#         level="success",
#     )

#     render_insight_box(
#         title="Action Plan",
#         message=(
#             "분석 결과, 매출 감소는 운영 문제나 상품 믹스 변화가 아닌 "
#             "신규 고객 유입 감소에 따른 수요 축소로 확인되었습니다.\n\n "

#             "이에 따라 단기적으로는 acquisition funnel을 채널 → 유입 → 전환 단계로 분해하여\n\n "
#             "유입 감소 원인 및 전환 병목 구간을 식별하고, "
#             "마케팅 채널 최적화 및 랜딩/결제 UX 개선을 통해 전환율을 회복해야 합니다. \n\n"

#             "중기적으로는 CRM, 리타겟팅, 프로모션을 활용한 재구매 유도 전략을 통해 "
#             "신규 고객 의존도를 완화하고,\n\n "

#             "장기적으로는 코호트 리텐션 개선을 통해 retention 기반 매출 구조로 전환하여\n\n "
#             "매출 변동성을 낮추는 방향으로 전략을 설계해야 합니다."
#         ),
#         level="info",
#     )

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


def _prepare_line_chart_df(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
) -> pd.DataFrame | None:
    """
    line chart용 최소 전처리
    """
    if df is None or df.empty:
        return None

    if x_col not in df.columns or y_col not in df.columns:
        return None

    chart_df = df[[x_col, y_col]].copy()
    chart_df[y_col] = pd.to_numeric(chart_df[y_col], errors="coerce")
    chart_df[y_col] = chart_df[y_col].replace([float("inf"), float("-inf")], pd.NA).fillna(0)
    chart_df = chart_df.set_index(x_col)
    chart_df.index = chart_df.index.astype(str)

    return chart_df


def render_overview(
    growth_df: pd.DataFrame,
    drill_df: pd.DataFrame,
    customer_df: pd.DataFrame,
    ops_df: pd.DataFrame,
) -> None:
    st.subheader("Overview")
    st.caption(
        "핵심 KPI와 주요 분석 모듈 결과를 한 화면에서 요약해 현재 비즈니스 구조를 빠르게 파악합니다.\n\n"
    )

    apply_kpi_metric_style()

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

    st.caption(
        "요약 지표 기준으로 볼 때, 매출은 고객 가치 상승보다 고객 수와 주문 수 변화에 더 크게 반응하며,\n\n"
        "운영 실패율은 상대적으로 안정적인 수준을 유지합니다."
    )

    render_insight_box(
        title="Executive Summary",
        message=(
            "전체 분석 결과, 플랫폼 매출은 고객 가치(AOV, ARPB)보다 신규 고객 유입 규모에 의해 결정되는 구조로 나타납니다.\n\n"
            "특히 매출 급락 구간에서도 가격(AOV)과 운영 안정성 지표는 큰 변화 없이 유지되었으며,\n\n"
            "신규 구매자 수 감소가 매출 하락을 직접적으로 설명합니다.\n\n"
            "이는 현재 매출 변동이 공급 또는 운영 이슈가 아닌, 외부 수요 변화(acquisition 감소)에 의해 발생했음을 의미합니다.\n\n"
            "결과적으로 현재 플랫폼은 신규 유입 의존도가 높은 구조를 가지며,\n\n"
            "유입 감소 시 매출이 직접적으로 영향을 받는 구조로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    # Growth Structure
    growth_chart_df = _prepare_line_chart_df(
        growth_df,
        x_col="year_month",
        y_col="gross_revenue",
    )

    if growth_chart_df is not None:
        st.markdown("### Growth Structure")
        st.caption(
            "월별 매출 추이를 통해 성장, 정체, 하락 구간을 빠르게 확인합니다.\n\n"
            "Overview에서는 전체 방향성만 요약하고, 세부 원인은 Growth Structure와 Growth Drill Down에서 확인합니다."
        )
        st.line_chart(growth_chart_df, height=260)

    # Operational Stability
    ops_chart_df = _prepare_line_chart_df(
        monthly_ops_df,
        x_col="year_month",
        y_col="failed_rate",
    )

    if ops_chart_df is not None:
        st.markdown("### Operational Stability")
        st.caption(
            "월별 실패율 추이를 통해 매출 변동이 운영 이슈와 연결되는지 빠르게 점검합니다.\n\n"
            "실패율이 안정적인 경우, 매출 하락 원인은 운영보다 수요 측면일 가능성이 높습니다."
        )
        st.line_chart(ops_chart_df, height=260)

    st.divider()

    # Module Summary
    st.markdown("### Module Summary")
    st.caption("각 모듈에서 확인된 핵심 결과를 요약합니다.")

    render_insight_box(
        title="Growth Drill Down",
        message=(
            "매출 감소 구간을 분해한 결과, 특정 카테고리 또는 특정 지역의 붕괴가 아니라\n\n"
            "전체적인 거래량 감소가 주요 원인으로 확인됩니다.\n\n"
            "특히 신규 고객 유입 감소가 매출 하락에 가장 크게 기여하고 있으며,\n\n"
            "재구매 고객의 소비 패턴은 비교적 안정적으로 유지되고 있습니다."
        ),
        level="success",
    )

    render_insight_box(
        title="Customer Value Structure",
        message=(
            "고객 1인당 가치(ARPB), 구매 빈도, AOV는 큰 변동 없이 안정적인 흐름을 보이며,\n\n"
            "매출 변화는 고객 가치 상승이 아닌 고객 수 변화에 의해 발생하는 구조입니다.\n\n"
            "즉, 현재 비즈니스는 고객의 질(Quality)보다 양(Volume)에 더 의존하는 성장 구조를 가지고 있습니다."
        ),
        level="success",
    )

    render_insight_box(
        title="Action Plan",
        message=(
            "분석 결과, 매출 감소는 운영 문제나 상품 믹스 변화가 아닌 신규 고객 유입 감소에 따른 수요 축소로 확인되었습니다.\n\n"
            "이에 따라 단기적으로는 acquisition funnel을 채널 → 유입 → 전환 단계로 분해하여\n\n"
            "유입 감소 원인과 전환 병목 구간을 식별하고, 마케팅 채널 최적화 및 랜딩/결제 UX 개선을 통해 전환율을 회복해야 합니다.\n\n"
            "중기적으로는 CRM, 리타겟팅, 프로모션을 활용한 재구매 유도 전략을 통해 신규 고객 의존도를 완화하고,\n\n"
            "장기적으로는 코호트 리텐션 개선을 통해 retention 기반 매출 구조로 전환하여\n\n"
            "매출 변동성을 낮추는 방향으로 전략을 설계해야 합니다."
        ),
        level="info",
    )