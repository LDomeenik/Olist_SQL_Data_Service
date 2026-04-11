"""
운영 안정성 페이지 파일

주요 역할:
- 운영 안정성 관련 핵심 KPI를 요약 카드 형태로 표시
- 월별 실패율 및 운영 지표 추이를 시각화
- 매출 변동이 운영 이슈와 직접 연결되는지 진단
"""

import streamlit as st
import pandas as pd

from ui.components.kpi_cards import render_kpi_cards
from ui.components.insight_box import render_insight_box


def render_operational_stability(ops_df: pd.DataFrame) -> None:
    st.subheader("Operational Stability")
    st.caption(
        "월별 취소율, unavailable 비율, 실패율 추이를 통해 운영 안정성의 추이를 확인하고, "
        "매출 변동이 운영 이슈와 직접적으로 연결되는지 확인합니다."
    )

    if ops_df.empty:
        st.warning("operational_stability.csv 파일이 없습니다.")
        return

    df = ops_df.copy()
    monthly_df = df.copy()

    if "row_type" in df.columns:
        monthly_df = df[df["row_type"] == "monthly_kpi"].copy()

    if monthly_df.empty:
        st.warning("monthly_kpi 데이터가 없습니다.")
        return

    if "year_month" in monthly_df.columns:
        monthly_df = monthly_df.sort_values("year_month").reset_index(drop=True)

    metrics = [
        {
            "label": "Avg Cancel Rate",
            "value": monthly_df["cancel_rate"].mean() * 100 if "cancel_rate" in monthly_df.columns else None,
            "type": "percent",
        },
        {
            "label": "Avg Unavailable Rate",
            "value": monthly_df["unavailable_rate"].mean() * 100 if "unavailable_rate" in monthly_df.columns else None,
            "type": "percent",
        },
        {
            "label": "Avg Failed Rate",
            "value": monthly_df["failed_rate"].mean() * 100 if "failed_rate" in monthly_df.columns else None,
            "type": "percent",
        },
        {
            "label": "Max Failed Rate",
            "value": monthly_df["failed_rate"].max() * 100 if "failed_rate" in monthly_df.columns else None,
            "type": "percent",
        },
    ]

    render_kpi_cards(metrics)

    render_insight_box(
        title="Key Insight",
        message=(
            "거래 안정성 지표는 2017년 대비 2018년에 전반적으로 개선되는 흐름을 보입니다. "
            "특히 매출 급락 구간에서도 취소율과 실패율은 상승하지 않아, 매출 감소가 운영 이슈보다 신규 고객 유입 감소와 더 관련된 것으로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    if {"year_month", "failed_rate"}.issubset(monthly_df.columns):
        st.markdown("#### Failed Rate Trend")
        st.caption(
            "월별 실패율 추이를 통해 운영 안정성이 전반적으로 개선되고 있는지, 그리고 이상 구간이 언제 발생하는지 확인합니다."
        )

        failed_df = monthly_df[["year_month", "failed_rate"]].copy().set_index("year_month")
        st.line_chart(failed_df, use_container_width=True)

    rate_cols = [
        col for col in ["cancel_rate", "unavailable_rate", "failed_rate"]
        if col in monthly_df.columns
    ]
    if "year_month" in monthly_df.columns and rate_cols:
        st.markdown("#### Operational Stability Rate Trend")
        st.caption(
            "취소율, unavailable 비율, 실패율을 함께 비교하여 어떤 운영 이슈가 월별 안정성 변동을 설명하는지 확인합니다."
        )

        rate_df = monthly_df[["year_month"] + rate_cols].copy().set_index("year_month")
        st.line_chart(rate_df, use_container_width=True)

    with st.expander("원본 데이터 보기"):
        st.dataframe(df, use_container_width=True)