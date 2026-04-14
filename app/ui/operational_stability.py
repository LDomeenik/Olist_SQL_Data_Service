"""
운영 안정성 페이지 파일

주요 역할:
- 운영 안정성 관련 핵심 KPI를 요약 카드 형태로 표시
- 월별 실패율 및 운영 지표 추이를 시각화
- 매출 변동이 운영 이슈와 직접 연결되는지 진단
"""

import numpy as np
import pandas as pd
import streamlit as st

from app.ui.components.kpi_cards import render_kpi_cards, apply_kpi_metric_style
from app.ui.components.insight_box import render_insight_box


def _safe_mean(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None

    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None

    return s.mean()


def _safe_max(df: pd.DataFrame, col: str):
    if df is None or df.empty or col not in df.columns:
        return None

    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return None

    return s.max()


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


def render_operational_stability(ops_df: pd.DataFrame) -> None:
    """
    Operational Stability 페이지 전체를 렌더링하는 함수
    """
    st.subheader("Operational Stability")
    st.caption(
        "월별 취소율, unavailable 비율, 실패율 추이를 통해 운영 안정성의 흐름을 확인하고, "
        "매출 변동이 운영 이슈와 직접적으로 연결되는지 진단합니다."
    )

    apply_kpi_metric_style()

    if ops_df is None or ops_df.empty:
        st.warning("operational_stability.csv 파일이 없습니다.")
        return

    # -------------------------
    # 데이터 준비
    # -------------------------
    df = ops_df.copy()
    df = df.replace([np.inf, -np.inf], None)

    monthly_df = df.copy()

    if "row_type" in df.columns:
        monthly_df = df[df["row_type"] == "monthly_kpi"].copy()

    if monthly_df.empty:
        st.warning("monthly_kpi 데이터가 없습니다.")
        return

    if "year_month" in monthly_df.columns:
        monthly_df = monthly_df.sort_values("year_month").reset_index(drop=True)

    # -------------------------
    # KPI 카드 계산
    # -------------------------
    avg_cancel_rate = _safe_mean(monthly_df, "cancel_rate")
    avg_unavailable_rate = _safe_mean(monthly_df, "unavailable_rate")
    avg_failed_rate = _safe_mean(monthly_df, "failed_rate")
    max_failed_rate = _safe_max(monthly_df, "failed_rate")

    metrics = [
        {
            "label": "Avg Cancel Rate",
            "value": avg_cancel_rate * 100 if avg_cancel_rate is not None else None,
            "type": "percent",
        },
        {
            "label": "Avg Unavailable Rate",
            "value": avg_unavailable_rate * 100 if avg_unavailable_rate is not None else None,
            "type": "percent",
        },
        {
            "label": "Avg Failed Rate",
            "value": avg_failed_rate * 100 if avg_failed_rate is not None else None,
            "type": "percent",
        },
        {
            "label": "Max Failed Rate",
            "value": max_failed_rate * 100 if max_failed_rate is not None else None,
            "type": "percent",
        },
    ]

    render_kpi_cards(metrics)

    st.caption(
        "요약 지표 기준으로 보면, 취소율과 실패율은 전반적으로 안정적이며 "
        "매출 급락 구간에서도 운영 지표의 급격한 악화는 관찰되지 않습니다."
    )

    render_insight_box(
        title="Key Insight",
        message=(
            "거래 안정성 지표는 전반적으로 양호하며 시간에 따라 개선되는 흐름을 보입니다.\n\n"
            "특히 매출 급락 구간에서도 취소율과 실패율은 오히려 감소하거나 안정적으로 유지되어,\n\n"
            "매출 하락이 운영 불안정성 확대와는 직접적인 관련이 없음을 보여줍니다.\n\n"
            "따라서 매출 변동은 공급 또는 운영 문제가 아닌, 수요 및 고객 유입 변화에 의해 발생한 것으로 해석됩니다."
        ),
        level="info",
    )

    st.divider()

    # -------------------------
    # Failed Rate Trend
    # -------------------------
    failed_chart_df = _prepare_chart_df(
        monthly_df,
        x_col="year_month",
        y_cols=["failed_rate"],
    )

    if failed_chart_df is not None:
        st.markdown("#### Failed Rate Trend")
        st.caption(
            "월별 실패율 추이를 통해 운영 안정성이 전반적으로 개선되고 있는지, "
            "그리고 특정 시점에 이상 구간이 존재하는지 확인합니다."
        )

        try:
            st.line_chart(failed_chart_df, use_container_width=True)
        except Exception:
            st.warning("실패율 차트를 렌더링할 수 없습니다.")

    # -------------------------
    # Operational Stability Rate Trend
    # -------------------------
    rate_cols = [
        col for col in ["cancel_rate", "unavailable_rate", "failed_rate"]
        if col in monthly_df.columns
    ]

    if rate_cols:
        rate_chart_df = _prepare_chart_df(
            monthly_df,
            x_col="year_month",
            y_cols=rate_cols,
        )

        if rate_chart_df is not None:
            st.markdown("#### Operational Stability Rate Trend")
            st.caption(
                "취소율, unavailable 비율, 실패율을 함께 비교하여 "
                "어떤 운영 이슈가 월별 안정성 변동을 설명하는지 확인합니다.\n\n"
                "세 지표가 모두 안정적이라면 매출 하락의 원인은 운영보다 수요 측면일 가능성이 높습니다."
            )

            try:
                st.line_chart(rate_chart_df, use_container_width=True)
            except Exception:
                st.warning("운영 안정성 차트를 렌더링할 수 없습니다.")

    # -------------------------
    # 원본 데이터
    # -------------------------
    with st.expander("원본 데이터 보기"):
        st.dataframe(df.head(100), use_container_width=True)