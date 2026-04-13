"""
KPI 카드 공통 컴포넌트 파일

주요 역할:
- KPI 값을 카드 형태로 표시
- 숫자, 통화, 퍼센트 등 표시 형식을 일관되게 처리
- 여러 페이지에서 재사용 가능한 KPI UI 제공
"""

import streamlit as st
import math


def format_value(value, value_type: str = "number") -> str:
    """
    KPI 값을 지정된 형식으로 문자열 변환하는 함수

    지원 형식:
    - currency: 통화 (BRL 기준, K/M 단위 축약)
    - percent: 퍼센트
    - integer: 정수 (천 단위 구분 및 축약)
    - float: 소수점 2자리
    - number: 기본 숫자 포맷

    예외 처리:
    - None 또는 NaN 값은 "-"로 표시
    - 변환 실패 시 문자열 그대로 반환
    """
    if value is None:
        return "-"

    if isinstance(value, float) and math.isnan(value):
        return "-"

    try:
        if value_type == "currency":
            if abs(value) >= 1_000_000:
                return f"BRL {value / 1_000_000:.2f}M"
            if abs(value) >= 1_000:
                return f"BRL {value / 1_000:.1f}K"
            return f"BRL {value:,.2f}"

        if value_type == "percent":
            return f"{value:.2f}%"

        if value_type == "integer":
            if abs(value) >= 1_000_000:
                return f"{value / 1_000_000:.2f}M"
            if abs(value) >= 1_000:
                return f"{value / 1_000:.1f}K"
            return f"{int(value):,}"

        if value_type == "float":
            return f"{value:.2f}"

        return f"{value:,}"

    except Exception:
        return str(value)


def render_kpi_cards(metrics: list[dict]) -> None:
    """
    KPI 카드들을 가로 컬럼 형태로 렌더링하는 함수

    동작:
    - metrics 개수만큼 컬럼 생성
    - 각 KPI를 st.metric 형태로 표시
    """
    if not metrics:
        return

    cols = st.columns(len(metrics))

    for col, metric in zip(cols, metrics):
        label = metric.get("label", "")
        value = metric.get("value")
        value_type = metric.get("type", "number")
        help_text = metric.get("help")

        with col:
            st.metric(
                label=label,
                value=format_value(value, value_type=value_type),
                help=help_text,
            )