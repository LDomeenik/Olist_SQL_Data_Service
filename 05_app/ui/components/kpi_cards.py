"""
KPI 카드 공통 컴포넌트 파일

주요 역할:
- KPI 값을 카드 형태로 표시
- 숫자, 통화, 퍼센트 등 표시 형식을 일관되게 처리
- 여러 페이지에서 재사용 가능한 KPI UI 제공
"""

import streamlit as st


def format_value(value, value_type: str = "number") -> str:
    if value is None:
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