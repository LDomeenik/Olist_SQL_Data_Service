"""
KPI 카드 공통 컴포넌트 파일

주요 역할:
- KPI 값을 카드 형태로 표시
- 숫자, 통화, 퍼센트 등 표시 형식을 일관되게 처리
- 여러 페이지에서 재사용 가능한 KPI UI 제공
"""

import math
import streamlit as st
import math as _math


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

    if isinstance(value, float) and _math.isnan(value):
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


def apply_kpi_metric_style() -> None:
    """
    KPI 카드용 st.metric 스타일 적용

    목적:
    - KPI 값이 카드 폭보다 길 때 잘림 현상 완화
    - 창 크기에 따라 폰트가 너무 커지지 않도록 반응형 처리
    - label / value / delta 가독성 개선
    """
    st.markdown(
        """
        <style>
        div[data-testid="stMetric"] {
            background-color: transparent;
            padding: 0.2rem 0.1rem;
        }

        div[data-testid="stMetricLabel"] {
            font-size: clamp(0.72rem, 0.85vw, 0.95rem);
            line-height: 1.2;
        }

        div[data-testid="stMetricValue"] {
            font-size: clamp(1.3rem, 2.0vw, 2.1rem);
            line-height: 1.15;
            word-break: break-word;
            overflow-wrap: anywhere;
        }

        div[data-testid="stMetricDelta"] {
            font-size: clamp(0.68rem, 0.8vw, 0.88rem);
            line-height: 1.1;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_kpi_cards(metrics: list[dict], max_cols: int = 4) -> None:
    """
    KPI 카드들을 가로 컬럼 형태로 렌더링하는 함수

    동작:
    - 최대 max_cols 개씩 한 줄에 배치
    - 화면이 좁아질 때 카드 폭 부족으로 잘리는 현상 완화
    - st.metric 스타일을 반응형으로 적용
    """
    if not metrics:
        return

    apply_kpi_metric_style()

    max_cols = max(1, int(max_cols))
    row_count = math.ceil(len(metrics) / max_cols)

    for row_idx in range(row_count):
        row_metrics = metrics[row_idx * max_cols:(row_idx + 1) * max_cols]
        cols = st.columns(len(row_metrics))

        for col, metric in zip(cols, row_metrics):
            label = metric.get("label", "")
            value = metric.get("value")
            value_type = metric.get("type", "number")
            help_text = metric.get("help")
            delta = metric.get("delta")
            delta_color = metric.get("delta_color", "normal")

            with col:
                st.metric(
                    label=label,
                    value=format_value(value, value_type=value_type),
                    delta=delta,
                    delta_color=delta_color,
                    help=help_text,
                )