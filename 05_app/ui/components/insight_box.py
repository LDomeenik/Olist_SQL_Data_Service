"""
인사이트 박스 공통 컴포넌트 파일

주요 역할:
- 페이지별 핵심 인사이트 메시지를 강조 박스로 표시
- info / success / warning / error 레벨에 따라 시각적 구분 제공
- 여러 페이지에서 재사용 가능한 메시지 UI 제공
"""

import streamlit as st


def render_insight_box(
    title: str,
    message: str,
    level: str = "info",
) -> None:
    """
    level: info | success | warning | error
    """
    content = f"**{title}**\n\n{message}"

    if level == "success":
        st.success(content)
    elif level == "warning":
        st.warning(content)
    elif level == "error":
        st.error(content)
    else:
        st.info(content)