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
    핵심 인사이트 메시지를 강조 박스 형태로 렌더링하는 함수

    입력:
    - title: 인사이트 제목 (굵게 표시)
    - message: 상세 설명 텍스트
    - level: 메시지 유형
        - info (기본)
        - success
        - warning
        - error

    동작:
    - level에 따라 Streamlit의 메시지 컴포넌트로 출력
    - title은 강조 텍스트로 표시
    """
    content = f"**{title}**\n\n{message}"

    # level 유효성 보정
    valid_levels = {"info", "success", "warning", "error"}
    if level not in valid_levels:
        level = "info"

    if level == "success":
        st.success(content)
    elif level == "warning":
        st.warning(content)
    elif level == "error":
        st.error(content)
    else:
        st.info(content)