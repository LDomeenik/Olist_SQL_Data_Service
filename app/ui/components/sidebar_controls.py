"""
사이드바 제어 패널 파일 - 경량 실사용 버전

주요 역할:
- Raw CSV 업로드 및 저장
- Raw / Output 파일 상태 표시
- 데이터 파이프라인 실행
- 캐시 리프레시
- 앱 상태 및 Output 파일 초기화

안정성 원칙:
- st.rerun() 사용하지 않음
- Refresh는 cache_data.clear()까지만 수행
- Reset은 session_state와 output 파일만 정리
- 과도한 상태 출력/복잡한 위젯 조합 제거
"""

import os
from datetime import datetime
from pathlib import Path

import streamlit as st

from app.config.settings import RAW_DATA_DIR, OUTPUT_DIR
from app.pipeline.loader import FILE_TABLE_MAP
from app.pipeline.pipeline_runner import run_pipeline


RAW_FILE_LIST = list(FILE_TABLE_MAP.keys())

OUTPUT_FILE_LIST = [
    "growth_structure.csv",
    "growth_drill_down.csv",
    "customer_value_structure.csv",
    "operational_stability.csv",
]


def save_uploaded_raw_files(uploaded_files) -> tuple[list[str], list[str]]:
    """
    업로드된 Raw CSV 파일을 지정된 경로(RAW_DATA_DIR)에 저장

    반환:
    - saved_files: 저장 성공 파일명 리스트
    - skipped_files: 예상 파일명과 달라서 skip된 파일명 리스트
    """
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    saved_files: list[str] = []
    skipped_files: list[str] = []
    expected_names = set(RAW_FILE_LIST)

    if not uploaded_files:
        return saved_files, skipped_files

    for uploaded_file in uploaded_files:
        if uploaded_file.name not in expected_names:
            skipped_files.append(uploaded_file.name)
            continue

        save_path = RAW_DATA_DIR / uploaded_file.name
        with open(save_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        saved_files.append(uploaded_file.name)

    return saved_files, skipped_files


def get_latest_modified_time(file_names: list[str], base_dir: Path) -> str:
    """
    파일 목록 중 가장 최근 수정 시간 반환
    """
    latest_time = None

    for file_name in file_names:
        file_path = base_dir / file_name
        if file_path.exists():
            file_time = os.path.getmtime(file_path)
            if latest_time is None or file_time > latest_time:
                latest_time = file_time

    if latest_time is None:
        return "No data"

    return datetime.fromtimestamp(latest_time).strftime("%Y-%m-%d %H:%M:%S")


def get_file_status(file_names: list[str], base_dir: Path) -> list[tuple[str, bool]]:
    """
    파일 존재 여부 반환
    """
    return [(file_name, (base_dir / file_name).exists()) for file_name in file_names]


def render_file_status_expander(title: str, file_status: list[tuple[str, bool]]) -> None:
    """
    파일 상태 expander 렌더링
    """
    complete = all(exists for _, exists in file_status) if file_status else False
    icon = "✅" if complete else "⚠️"

    with st.sidebar.expander(f"{title} {icon}", expanded=False):
        for file_name, exists in file_status:
            mark = "✅" if exists else "❌"
            st.write(f"{mark} {file_name}")


def render_sidebar_controls() -> None:
    """
    경량 실사용 사이드바 렌더링
    """
    st.sidebar.header("⚙️ Control Panel")

    # 세션 상태 초기값
    if "raw_imported" not in st.session_state:
        st.session_state["raw_imported"] = False

    if "pipeline_ready" not in st.session_state:
        st.session_state["pipeline_ready"] = False

    # 메시지 표시
    if "import_message" in st.session_state:
        st.sidebar.success(st.session_state["import_message"])
        del st.session_state["import_message"]

    if "import_warning" in st.session_state:
        st.sidebar.warning("일부 파일은 예상 파일명이 아니어서 제외되었습니다.")
        for name in st.session_state["import_warning"]:
            st.sidebar.write(f"⚠️ {name}")
        del st.session_state["import_warning"]

    if "pipeline_message" in st.session_state:
        st.sidebar.success(st.session_state["pipeline_message"])
        del st.session_state["pipeline_message"]

    if "reset_message" in st.session_state:
        st.sidebar.success(st.session_state["reset_message"])
        del st.session_state["reset_message"]

    # Data Import
    st.sidebar.markdown("### Data Import")
    st.sidebar.caption(
        "Raw CSV 파일을 업로드한 뒤 Import Raw Data 버튼으로 저장하세요."
    )

    uploaded_files = st.sidebar.file_uploader(
        "Upload raw CSV files",
        type=["csv"],
        accept_multiple_files=True,
        key="raw_file_uploader",
    )

    if uploaded_files:
        uploaded_names = {file.name for file in uploaded_files}
        expected_names = set(RAW_FILE_LIST)

        matched_files = sorted(uploaded_names & expected_names)
        unexpected_files = sorted(uploaded_names - expected_names)
        missing_expected_files = sorted(expected_names - uploaded_names)

        st.sidebar.caption(
            f"Selected: {len(uploaded_names)} / Expected: {len(expected_names)}"
        )

        if not unexpected_files and not missing_expected_files:
            st.sidebar.success("업로드 파일명이 모두 정상입니다.")
        else:
            st.sidebar.warning("업로드 파일명을 확인해 주세요.")

        with st.sidebar.expander("Upload Check", expanded=False):
            st.write(f"✅ Matched: {len(matched_files)}")
            for name in matched_files:
                st.write(f"✅ {name}")

            if unexpected_files:
                st.write(f"⚠️ Unexpected: {len(unexpected_files)}")
                for name in unexpected_files:
                    st.write(f"⚠️ {name}")

            if missing_expected_files:
                st.write(f"❌ Missing: {len(missing_expected_files)}")
                for name in missing_expected_files:
                    st.write(f"❌ {name}")

    if st.sidebar.button("📂 Import Raw Data", use_container_width=True):
        try:
            saved_files, skipped_files = save_uploaded_raw_files(uploaded_files)

            if saved_files:
                st.session_state["raw_imported"] = True
                st.session_state["pipeline_ready"] = False
                st.session_state["import_message"] = (
                    f"{len(saved_files)}개 파일을 저장했습니다."
                )
            else:
                st.sidebar.warning("저장된 파일이 없습니다.")

            if skipped_files:
                st.session_state["import_warning"] = skipped_files

        except Exception as e:
            st.sidebar.error(f"Raw data import failed: {e}")

    st.sidebar.divider()

    # File Status
    st.sidebar.markdown("### File Status")

    raw_status = get_file_status(RAW_FILE_LIST, RAW_DATA_DIR)
    output_status = get_file_status(OUTPUT_FILE_LIST, OUTPUT_DIR)

    render_file_status_expander("Raw Files", raw_status)
    render_file_status_expander("Output Files", output_status)

    st.sidebar.caption(f"Raw updated: {get_latest_modified_time(RAW_FILE_LIST, RAW_DATA_DIR)}")
    st.sidebar.caption(f"Output updated: {get_latest_modified_time(OUTPUT_FILE_LIST, OUTPUT_DIR)}")

    st.sidebar.divider()

    # Workflow Status
    st.sidebar.markdown("### Workflow Status")

    raw_imported = st.session_state.get("raw_imported", False)
    pipeline_ready = st.session_state.get("pipeline_ready", False)

    import_icon = "✅" if raw_imported else "⏳"
    pipeline_icon = "✅" if pipeline_ready else "⏳"

    st.sidebar.write(f"{import_icon} Raw Import Step")
    st.sidebar.write(f"{pipeline_icon} Pipeline Run Step")

    st.sidebar.divider()

    # Actions
    st.sidebar.markdown("### Actions")

    raw_complete = all(exists for _, exists in raw_status)

    if not raw_complete:
        st.sidebar.caption("필수 Raw CSV 파일이 모두 있어야 Pipeline 실행이 가능합니다.")
    else:
        st.sidebar.caption("Raw CSV 준비 완료. Pipeline을 실행할 수 있습니다.")

    if st.sidebar.button(
        "▶ Run Data Pipeline",
        use_container_width=True,
        disabled=not raw_complete,
    ):
        try:
            with st.spinner("Running data pipeline..."):
                run_pipeline()

            st.session_state["pipeline_ready"] = True
            st.cache_data.clear()
            st.session_state["pipeline_message"] = "Pipeline이 정상적으로 완료되었습니다."

        except Exception as e:
            st.sidebar.error(f"Data pipeline failed: {e}")

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        try:
            st.cache_data.clear()
            st.sidebar.success("데이터 캐시를 초기화했습니다.")
        except Exception as e:
            st.sidebar.error(f"Refresh failed: {e}")

    st.sidebar.divider()

    # Reset
    st.sidebar.markdown("### Reset")

    st.sidebar.caption("앱 상태와 Output CSV를 초기화합니다. Raw 원본 파일은 삭제하지 않습니다.")

    if st.sidebar.button("🗑 Reset App State", use_container_width=True):
        try:
            # 세션 플래그 초기화
            st.session_state["raw_imported"] = False
            st.session_state["pipeline_ready"] = False

            # 페이지 상태 초기화
            st.session_state["current_page"] = "Overview"

            # 메시지 상태 정리
            for key in [
                "import_message",
                "import_warning",
                "pipeline_message",
                "reset_message",
            ]:
                st.session_state.pop(key, None)

            # Output CSV 삭제
            deleted_cnt = 0
            for file_name in OUTPUT_FILE_LIST:
                output_path = OUTPUT_DIR / file_name
                if output_path.exists():
                    output_path.unlink()
                    deleted_cnt += 1

            # 캐시 초기화
            st.cache_data.clear()

            st.session_state["reset_message"] = (
                f"앱 상태를 초기화했고 Output 파일 {deleted_cnt}개를 삭제했습니다."
            )

        except Exception as e:
            st.sidebar.error(f"Reset failed: {e}")