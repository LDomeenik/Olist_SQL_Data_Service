"""
사이드바 제어 패널 파일

주요 역할:
- Raw CSV 업로드 및 저장
- Raw / Output 데이터 워크플로우 상태 표시
- 실제 파일 존재 여부를 기반으로 완전성 표시
- 데이터 파이프라인 실행 및 새로고침
- 앱 상태 초기화와 사용자 액션 제어
"""

import os
from datetime import datetime
from pathlib import Path

import streamlit as st

from config.settings import DATA_DIR, OUTPUT_DIR
from pipeline.loader import FILE_TABLE_MAP
from pipeline.pipeline_manager import run_pipeline

RAW_FILE_LIST = list(FILE_TABLE_MAP.keys())

OUTPUT_FILE_LIST = [
    "growth_structure.csv",
    "growth_drill_down.csv",
    "customer_value_structure.csv",
    "operational_stability.csv",
]


def save_uploaded_raw_files(uploaded_files) -> tuple[list[str], list[str]]:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    saved_files = []
    skipped_files = []
    expected_names = set(RAW_FILE_LIST)

    for uploaded_file in uploaded_files:
        if uploaded_file.name not in expected_names:
            skipped_files.append(uploaded_file.name)
            continue

        save_path = DATA_DIR / uploaded_file.name
        with open(save_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        saved_files.append(uploaded_file.name)

    return saved_files, skipped_files


def get_latest_modified_time(file_names: list[str], base_dir: Path) -> str:
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
    return [(file_name, (base_dir / file_name).exists()) for file_name in file_names]


def get_dataset_status(
    file_names: list[str],
    base_dir: Path,
    step_completed: bool,
) -> tuple[str, list[tuple[str, bool]]]:
    """
    상태 반환:
    - not_started: 이번 세션에서 아직 단계 진행 안 함
    - partial: 단계는 진행했지만 파일이 일부만 존재
    - complete: 단계도 진행했고 파일도 모두 존재
    """
    file_status = get_file_status(file_names, base_dir)
    existing_count = sum(1 for _, exists in file_status if exists)
    expected_count = len(file_names)

    if not step_completed:
        return "not_started", file_status

    if existing_count < expected_count:
        return "partial", file_status

    return "complete", file_status


def render_dataset_expander(
    section_title: str,
    dataset_status: str,
    file_status: list[tuple[str, bool]],
    notice_text: str | None = None,
    expanded: bool = False,
) -> None:
    status_icon_map = {
        "not_started": "❌",
        "partial": "⚠️",
        "complete": "✅",
    }

    status_icon = status_icon_map.get(dataset_status, "❌")

    with st.sidebar.expander(f"{section_title} {status_icon}", expanded=expanded):
        if notice_text:
            st.caption(notice_text)

        if dataset_status == "not_started":
            for file_name, _ in file_status:
                st.write(f"❌ {file_name}")
            return

        for file_name, exists in file_status:
            icon = "✅" if exists else "❌"
            st.write(f"{icon} {file_name}")


def render_workflow_status(raw_imported: bool, pipeline_ready: bool) -> None:
    st.sidebar.markdown("### Workflow Status")

    import_icon = "✅" if raw_imported else "⏳"
    pipeline_icon = "✅" if pipeline_ready else "⏳"

    st.sidebar.write(f"{import_icon} Raw Import Step")
    st.sidebar.write(f"{pipeline_icon} Pipeline Run Step")


def render_sidebar_controls() -> None:
    st.sidebar.header("⚙️ Control Panel")

    raw_imported = st.session_state.get("raw_imported", False)
    pipeline_ready = st.session_state.get("pipeline_ready", False)

    # -----------------------------
    # 1) Data Import
    # -----------------------------
    st.sidebar.markdown("### Data Import")
    st.sidebar.caption(
        "• Raw CSV 파일을 업로드하세요.\n"
        "• 업로드한 파일은 내부 raw 폴더에 저장됩니다.\n"
        "• 파일명은 지정된 목록과 일치해야 합니다."
    )

    uploaded_files = st.sidebar.file_uploader(
        "Upload raw CSV files",
        type=["csv"],
        accept_multiple_files=True,
        label_visibility="collapsed",
    )

    if "import_message" in st.session_state:
        st.sidebar.success(st.session_state["import_message"])
        del st.session_state["import_message"]

    if "import_warning" in st.session_state:
        st.sidebar.warning("Unexpected files were skipped")
        for name in st.session_state["import_warning"]:
            st.sidebar.write(f"⚠️ {name}")
        del st.session_state["import_warning"]

    if uploaded_files:
        uploaded_names = {file.name for file in uploaded_files}
        expected_names = set(RAW_FILE_LIST)

        unexpected_files = sorted(uploaded_names - expected_names)
        missing_expected_files = sorted(expected_names - uploaded_names)
        matched_files = sorted(uploaded_names & expected_names)

        st.sidebar.caption(
            f"Selected: {len(uploaded_names)} / Expected: {len(expected_names)}"
        )

        if not unexpected_files and not missing_expected_files:
            st.sidebar.success("Upload check passed")
        else:
            st.sidebar.warning("Upload check needs review")

        with st.sidebar.expander("Upload Check", expanded=False):
            st.write(f"✅ Matched files: {len(matched_files)}")
            for name in matched_files:
                st.write(f"✅ {name}")

            if unexpected_files:
                st.write(f"⚠️ Unexpected files: {len(unexpected_files)}")
                for name in unexpected_files:
                    st.write(f"⚠️ {name}")

            if missing_expected_files:
                st.write(f"❌ Missing expected files: {len(missing_expected_files)}")
                for name in missing_expected_files:
                    st.write(f"❌ {name}")
    else:
        st.sidebar.caption(f"Expected raw files: {len(RAW_FILE_LIST)}")

    import_clicked = st.sidebar.button(
        "📂 Import Raw Data",
        use_container_width=True,
        disabled=not uploaded_files,
    )

    if import_clicked:
        saved_files, skipped_files = save_uploaded_raw_files(uploaded_files)

        if saved_files:
            st.session_state["raw_imported"] = True
            st.session_state["pipeline_ready"] = False
            st.session_state["import_message"] = (
                f"{len(saved_files)} file(s) imported successfully."
            )

        if skipped_files:
            st.session_state["import_warning"] = skipped_files

        st.rerun()

    # -----------------------------
    # 2) Data Status
    # -----------------------------
    st.sidebar.markdown("### Data Status")

    raw_status, raw_file_status = get_dataset_status(
        RAW_FILE_LIST,
        DATA_DIR,
        step_completed=raw_imported,
    )

    output_status, output_file_status = get_dataset_status(
        OUTPUT_FILE_LIST,
        OUTPUT_DIR,
        step_completed=pipeline_ready,
    )

    render_dataset_expander(
        "Raw Data",
        dataset_status=raw_status,
        file_status=raw_file_status,
        notice_text=(
            "• 체크 표시는 이번 세션의 진행 상태와 파일 완전성을 함께 반영합니다.\n"
            "• Import 전에는 전체가 ❌로 표시됩니다.\n"
            "• Import 후 누락 파일이 있으면 ⚠️로 표시됩니다."
        ),
        expanded=False,
    )

    render_dataset_expander(
        "Output Data",
        dataset_status=output_status,
        file_status=output_file_status,
        notice_text=(
            "• Pipeline 실행 전에는 전체가 ❌로 표시됩니다.\n"
            "• 실행 후 일부 결과만 생성되면 ⚠️로 표시됩니다."
        ),
        expanded=False,
    )

    raw_time = get_latest_modified_time(RAW_FILE_LIST, DATA_DIR)
    output_time = get_latest_modified_time(OUTPUT_FILE_LIST, OUTPUT_DIR)

    st.sidebar.caption(f"Raw updated: {raw_time}")
    st.sidebar.caption(f"Output updated: {output_time}")

    # -----------------------------
    # 3) Workflow Status
    # -----------------------------
    render_workflow_status(raw_imported=raw_imported, pipeline_ready=pipeline_ready)

    if raw_status == "not_started":
        st.sidebar.warning("Upload files and click 'Import Raw Data' to continue.")
    elif raw_status == "partial":
        st.sidebar.warning("Raw import completed, but some required files are missing.")
    elif output_status == "not_started":
        st.sidebar.warning("Run pipeline to generate outputs and load the dashboard.")
    elif output_status == "partial":
        st.sidebar.warning("Pipeline completed, but some output files are missing.")
    else:
        st.sidebar.success("All steps completed. Dashboard is ready.")

    # -----------------------------
    # 4) Actions
    # -----------------------------
    st.sidebar.markdown("### Actions")

    pipeline_disabled = raw_status != "complete"

    if raw_status == "not_started":
        st.sidebar.caption("먼저 Raw Data를 업로드하고 Import해야 합니다.")
    elif raw_status == "partial":
        st.sidebar.caption("필수 Raw Data 파일이 모두 준비되어야 파이프라인을 실행할 수 있습니다.")
    elif output_status in {"complete", "partial"}:
        st.sidebar.caption("기존 Output Data가 존재합니다. 실행 시 결과가 갱신됩니다.")
    else:
        st.sidebar.caption("Output Data를 생성하려면 데이터 파이프라인을 실행하세요.")

    if st.sidebar.button(
        "▶ Run Data Pipeline",
        use_container_width=True,
        disabled=pipeline_disabled,
    ):
        with st.spinner("Running data pipeline... Please wait."):
            try:
                run_pipeline()
                st.session_state["pipeline_ready"] = True
                st.cache_data.clear()
                st.sidebar.success("Data pipeline completed. Output data has been updated.")
                st.rerun()
            except Exception as e:
                st.sidebar.error(f"Data pipeline failed: {e}")

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # -----------------------------
    # 5) Danger Zone
    # -----------------------------
    st.sidebar.markdown("### Danger Zone")

    confirm_reset = st.sidebar.checkbox(
        "현재 앱 상태를 초기화합니다. 원본 파일은 삭제되지 않습니다."
    )

    if st.sidebar.button(
        "🗑 Reset Data",
        use_container_width=True,
        disabled=not confirm_reset,
    ):
        st.session_state["raw_imported"] = False
        st.session_state["pipeline_ready"] = False
        st.cache_data.clear()
        st.sidebar.success("앱 상태가 초기화되었습니다.")
        st.rerun()