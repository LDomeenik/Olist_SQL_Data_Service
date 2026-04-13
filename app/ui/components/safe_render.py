import pandas as pd
import streamlit as st


def safe_section(title: str, caption: str | None = None) -> None:
    st.markdown(f"### {title}")
    if caption:
        st.caption(caption)


def safe_kpi_list(metrics: list[tuple[str, str]]) -> None:
    if not metrics:
        return

    lines = []
    for label, value in metrics:
        lines.append(f"- **{label}**: {value}")

    st.markdown("\n".join(lines))


def safe_text_preview(df: pd.DataFrame, title: str = "Preview", n: int = 10) -> None:
    st.markdown(f"#### {title}")

    if df is None or df.empty:
        st.info("표시할 데이터가 없습니다.")
        return

    preview = df.head(n).copy()

    for col in preview.columns:
        preview[col] = preview[col].astype(str)

    try:
        st.code(preview.to_csv(index=False), language="text")
    except Exception:
        st.write(preview.to_dict(orient="records"))