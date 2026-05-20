from typing import Any, Callable

import pandas as pd


def _render_empty_state(
    streamlit_module: Any,
    title: str,
    copy: str | None = None,
    safe_html: Callable[[str], str] | None = None,
) -> None:
    title_html = safe_html(title) if safe_html else title
    copy_html = ""
    if copy is not None:
        copy_text = safe_html(copy) if safe_html else copy
        copy_html = f'<div class="cp-empty-state-copy">{copy_text}</div>'
    streamlit_module.markdown(
        '<div class="cp-empty-state">'
        f'<div class="cp-empty-state-title">{title_html}</div>'
        f"{copy_html}"
        "</div>",
        unsafe_allow_html=True,
    )


def render_user_dataframe(
    df: pd.DataFrame,
    *,
    streamlit_module: Any,
    user_table_df: Callable[[pd.DataFrame, list[str] | None], pd.DataFrame],
    user_table_row_height: Callable[[pd.DataFrame], int],
    user_table_height: Callable[[pd.DataFrame, int], int | str],
    user_table_column_config: Callable[[pd.DataFrame, dict[str, Any] | None], dict[str, Any]],
    columns: list[str] | None = None,
    hide_index: bool = True,
    column_config: dict[str, Any] | None = None,
    key: str | None = None,
) -> None:
    table = user_table_df(df, columns)
    row_height = user_table_row_height(table)
    streamlit_module.dataframe(
        table,
        width="stretch",
        height=user_table_height(table, row_height),
        hide_index=hide_index,
        row_height=row_height,
        column_config=user_table_column_config(table, column_config),
        key=key,
    )


def render_risk_logic_note(*, streamlit_module: Any) -> None:
    streamlit_module.markdown(
        """
        <div class="cp-note">
            <div><strong>说明：</strong>结果用于帮助你快速筛选岗位。推荐评分越高，通常越应优先关注；风险提示越多，越需要进一步核实岗位职责、成长空间和投递成本。</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_state_alerts(
    *,
    streamlit_module: Any,
    session_state: Any,
    current_jd_fingerprint: Callable[[], str],
    current_resume_fingerprint: Callable[[], str],
) -> None:
    jd_fingerprint = current_jd_fingerprint()
    resume_fingerprint = current_resume_fingerprint()
    target_meta = session_state.get("target_jd_meta")
    if target_meta and jd_fingerprint:
        streamlit_module.caption(
            f"当前目标 JD：{target_meta.get('title', '目标JD')}｜来源：{target_meta.get('source', '')}｜更新时间：{target_meta.get('updated_at', '')}"
        )

    if session_state.get("resume_match"):
        stale_parts = []
        if session_state.get("resume_match_jd_fingerprint") != jd_fingerprint:
            stale_parts.append("目标 JD 已变化")
        if session_state.get("resume_match_resume_fingerprint") != resume_fingerprint:
            stale_parts.append("当前简历已变化")
        if stale_parts:
            streamlit_module.warning("当前简历匹配结果可能已过期：" + "、".join(stale_parts) + "。请重新运行简历匹配。")

    if session_state.get("custom_resume"):
        stale_parts = []
        if session_state.get("custom_resume_jd_fingerprint") != jd_fingerprint:
            stale_parts.append("目标 JD 已变化")
        if session_state.get("custom_resume_resume_fingerprint") != resume_fingerprint:
            stale_parts.append("当前简历已变化")
        if stale_parts:
            streamlit_module.info("定制简历片段可能已过期：" + "、".join(stale_parts) + "。建议重新生成。")


def render_workspace_heading(
    *,
    streamlit_module: Any,
    eyebrow: str,
    title: str,
    description: str | None = None,
    note: str | None = None,
) -> None:
    description_html = f'<div class="cp-workspace-copy">{description}</div>' if description else ""
    note_html = f'<div class="cp-mode-note">{note}</div>' if note else ""
    streamlit_module.markdown(
        f"""
        <div class="cp-workspace-head">
            <div>
                <div class="cp-workspace-eyebrow">{eyebrow}</div>
                <div class="cp-workspace-title">{title}</div>
                {description_html}
            </div>
            {note_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_section_banner(
    *,
    streamlit_module: Any,
    title: str,
    description: str | None = None,
    badge: str | None = None,
) -> None:
    badge_html = f'<div class="cp-section-badge">{badge}</div>' if badge else ""
    description_html = f'<div class="cp-section-copy">{description}</div>' if description else ""
    streamlit_module.markdown(
        f"""
        <div class="cp-section-banner">
            <div class="cp-section-heading">
                {badge_html}
                <div class="cp-section-title">{title}</div>
            </div>
            {description_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_jd_empty_state(*, streamlit_module: Any) -> None:
    streamlit_module.markdown(
        """
        <div class="cp-empty-state">
            <div class="cp-empty-state-title">等待导入岗位 JD</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_decision_empty_state(
    message: str,
    steps: list[str],
    *,
    streamlit_module: Any,
    safe_html: Callable[[str], str],
) -> None:
    # steps is retained for compatibility with older call sites.
    del steps
    _render_empty_state(streamlit_module, "还不能生成决策", message, safe_html)


def render_resume_empty_state(
    jd_analysis: dict[str, Any] | None,
    active_resume: dict[str, Any],
    has_match: bool,
    *,
    streamlit_module: Any,
    safe_html: Callable[[str], str],
) -> None:
    if not jd_analysis:
        title = "先选择一个目标岗位"
        copy = "在岗位工作台导入并分析一条JD后，这里会检查当前简历和岗位要求的匹配情况。"
    elif not active_resume.get("content", "").strip():
        title = "先上传或粘贴当前简历"
        copy = "在“简历与档案 > 简历管理”上传或保存一份真实简历后，再查看匹配分、优势证据和待补齐内容。"
    elif not has_match:
        title = "可以开始匹配"
        copy = "目标岗位和当前简历已准备好，点击下方按钮生成匹配结论。"
    else:
        return
    _render_empty_state(streamlit_module, title, copy, safe_html)
