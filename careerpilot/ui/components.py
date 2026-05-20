from __future__ import annotations

import html
from typing import Any, Iterable, Sequence

import pandas as pd
import streamlit as st


def _escape(value: object) -> str:
    return html.escape("" if value is None else str(value), quote=True)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, str):
        return [item.strip() for item in value.replace("，", ",").replace("；", ",").split(",") if item.strip()]
    return [value]


def _score_text(value: object) -> str:
    if value is None or value == "":
        return "--"
    try:
        return str(max(0, min(100, int(float(value or 0)))))
    except (TypeError, ValueError):
        return _escape(value)


def _score_level(score: object, level: str | None = None) -> str:
    text = str(level or "").lower()
    if any(token in text for token in ["high", "strong", "优先", "highly", "p1"]):
        return "high"
    if any(token in text for token in ["low", "risk", "不建议", "不足", "not_safe"]):
        return "low"
    try:
        value = int(float(score or 0))
    except (TypeError, ValueError):
        return "medium"
    if value >= 80:
        return "high"
    if value < 55:
        return "low"
    return "medium"


def _tag_tone(level: object) -> str:
    text = str(level or "").lower()
    if any(token in text for token in ["danger", "high", "risk", "red", "严重", "不建议"]):
        return "danger"
    if any(token in text for token in ["warning", "medium", "warn", "弱", "警示"]):
        return "warning"
    if any(token in text for token in ["success", "low", "green", "ok", "通过", "安全"]):
        return "success"
    return "info"


def _item_text(item: Any, *keys: str, fallback: str = "") -> str:
    if isinstance(item, dict):
        for key in keys:
            value = item.get(key)
            if value:
                return str(value)
        return fallback
    return str(item or fallback)


def render_app_shell_header() -> None:
    st.markdown('<div class="cp-shell-spacer"></div>', unsafe_allow_html=True)


def render_sidebar_brand(version: str = "local") -> None:
    st.sidebar.markdown(
        f"""
        <section class="cp-sidebar-brand">
            <div class="cp-sidebar-brand-row">
                <span class="cp-logo-mark">CP</span>
                <div>
                    <strong>CareerPilot</strong>
                    <em>{_escape(version)} version</em>
                </div>
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_status(user_name: str, resume_name: str, target_label: str) -> None:
    st.sidebar.markdown(
        f"""
        <section class="cp-sidebar-status-card">
            <div class="cp-sidebar-status-row"><span>当前用户</span><strong>{_escape(user_name)}</strong></div>
            <div class="cp-sidebar-status-row"><span>当前简历</span><strong>{_escape(resume_name)}</strong></div>
            <div class="cp-sidebar-status-row"><span>求职目标</span><strong>{_escape(target_label)}</strong></div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar_nav(options: dict[str, str], selected: str, *, key: str = "main_workspace") -> str:
    keys = list(options.keys())
    if selected not in keys and keys:
        selected = keys[0]
    return st.sidebar.radio(
        "\u200b",
        keys,
        index=keys.index(selected) if selected in keys else 0,
        format_func=lambda item: options.get(item, item),
        key=key,
        label_visibility="collapsed",
    )


def render_topbar(title: str, description: str, chips: Sequence[str] | None = None) -> None:
    chip_html = "".join(f'<span class="cp-topbar-chip">{_escape(chip)}</span>' for chip in (chips or []) if str(chip).strip())
    st.markdown(
        f"""
        <header class="cp-topbar">
            <div>
                <h1>{_escape(title)}</h1>
                <p>{_escape(description)}</p>
            </div>
            <div class="cp-topbar-chips">{chip_html}</div>
        </header>
        """,
        unsafe_allow_html=True,
    )


def render_segmented_nav(options: Iterable[str], selected: str, *, key: str | None = None) -> str:
    option_list = list(options)
    if selected not in option_list and option_list:
        selected = option_list[0]
    return st.radio(
        "\u200b",
        option_list,
        index=option_list.index(selected) if selected in option_list else 0,
        horizontal=True,
        label_visibility="collapsed",
        key=key,
    )


def render_workspace_card(title: str, subtitle: str | None = None, class_name: str = "") -> None:
    subtitle_html = f'<p>{_escape(subtitle)}</p>' if subtitle else ""
    st.markdown(
        f"""
        <section class="cp-workspace-card {_escape(class_name)}">
            <div class="cp-workspace-card-head">
                <h2>{_escape(title)}</h2>
                {subtitle_html}
            </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_empty_state(title: str, description: str, icon: str | None = None, compact: bool = True) -> None:
    del icon
    compact_class = " cp-empty-compact" if compact else ""
    st.markdown(
        f"""
        <div class="cp-empty-state{compact_class}">
            <div>
                <strong>{_escape(title)}</strong>
                <p>{_escape(description)}</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_score_pill(label: str, score: object, level: str | None = None) -> None:
    tone = _score_level(score, level)
    level_html = f'<em>{_escape(level)}</em>' if level else ""
    st.markdown(
        f"""
        <div class="cp-score-pill cp-score-{tone}">
            <span>{_escape(label)}</span>
            <strong>{_score_text(score)}</strong>
            {level_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_score_grid(items: Sequence[dict[str, Any] | tuple[Any, ...]]) -> None:
    cells: list[str] = []
    for item in items:
        if isinstance(item, dict):
            label = item.get("label", "")
            score = item.get("score", item.get("value"))
            level = item.get("level")
            subtitle = item.get("subtitle")
        else:
            label = item[0] if len(item) > 0 else ""
            score = item[1] if len(item) > 1 else ""
            level = item[2] if len(item) > 2 else None
            subtitle = item[3] if len(item) > 3 else None
        tone = _score_level(score, str(level) if level else None)
        cells.append(
            f"""
            <div class="cp-score-card cp-score-{tone}">
                <span>{_escape(label)}</span>
                <strong>{_score_text(score)}</strong>
                {f'<em>{_escape(level)}</em>' if level else ''}
                {f'<p>{_escape(subtitle)}</p>' if subtitle else ''}
            </div>
            """
        )
    st.markdown(f'<div class="cp-score-grid">{"".join(cells)}</div>', unsafe_allow_html=True)


def render_risk_tag(text: str, level: str = "danger") -> None:
    tone = _tag_tone(level)
    st.markdown(f'<span class="cp-risk-tag cp-risk-tag-{tone}">{_escape(text)}</span>', unsafe_allow_html=True)


def render_risk_tags(tags: Any) -> None:
    tag_items: list[tuple[str, str]] = []
    if isinstance(tags, dict):
        tag_items.extend(_result_risk_payload(tags))
    elif isinstance(tags, list):
        for item in tags:
            if isinstance(item, tuple) and len(item) >= 2:
                tag_items.append((str(item[0]), str(item[1])))
            elif isinstance(item, dict):
                tag_items.append((_item_text(item, "text", "reason", "warning", "requirement"), _item_text(item, "level", "severity", fallback="medium")))
            elif str(item).strip():
                tag_items.append((str(item), "medium"))
    elif tags:
        tag_items.append((str(tags), "medium"))

    seen: set[str] = set()
    html_items = ""
    for text, level in tag_items:
        text = str(text or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        html_items += f'<span class="cp-risk-tag cp-risk-tag-{_tag_tone(level)}">{_escape(text)}</span>'
    if not html_items:
        html_items = '<span class="cp-risk-tag cp-risk-tag-success">暂无明显风险</span>'
    st.markdown(f'<div class="cp-risk-tag-row">{html_items}</div>', unsafe_allow_html=True)


def render_decision_card(title: str, value: str, description: str | None = None) -> None:
    description_html = f'<p>{_escape(description)}</p>' if description else ""
    st.markdown(
        f"""
        <div class="cp-decision-card">
            <span>{_escape(title)}</span>
            <strong>{_escape(value)}</strong>
            {description_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_action_list(actions: Sequence[Any]) -> None:
    rows = ""
    for idx, action in enumerate([str(item).strip() for item in actions if str(item).strip()][:5], start=1):
        rows += f'<div class="cp-action-row"><span>{idx}</span><p>{_escape(action)}</p></div>'
    if not rows:
        render_empty_state("暂无下一步行动", "完成分析后会给出更具体的执行建议。")
        return
    st.markdown(f'<div class="cp-action-list">{rows}</div>', unsafe_allow_html=True)


def render_job_card(row: pd.Series | dict[str, Any], selected: bool = False) -> None:
    get = row.get if hasattr(row, "get") else dict(row).get
    selected_class = " is-selected" if selected else ""
    st.markdown(
        f"""
        <div class="cp-job-card{selected_class}">
            <div class="cp-job-card-top">
                <strong>{_escape(get("岗位") or get("岗位名称") or "未识别岗位")}</strong>
                <span>{_score_text(get("final_rank_score"))}</span>
            </div>
            <p>{_escape(get("公司") or "未识别公司")} · {_escape(get("recommendation_level") or get("投递建议") or "待判断")}</p>
            <div class="cp-job-card-meta">
                <em>简历 {_score_text(get("overall_score") or get("简历匹配分"))}</em>
                <em>方向 {_score_text(get("career_target_fit_score"))}</em>
                <em>偏好 {_score_text(get("preference_fit_score"))}</em>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_revision_card(title: str, before: str, after: str, reason: str | None = None) -> None:
    reason_html = f'<p>{_escape(reason)}</p>' if reason else ""
    st.markdown(
        f"""
        <div class="cp-revision-card">
            <strong>{_escape(title)}</strong>
            <div><span>原文</span><p>{_escape(before)}</p></div>
            <div><span>建议</span><p>{_escape(after)}</p></div>
            {reason_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _result_risk_payload(match_result: dict[str, Any]) -> list[tuple[str, str]]:
    tags: list[tuple[str, str]] = []
    for key, level in [
        ("career_mismatch_warnings", "high"),
        ("salary_mismatch_warnings", "high"),
        ("preference_warnings", "medium"),
        ("risk_tags", "medium"),
    ]:
        for item in _as_list(match_result.get(key)):
            text = _item_text(item, "text", "reason", "warning", fallback=str(item)).strip()
            if text:
                tags.append((text, level))
    for item in _as_list(match_result.get("missing_requirements"))[:2]:
        name = _item_text(item, "requirement", "jd_requirement", "target_requirement", "keyword", fallback=str(item)).strip()
        if name:
            tags.append((f"缺证据：{name}", "high"))
    for item in _as_list(match_result.get("weak_requirements"))[:2]:
        name = _item_text(item, "requirement", "jd_requirement", "target_requirement", "keyword", fallback=str(item)).strip()
        if name:
            tags.append((f"弱证据：{name}", "medium"))
    return tags


def render_result_panel_empty() -> None:
    render_empty_state(
        "还没有导入JD",
        "粘贴岗位描述后，会生成推荐分、风险点和下一步行动。",
        compact=True,
    )


def render_result_panel(match_result: dict[str, Any]) -> None:
    render_score_grid(
        [
            {"label": "推荐分", "score": match_result.get("final_rank_score"), "level": match_result.get("recommendation_level")},
            {"label": "简历匹配", "score": match_result.get("overall_score")},
            {"label": "职业方向", "score": match_result.get("career_target_fit_score")},
            {"label": "偏好匹配", "score": match_result.get("preference_fit_score")},
        ]
    )
    render_decision_card(
        "推荐结论",
        str(match_result.get("recommendation_level") or "待判断"),
        str(match_result.get("recommendation_reason") or "完成 JD 分析后，会在这里显示推荐结论。"),
    )
    st.markdown('<div class="cp-subsection-label">风险标签</div>', unsafe_allow_html=True)
    render_risk_tags(_result_risk_payload(match_result))
    st.markdown('<div class="cp-subsection-label">下一步行动</div>', unsafe_allow_html=True)
    render_action_list(_as_list(match_result.get("next_actions"))[:3])


def render_page_header(title: str, subtitle: str | None = None, chips: Sequence[str] | None = None) -> None:
    render_topbar(title, subtitle or "", chips)


def render_section_header(title: str, subtitle: str | None = None, right_text: str | None = None) -> None:
    right_html = f'<span>{_escape(right_text)}</span>' if right_text else ""
    st.markdown(
        f"""
        <div class="cp-section-title-block">
            <div>
                <h2>{_escape(title)}</h2>
                {f'<p>{_escape(subtitle)}</p>' if subtitle else ''}
            </div>
            {right_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_panel(title: str, subtitle: str | None = None, body_html: str | None = None, class_name: str = "") -> None:
    st.markdown(
        f"""
        <section class="cp-panel {_escape(class_name)}">
            <div class="cp-panel-head">
                <h3>{_escape(title)}</h3>
                {f'<p>{_escape(subtitle)}</p>' if subtitle else ''}
            </div>
            {body_html or ''}
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_score_badge(label: str, score: object, level: str | None = None) -> None:
    render_score_pill(label, score, level)


def render_result_score_grid(match_result: dict[str, Any]) -> None:
    render_score_grid(
        [
            ("岗位推荐分", match_result.get("final_rank_score"), match_result.get("recommendation_level")),
            ("简历匹配分", match_result.get("overall_score")),
            ("职业方向分", match_result.get("career_target_fit_score")),
            ("偏好匹配分", match_result.get("preference_fit_score")),
        ]
    )


def render_result_decision_card(match_result: dict[str, Any]) -> None:
    render_decision_card(
        "推荐结论",
        str(match_result.get("recommendation_level") or "待判断"),
        str(match_result.get("recommendation_reason") or "完成 JD 分析后，会在这里显示推荐结论。"),
    )


def render_next_actions(match_result: dict[str, Any]) -> None:
    render_action_list(_as_list(match_result.get("next_actions")))


def render_sidebar_profile_card(user_name: str, resume_name: str, target_label: str, version: str) -> None:
    render_sidebar_brand(version)
    render_sidebar_status(user_name, resume_name, target_label)


def render_job_rank_card(row: pd.Series | dict[str, Any], selected: bool = False) -> None:
    render_job_card(row, selected)


def render_table_toolbar(title: str, count: int | None = None, filters: Sequence[str] | None = None) -> None:
    filters_html = "".join(f'<span class="cp-topbar-chip">{_escape(item)}</span>' for item in (filters or []))
    count_html = f'<span class="cp-table-count">{int(count)} 条</span>' if count is not None else ""
    st.markdown(f'<div class="cp-table-toolbar"><strong>{_escape(title)}</strong>{count_html}<div>{filters_html}</div></div>', unsafe_allow_html=True)


def render_evidence_card(requirement: str, evidence: str, strength: object | None = None, explanation: str | None = None) -> None:
    render_revision_card(requirement or "匹配证据", evidence or "暂无可展示证据", str(strength or ""), explanation)


def render_gap_card(requirement: str, reason: str, suggestion: str | None = None, importance: str | None = None) -> None:
    render_decision_card(requirement or "待补齐要求", importance or "缺口", f"{reason or '暂无原因说明'} {suggestion or ''}".strip())


def render_rewrite_card(original_text: str, suggested_text: str, reason: str | None = None, risk_warning: str | None = None, mode: str = "safe") -> None:
    render_revision_card("简历改写", original_text, suggested_text, risk_warning or reason)


def render_compact_breakdown(score_breakdown: dict | list | None) -> None:
    if not score_breakdown:
        render_empty_state("暂无评分拆解", "完成分析后会显示各维度得分。")
        return
    items = score_breakdown.items() if isinstance(score_breakdown, dict) else enumerate(score_breakdown)
    render_score_grid([{"label": str(key), "score": value.get("score", value) if isinstance(value, dict) else value} for key, value in list(items)[:8]])


def render_revision_panel(revision_result: dict[str, Any], match_result: dict[str, Any] | None = None) -> None:
    match_reference = revision_result.get("match_reference", {}) or {}
    assessment = revision_result.get("revision_assessment", {}) or {}
    render_score_grid(
        [
            {"label": "可改写度", "score": revision_result.get("target_revision_feasibility_score") or assessment.get("target_revision_feasibility_score")},
            {"label": "简历匹配", "score": match_reference.get("overall_score") or (match_result or {}).get("overall_score")},
            {"label": "岗位推荐", "score": match_reference.get("final_rank_score") or (match_result or {}).get("final_rank_score")},
        ]
    )
    for item in _as_list(revision_result.get("experience_bullet_rewrites"))[:6]:
        render_rewrite_card(
            _item_text(item, "original_text", "resume_evidence"),
            _item_text(item, "suggested_text", "suggested_bullet"),
            _item_text(item, "reason"),
            _item_text(item, "risk_warning"),
        )


def info_card(title: str, body: str) -> None:
    st.markdown(f'<div class="cp-note-card cp-note-card-info"><strong>{_escape(title)}</strong><span>{_escape(body)}</span></div>', unsafe_allow_html=True)


def warning_card(title: str, body: str) -> None:
    st.markdown(f'<div class="cp-note-card cp-note-card-warning"><strong>{_escape(title)}</strong><span>{_escape(body)}</span></div>', unsafe_allow_html=True)


def section_title(title: str, subtitle: str | None = None) -> None:
    render_section_header(title, subtitle)


def compact_metric(label: str, value: object) -> None:
    st.markdown(f'<div class="cp-compact-metric"><span>{_escape(label)}</span><strong>{_escape(value)}</strong></div>', unsafe_allow_html=True)


def empty_state(title: str, description: str) -> None:
    render_empty_state(title, description)


def score_badge(score: int | float, label: str) -> None:
    render_score_badge(label, score)


def score_summary_card(title: str, score: int | float, subtitle: str | None = None, level: str | None = None) -> None:
    render_score_grid([{"label": title, "score": score, "subtitle": subtitle, "level": level}])


def evidence_card(requirement: str, evidence: str, strength: str | int | float, explanation: str | None = None) -> None:
    render_evidence_card(requirement, evidence, strength, explanation)


def gap_card(requirement: str, reason: str, suggestion: str | None = None, importance: str | None = None) -> None:
    render_gap_card(requirement, reason, suggestion, importance)


def risk_tag(text: str, level: str = "medium") -> None:
    render_risk_tag(text, level)


def rewrite_card(original_text: str, suggested_text: str, reason: str, risk_warning: str | None = None) -> None:
    render_rewrite_card(original_text, suggested_text, reason, risk_warning)


def compact_breakdown(score_breakdown: dict | list | None) -> None:
    render_compact_breakdown(score_breakdown)


def pill_nav(options: Iterable[str], selected: str, *, key: str | None = None) -> str:
    return render_segmented_nav(options, selected, key=key)
