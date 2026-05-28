"""Interview report service skeleton for the Web API."""

from __future__ import annotations

import re
from typing import Any

from careerpilot_api.schemas import ScoreItemSchema


def summarize_interview_for_api(payload: dict[str, Any]) -> dict[str, object]:
    text = _clean(payload.get("text") or payload.get("interview_text") or "")
    highlights = _highlights(text)
    risks = _risks(text)
    return {
        "summary": _summary(text, highlights, risks),
        "highlights": highlights,
        "risks": risks,
        "follow_up_actions": _follow_up_actions(text, risks),
        "scores": [
            ScoreItemSchema(id="report-completeness", label="记录完整度", value=_completeness(text), note="根据记录长度和问题线索估算"),
            ScoreItemSchema(id="report-risk", label="复盘风险", value=max(0, 100 - len(risks) * 18), note="用于提示是否需要补充复盘信息"),
        ],
    }


def _summary(text: str, highlights: list[str], risks: list[str]) -> str:
    if not text:
        return "尚未提供面试记录，当前返回通用报告骨架。"
    return f"已生成面试复盘占位摘要，提取到 {len(highlights)} 个亮点和 {len(risks)} 个风险提示。"


def _highlights(text: str) -> list[str]:
    if not text:
        return ["待补充面试亮点"]
    candidates = []
    if re.search(r"项目|成果|指标|impact|result", text, flags=re.I):
        candidates.append("记录中包含成果或项目线索。")
    if re.search(r"沟通|协作|stakeholder|communication", text, flags=re.I):
        candidates.append("记录中包含沟通协作线索。")
    if re.search(r"问题|question|面试官", text, flags=re.I):
        candidates.append("记录中包含可复盘的问题线索。")
    return candidates or ["已记录基础面试信息。"]


def _risks(text: str) -> list[str]:
    risks = []
    if not text:
        risks.append("尚未提供面试记录。")
    elif len(text) < 80:
        risks.append("面试记录较短，摘要需要人工确认。")
    if not re.search(r"问题|question|回答|answer", text, flags=re.I):
        risks.append("未识别到明确问答片段。")
    return risks or ["暂无明显复盘风险。"]


def _follow_up_actions(text: str, risks: list[str]) -> list[str]:
    actions = ["补充关键问题、回答摘要和面试官反馈。"]
    if text:
        actions.append("整理下一轮需要准备的证据和反问。")
    if risks and "暂无明显" not in risks[0]:
        actions.append("优先补全缺失记录后再生成正式报告。")
    return actions


def _completeness(text: str) -> int:
    if not text:
        return 20
    return min(100, 40 + min(len(text) // 20, 35))


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
