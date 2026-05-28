"""Resume service skeleton for the Web API."""

from __future__ import annotations

import re
from typing import Any

from careerpilot_api.schemas import ActionItemSchema, ScoreItemSchema


KEYWORD_RULES = {
    "数据分析": ["数据分析", "SQL", "Python", "Excel", "指标", "看板", "dashboard"],
    "产品能力": ["产品", "需求", "PRD", "原型", "用户研究", "prototype"],
    "项目管理": ["项目", "推进", "交付", "协调", "PMO", "project"],
    "沟通协作": ["沟通", "协作", "汇报", "跨部门", "communication"],
    "市场运营": ["运营", "市场", "活动", "增长", "campaign", "marketing"],
}


def parse_resume_for_api(payload: dict[str, Any]) -> dict[str, object]:
    text = _clean(payload.get("text") or payload.get("resume_text") or "")
    sections = _extract_sections(text)
    keywords = _extract_keywords(text)
    warnings = _resume_warnings(text, sections, keywords)
    completeness = _completeness_score(text, sections, keywords)
    return {
        "summary": _resume_summary(text, sections, keywords),
        "sections": sections,
        "keywords": keywords or ["待确认关键词"],
        "warnings": warnings,
        "scores": [
            ScoreItemSchema(id="resume-completeness", label="完整度", value=completeness, note="根据文本长度、段落和关键词覆盖估算"),
            ScoreItemSchema(id="resume-keywords", label="关键词覆盖", value=min(100, 35 + len(keywords) * 12), note="用于后续匹配的占位评分"),
        ],
    }


def match_resume_for_api(payload: dict[str, Any]) -> dict[str, object]:
    resume_text = _clean(payload.get("resume_text") or payload.get("text") or "")
    jd_text = _clean(payload.get("jd_text") or payload.get("target_text") or "")
    resume_keywords = set(_extract_keywords(resume_text))
    jd_keywords = set(_extract_keywords(jd_text))
    matched = sorted(resume_keywords & jd_keywords)
    missing = sorted(jd_keywords - resume_keywords)
    if not jd_keywords:
        missing = ["目标岗位关键词待确认"]
    score = _match_score(resume_text, jd_text, matched, missing)
    risks = []
    if not resume_text:
        risks.append("尚未提供简历文本。")
    if not jd_text:
        risks.append("尚未提供目标岗位文本。")
    if missing and jd_keywords:
        risks.append("部分目标岗位关键词缺少简历证据。")
    return {
        "summary": f"当前简历与目标文本的占位匹配分为 {score}，后续可替换为真实匹配算法。",
        "match_score": score,
        "matched_keywords": matched or ["待确认匹配关键词"],
        "missing_keywords": missing,
        "risks": risks or ["暂无明显结构性风险，仍建议人工复核。"],
        "suggested_actions": [
            ActionItemSchema(id="resume-evidence", title="补充证据", detail="围绕缺失关键词补充真实经历、项目或成果。", priority="高"),
            ActionItemSchema(id="resume-language", title="贴近岗位表达", detail="将简历表述调整为目标岗位使用的关键词。", priority="中"),
        ],
    }


def _extract_sections(text: str) -> dict[str, str]:
    if not text:
        return {"overview": "尚未提供简历文本"}
    lines = [line.strip() for line in re.split(r"[\n\r]+", text) if line.strip()]
    return {
        "overview": lines[0][:160] if lines else text[:160],
        "experience": " / ".join(line for line in lines if re.search(r"项目|经历|实习|experience|project", line, flags=re.I))[:240],
        "skills": " / ".join(line for line in lines if re.search(r"技能|能力|skills?|tools?", line, flags=re.I))[:240],
    }


def _extract_keywords(text: str) -> list[str]:
    hits = [label for label, aliases in KEYWORD_RULES.items() if any(alias.lower() in text.lower() for alias in aliases)]
    return hits[:10]


def _resume_warnings(text: str, sections: dict[str, str], keywords: list[str]) -> list[str]:
    warnings = []
    if not text:
        warnings.append("尚未提供简历文本。")
    elif len(text) < 80:
        warnings.append("简历文本较短，解析结果需要人工确认。")
    if not sections.get("experience"):
        warnings.append("未识别到明确经历或项目段落。")
    if not keywords:
        warnings.append("未识别到稳定关键词。")
    return warnings or ["暂无明显解析风险。"]


def _resume_summary(text: str, sections: dict[str, str], keywords: list[str]) -> str:
    if not text:
        return "尚未提供简历文本，当前返回通用解析骨架。"
    return f"已生成简历解析占位结果，识别到 {len([value for value in sections.values() if value])} 个段落和 {len(keywords)} 个关键词。"


def _completeness_score(text: str, sections: dict[str, str], keywords: list[str]) -> int:
    if not text:
        return 20
    return min(100, 35 + min(len(text) // 20, 25) + len([value for value in sections.values() if value]) * 8 + len(keywords) * 6)


def _match_score(resume_text: str, jd_text: str, matched: list[str], missing: list[str]) -> int:
    if not resume_text or not jd_text:
        return 25
    base = 50 + len(matched) * 12 - max(0, len(missing) - 1) * 6
    return max(0, min(100, base))


def _clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()
