from __future__ import annotations

import re
from typing import Any

from .jd_parser import extract_match_industries, extract_match_tools
from .schema import JsonDict, MatchingServices, clamp_float


def line_capability(line: str, services: MatchingServices) -> str:
    group_hits = services.capability_group_hits(line)
    if group_hits:
        return max(group_hits, key=lambda group: len(group_hits[group]))
    for skill, aliases in services.skill_aliases.items():
        if any(services.text_contains(line, alias) for alias in aliases):
            return skill
    return "通用项目执行"


def evidence_strength(line: str, section: str, services: MatchingServices) -> float:
    clean = services.normalize_text(line)
    has_action = any(services.text_contains(clean, term) for term in services.resume_action_terms)
    has_result = any(services.text_contains(clean, term) for term in services.resume_result_terms)
    has_quant = bool(re.search(r"\d+(?:\.\d+)?\s*(?:%|人|次|个|份|天|周|月|年|万|千|k|K|元|小时|h|H|分)", clean))
    has_tool = bool(extract_match_tools(clean, services))
    has_capability = bool(services.capability_group_hits(clean))
    in_project_section = section in services.resume_project_sections or section == "全文"
    score = 0.12
    score += 0.22 if in_project_section else 0.08
    score += 0.20 if has_action else 0
    score += 0.16 if has_result else 0
    score += 0.18 if has_quant else 0
    score += 0.10 if has_tool else 0
    score += 0.08 if has_capability else 0
    if len(clean) >= 48:
        score += 0.06
    return clamp_float(score, 0.05, 1.0)


def build_structured_resume(resume_text: str, services: MatchingServices) -> JsonDict:
    sections = services.parse_resume_sections(resume_text)
    all_lines = services.normalize_resume_lines(resume_text)
    sectioned_lines = services.sectioned_resume_evidence_lines(resume_text)
    evidence_items: list[JsonDict] = []
    for section, line in sectioned_lines:
        clean = services.normalize_text(line)
        if len(clean) < 8:
            continue
        strength = evidence_strength(clean, section, services)
        if strength < 0.28 and section in services.resume_skill_list_sections:
            continue
        tools = extract_match_tools(clean, services)
        action = clean if any(services.text_contains(clean, term) for term in services.resume_action_terms) else ""
        result = clean if any(services.text_contains(clean, term) for term in services.resume_result_terms) or re.search(r"\d", clean) else ""
        evidence_items.append(
            {
                "source_text": clean,
                "normalized_capability": line_capability(clean, services),
                "tools_used": tools,
                "scenario": section,
                "action": action,
                "result": result,
                "strength": round(strength, 2),
            }
        )
    education = sections.get("教育背景", []) or services.first_matching_lines(all_lines, ["本科", "硕士", "研究生", "博士", "大学", "学院"], 4)
    work_experience = sections.get("实习/工作经历", []) or services.first_matching_lines(all_lines, ["实习", "工作", "公司", "负责", "参与"], 4)
    project_experience = sections.get("项目经历", []) + sections.get("科研/论文", [])
    certifications = services.first_matching_lines(all_lines, ["证书", "CET", "六级", "雅思", "托福", "CPA", "CFA", "FRM", "法考"], 4)
    return {
        "education": education[:8],
        "work_experience": work_experience[:12],
        "project_experience": project_experience[:14],
        "skills": services.resume_skill_hits(resume_text),
        "tools": extract_match_tools(resume_text, services),
        "certifications": certifications[:8],
        "industry_experience": extract_match_industries(resume_text, services),
        "achievements": [line for line in all_lines if re.search(r"\d+(?:\.\d+)?\s*(?:%|人|次|个|份|万|千|元)", line)][:12],
        "evidence_items": evidence_items[:40],
    }
