from __future__ import annotations

from typing import Any


JsonDict = dict[str, Any]
CORE_REVISION_CATEGORIES = {
    "core_responsibility",
    "skill",
    "tool",
    "project_experience",
}
SUPPORTED_CORE_CATEGORIES = {"core_responsibility", "project_experience", "skill", "tool"}
STATUS_SCORES = {"STRONG": 100, "MEDIUM": 70, "WEAK": 35, "MISSING": 0}
IMPORTANCE_WEIGHTS = {"HIGH": 1.5, "MEDIUM": 1.0, "LOW": 0.4}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _first(items: list[Any], default: str = "") -> str:
    for item in items:
        text = _clean(item)
        if text:
            return text
    return default


def _requirements_by_status(requirement_evidence_map: list[JsonDict], statuses: set[str]) -> list[JsonDict]:
    return [item for item in requirement_evidence_map if item.get("evidence_status") in statuses]


def _primary_evidence(item: JsonDict) -> JsonDict:
    evidence = item.get("matched_evidence") or []
    return evidence[0] if evidence else {}


def _revision_level(score: int) -> str:
    if score >= 75:
        return "HIGHLY_CUSTOMIZABLE"
    if score >= 55:
        return "PARTIAL_CUSTOMIZATION"
    if score >= 35:
        return "LIMITED_CUSTOMIZATION"
    return "NOT_SAFE_TO_TARGET"


def calculate_revision_feasibility(
    requirement_evidence_map: list[dict],
    match_result: dict | None = None,
) -> dict:
    # match_result is intentionally ignored for compatibility with older callers.
    # Feasibility must be derived only from requirement_evidence_map; remove this
    # parameter after external call sites are migrated.
    core_records = [
        item for item in requirement_evidence_map
        if item.get("category") in CORE_REVISION_CATEGORIES
    ]
    weighted_total = 0.0
    weight_total = 0.0
    high_records = [item for item in core_records if item.get("importance") == "HIGH"]
    high_missing = [item for item in high_records if item.get("evidence_status") == "MISSING"]
    supported_core = [
        item for item in core_records
        if item.get("category") in SUPPORTED_CORE_CATEGORIES
        and item.get("evidence_status") in {"STRONG", "MEDIUM"}
    ]
    cap_reasons: list[str] = []
    for item in core_records:
        weight = IMPORTANCE_WEIGHTS.get(str(item.get("importance", "MEDIUM")), 1.0)
        weighted_total += STATUS_SCORES.get(str(item.get("evidence_status", "MISSING")), 0) * weight
        weight_total += weight
    score = int(round(weighted_total / max(weight_total, 1.0))) if core_records else 0
    high_missing_ratio = round(len(high_missing) / max(len(high_records), 1), 2) if high_records else 0.0
    if high_missing_ratio >= 0.6:
        score = min(score, 40)
        cap_reasons.append("High-priority missing ratio is at least 60%; cap feasibility at 40.")
    elif high_missing_ratio >= 0.4:
        score = min(score, 55)
        cap_reasons.append("High-priority missing ratio is at least 40%; cap feasibility at 55.")
    if not supported_core:
        score = min(score, 35)
        cap_reasons.append("No strong or medium evidence for core responsibility, project, skill, or tool requirements.")
    elif high_missing and len(supported_core) <= 2:
        score = min(score, 54)
        cap_reasons.append("Only one or two supported core records with high-priority missing requirements; cap feasibility at 54.")
    score = max(0, min(100, score))
    return {
        "target_revision_feasibility_score": score,
        "revision_level": _revision_level(score),
        "cap_reasons": cap_reasons,
        "high_missing_ratio": high_missing_ratio,
        "supported_core_count": len(supported_core),
    }


def _safe_keywords(keyword_coverage: JsonDict | None, requirement_evidence_map: list[JsonDict]) -> list[JsonDict]:
    coverage = keyword_coverage or {}
    candidates = list(coverage.get("must_have_keywords", []) or []) + list(coverage.get("important_keywords", []) or [])
    records_by_requirement = {
        _clean(item.get("requirement")): item
        for item in requirement_evidence_map
        if _clean(item.get("requirement"))
    }
    suggestions: list[JsonDict] = []
    seen: set[str] = set()
    for keyword in candidates:
        keyword = _clean(keyword)
        if not keyword or keyword in seen:
            continue
        seen.add(keyword)
        record = records_by_requirement.get(keyword)
        status = record.get("evidence_status") if record else "MISSING"
        evidence = _primary_evidence(record or {})
        direct_tool_evidence = (
            record
            and record.get("category") == "tool"
            and record.get("match_type") in {"EXACT_MATCH", "EVIDENCE_MATCH"}
            and evidence.get("source_text")
        )
        safe = bool(
            record
            and status in {"STRONG", "MEDIUM"}
            and (record.get("category") != "tool" or direct_tool_evidence)
        )
        suggestions.append(
            {
                "keyword": keyword,
                "insertion_location": "skills" if safe else "summary",
                "safe_to_add": safe,
                "reason": "Supported by strong or medium requirement evidence." if safe else "Evidence is weak or missing; do not add as a confident keyword.",
            }
        )
    return suggestions[:10]


def _advantage_cards(records: list[JsonDict]) -> list[JsonDict]:
    cards: list[JsonDict] = []
    for item in records:
        evidence = _primary_evidence(item)
        source = _clean(evidence.get("source_text"))
        requirement = _clean(item.get("requirement"))
        if not source or not requirement:
            continue
        cards.append(
            {
                "target_requirement": requirement,
                "resume_evidence": source,
                "evidence_strength": evidence.get("strength", 0.0),
                "suggested_expression": f"Use this as a supported proof point for: {requirement}",
            }
        )
    return cards[:6]


def _warning_items(items: list[JsonDict]) -> list[JsonDict]:
    return [
        {
            "requirement": _clean(item.get("requirement")),
            "warning": _clean(item.get("missing_reason")) or "No reliable resume evidence supports this requirement.",
            "safe_action": "Do not generate a resume bullet for this missing requirement. First collect real proof such as a project, course artifact, portfolio, certificate, or interview-ready example.",
        }
        for item in items
        if _clean(item.get("requirement"))
    ]


def generate_targeted_resume_revision(
    parsed_jd: dict,
    parsed_resume: dict,
    requirement_evidence_map: list[dict],
    keyword_coverage: dict | None = None,
    match_result: dict | None = None,
) -> dict:
    revision_assessment = calculate_revision_feasibility(requirement_evidence_map, match_result)
    revision_level = revision_assessment["revision_level"]
    strong = _requirements_by_status(requirement_evidence_map, {"STRONG"})
    medium = _requirements_by_status(requirement_evidence_map, {"MEDIUM"})
    weak = _requirements_by_status(requirement_evidence_map, {"WEAK"})
    missing = _requirements_by_status(requirement_evidence_map, {"MISSING"})
    job_title = _clean(parsed_jd.get("job_title")) or "selected JD"
    usable_records = strong if revision_level == "LIMITED_CUSTOMIZATION" else strong + medium
    advantage_cards = _advantage_cards(usable_records)
    supported_names = [card["target_requirement"] for card in advantage_cards[:5]]
    risky_names = [_clean(item.get("requirement")) for item in missing[:4] if _clean(item.get("requirement"))]

    if revision_level == "NOT_SAFE_TO_TARGET":
        return {
            "target_revision_feasibility_score": revision_assessment["target_revision_feasibility_score"],
            "revision_level": revision_level,
            "revision_assessment": revision_assessment,
            "target_positioning": "Evidence is insufficient for a safe targeted rewrite.",
            "revision_strategy": "Do not package this resume as a strong fit. Focus on gap confirmation, learning, and adding real evidence first.",
            "advantage_cards": [],
            "summary_revision": {
                "original_text": _first(parsed_resume.get("summary", []) or parsed_resume.get("profile", []) or []),
                "suggested_text": "",
                "reason": "Too many key target JD requirements are missing, so a confident summary would overstate fit.",
            },
            "skills_section_revision": {
                "skills_to_keep": [],
                "skills_to_add_if_true": [item.get("requirement") for item in weak if item.get("category") in {"skill", "tool"}][:8],
                "skills_to_remove_or_deemphasize": [item.get("requirement") for item in missing if item.get("category") in {"skill", "tool"}][:8],
                "ordering_suggestion": [],
            },
            "experience_bullet_rewrites": [],
            "keyword_insertion_suggestions": _safe_keywords(keyword_coverage, requirement_evidence_map),
            "missing_experience_warnings": _warning_items(missing[:8]),
            "interview_risk_points": [
                {
                    "topic": _clean(item.get("requirement")),
                    "risk_reason": "Missing evidence would likely be challenged in interviews.",
                    "prep_suggestion": "Prepare a truthful answer or build a real proof project before targeting this JD.",
                }
                for item in (missing[:5] + weak[:3])
            ],
            "unsupported_requirements": risky_names,
            "match_reference": {
                "overall_score": (match_result or {}).get("overall_score"),
                "recommendation_level": (match_result or {}).get("recommendation_level"),
            },
        }

    if supported_names:
        if revision_level == "LIMITED_CUSTOMIZATION":
            target_positioning = f"Limited alignment with {job_title}; use only verified evidence around: {' / '.join(supported_names[:3])}."
            summary_text = f"Limited fit for {job_title}: keep the summary narrow and evidence-based around {' / '.join(supported_names[:3])}."
        else:
            target_positioning = f"Position for {job_title} with verified evidence around: {' / '.join(supported_names[:4])}."
            summary_text = f"For {job_title}, summarize only verified evidence in {' / '.join(supported_names[:4])}, with concrete task, action, tool, and outcome details."
    else:
        target_positioning = ""
        summary_text = ""

    summary_revision = {
        "original_text": _first(parsed_resume.get("summary", []) or parsed_resume.get("profile", []) or []),
        "suggested_text": summary_text,
        "reason": "Uses only supported requirements; no generic fallback advantage is invented." if summary_text else "No supported advantage was found, so no confident summary rewrite is generated.",
    }
    keep_skills = [
        item.get("requirement")
        for item in requirement_evidence_map
        if item.get("category") in {"skill", "tool"} and item.get("evidence_status") in {"STRONG", "MEDIUM"}
    ]
    add_if_true = [
        item.get("requirement")
        for item in requirement_evidence_map
        if item.get("category") in {"skill", "tool"} and item.get("evidence_status") == "WEAK"
    ]
    remove_or_deemphasize = [
        item.get("requirement")
        for item in requirement_evidence_map
        if item.get("category") in {"skill", "tool"} and item.get("evidence_status") == "MISSING"
    ]
    bullet_records = strong if revision_level == "LIMITED_CUSTOMIZATION" else strong + medium
    bullet_rewrites: list[JsonDict] = []
    for item in bullet_records[:8]:
        evidence = _primary_evidence(item)
        source_text = _clean(evidence.get("source_text"))
        requirement = _clean(item.get("requirement"))
        if not source_text or not requirement:
            continue
        cautious = item.get("evidence_status") == "MEDIUM"
        bullet_rewrites.append(
            {
                "target_requirement": requirement,
                "original_bullet": source_text,
                "suggested_bullet": f"基于当前简历原文，可围绕「{requirement}」补充任务背景、个人动作、使用方法或工具、可验证结果；请只在真实经历存在时改写。原始证据：{source_text}",
                "evidence_source": evidence.get("evidence_type", ""),
                "evidence_strength": evidence.get("strength", 0.0),
                "rewrite_potential": item.get("rewrite_potential", "MEDIUM"),
                "risk_warning": "中等证据：表达应保持谨慎，避免声称未被原文支持的主导权或精通程度。" if cautious else "",
                "reason": "基于已有简历证据生成改写方向，不新增未证明经历。",
            }
        )
    for item in weak[:4]:
        evidence = _primary_evidence(item)
        bullet_rewrites.append(
            {
                "target_requirement": _clean(item.get("requirement")),
                "original_bullet": _clean(evidence.get("source_text")),
                "suggested_bullet": "当前证据较弱，只建议补充真实信息：任务背景、个人动作、使用方法或工具、可验证结果。不要写成已经稳定具备该能力。",
                "evidence_source": evidence.get("evidence_type", ""),
                "evidence_strength": evidence.get("strength", 0.0),
                "rewrite_potential": "LOW",
                "risk_warning": "弱证据：先补事实，不要改写成自信的目标 JD 经历句。",
                "reason": "只适合补充事实或清理表达，不适合生成可复制经历。",
            }
        )

    return {
        "target_revision_feasibility_score": revision_assessment["target_revision_feasibility_score"],
        "revision_level": revision_level,
        "revision_assessment": revision_assessment,
        "target_positioning": target_positioning,
        "revision_strategy": "Limited optimization only; preserve gaps." if revision_level == "LIMITED_CUSTOMIZATION" else "Strengthen true evidence first, then make supported keywords explicit.",
        "advantage_cards": advantage_cards,
        "summary_revision": summary_revision,
        "skills_section_revision": {
            "skills_to_keep": [item for item in keep_skills if item][:10],
            "skills_to_add_if_true": [item for item in add_if_true if item][:8],
            "skills_to_remove_or_deemphasize": [item for item in remove_or_deemphasize if item][:8],
            "ordering_suggestion": [item for item in keep_skills + add_if_true if item][:12],
        },
        "experience_bullet_rewrites": bullet_rewrites[:10],
        "keyword_insertion_suggestions": _safe_keywords(keyword_coverage, requirement_evidence_map),
        "missing_experience_warnings": _warning_items(missing[:8]),
        "interview_risk_points": [
            {
                "topic": _clean(item.get("requirement")),
                "risk_reason": "Evidence is missing or weak, so interviewers may challenge the detail.",
                "prep_suggestion": "Prepare a real example, or keep the resume wording modest.",
            }
            for item in (missing[:5] + weak[:3])
        ],
        "unsupported_requirements": risky_names,
        "match_reference": {
            "overall_score": (match_result or {}).get("overall_score"),
            "recommendation_level": (match_result or {}).get("recommendation_level"),
        },
    }
