"""Batch JD screening logic for the CareerPilot Web API.

The screener models a senior HR + headhunter review: it ranks jobs by hard
match, resume evidence, user preferences, growth value, risk, and JD clarity.
It only returns recommendations; it never mutates resume, preference, or
application state.
"""

from __future__ import annotations

import re
from dataclasses import asdict
from typing import Any

from careerpilot.matching.jd_parser import build_structured_jd
from careerpilot.matching.preference_fit import calculate_preference_fit
from careerpilot_api.jd_service import _matching_services, normalize_multiline_text, normalize_text, text_contains
from careerpilot_api.schemas import JobOpportunity, MatchAssessment, ResumeEvidence


JsonDict = dict[str, Any]

NEXT_ACTION_BY_LEVEL = {
    "priority_apply": "立即投递",
    "apply_after_rewrite": "修改简历后投递",
    "cautious_apply": "补充项目证据",
    "backup": "仅作为备选",
    "not_recommended": "不建议投递",
}

RISK_TERMS = {
    "薪资或待遇不清晰": ["薪资面议", "待遇面议", "薪资未写", "薪酬面议", "unpaid"],
    "职责边界可能偏泛": ["综合支持", "其他事项", "领导安排", "多任务", "杂项"],
    "销售或业绩压力较强": ["销售指标", "业绩指标", "KPI", "陌拜", "地推"],
    "出差或到岗成本较高": ["长期出差", "频繁出差", "驻场", "外派"],
    "岗位稳定性需要确认": ["短期项目", "外包", "劳务派遣", "contractor"],
}

GROWTH_TERMS = [
    "培养",
    "导师",
    "轮岗",
    "核心项目",
    "数据驱动",
    "跨部门",
    "业务复盘",
    "增长",
    "平台",
    "training",
    "mentor",
]


def screen_batch_jds_for_api(payload: JsonDict) -> list[MatchAssessment]:
    jobs = _parse_jobs(payload)
    evidence_items = _parse_resume_evidence(payload.get("resume_evidence") or payload.get("evidence") or [])
    preferences = _dict_or_empty(payload.get("preferences") or payload.get("user_preferences"))
    assessments = [
        _assess_job(job, evidence_items, preferences)
        for job in jobs
    ]
    return _sort_and_limit_priority(assessments)


def _parse_jobs(payload: JsonDict) -> list[JobOpportunity]:
    raw_jobs = payload.get("jobs") or payload.get("jd_list") or payload.get("jds") or []
    if isinstance(raw_jobs, str):
        raw_jobs = [item for item in re.split(r"\n\s*---+\s*\n", raw_jobs) if item.strip()]
    output: list[JobOpportunity] = []
    for index, item in enumerate(raw_jobs, start=1):
        if isinstance(item, str):
            jd_text = normalize_multiline_text(item)
            output.append(
                JobOpportunity(
                    job_id=f"job-{index:03d}",
                    title=_first_line(jd_text) or "示例岗位",
                    company="示例公司",
                    location="待确认",
                    industry="待确认",
                    jd_text=jd_text,
                    source="批量粘贴",
                    captured_at="",
                )
            )
            continue
        if not isinstance(item, dict):
            continue
        jd_text = normalize_multiline_text(item.get("jd_text") or item.get("text") or item.get("description") or "")
        output.append(
            JobOpportunity(
                job_id=str(item.get("job_id") or item.get("id") or f"job-{index:03d}"),
                title=str(item.get("title") or _first_line(jd_text) or "示例岗位"),
                company=str(item.get("company") or "示例公司"),
                location=str(item.get("location") or "待确认"),
                industry=str(item.get("industry") or "待确认"),
                jd_text=jd_text,
                source=str(item.get("source") or "批量导入"),
                captured_at=str(item.get("captured_at") or ""),
            )
        )
    return output


def _parse_resume_evidence(value: Any) -> list[ResumeEvidence]:
    if isinstance(value, dict):
        value = value.get("items") or value.get("evidence_items") or []
    if not isinstance(value, list):
        return []
    output: list[ResumeEvidence] = []
    for index, item in enumerate(value, start=1):
        if isinstance(item, str):
            output.append(
                ResumeEvidence(
                    evidence_id=f"evidence-{index:03d}",
                    section="未分类",
                    original_text=normalize_text(item),
                    skills=[],
                    metrics=_extract_metrics(item),
                    confidence=0.55,
                )
            )
            continue
        if not isinstance(item, dict):
            continue
        original_text = normalize_text(item.get("original_text") or item.get("source_text") or item.get("text") or "")
        output.append(
            ResumeEvidence(
                evidence_id=str(item.get("evidence_id") or item.get("id") or f"evidence-{index:03d}"),
                section=str(item.get("section") or item.get("evidence_type") or "未分类"),
                original_text=original_text,
                skills=_string_list(item.get("skills") or item.get("keywords") or []),
                metrics=_string_list(item.get("metrics") or _extract_metrics(original_text)),
                confidence=_clamp_float(item.get("confidence"), default=0.6),
            )
        )
    return output


def _assess_job(job: JobOpportunity, evidence_items: list[ResumeEvidence], preferences: JsonDict) -> MatchAssessment:
    structured = _structured_jd(job)
    preference_fit = calculate_preference_fit(structured, preferences)
    preference_score = int(preference_fit.get("preference_fit_score", 70))
    clarity_score, clarity_risks = _jd_clarity(job, structured)
    hard_score, hard_missing = _hard_match_score(structured, evidence_items)
    evidence_score, weak_evidence, matched_terms = _evidence_score(structured, evidence_items)
    growth_score = _growth_value_score(job, structured)
    risk_score, risks = _risk_score(job, structured, preference_fit, clarity_risks)
    overall_score = _weighted_score(
        hard_score=hard_score,
        preference_score=preference_score,
        evidence_score=evidence_score,
        growth_score=growth_score,
        clarity_score=clarity_score,
        risk_score=risk_score,
    )
    level = _recommendation_level(
        overall_score=overall_score,
        hard_score=hard_score,
        evidence_score=evidence_score,
        preference_score=preference_score,
        growth_score=growth_score,
        risk_score=risk_score,
        clarity_score=clarity_score,
    )
    next_action = NEXT_ACTION_BY_LEVEL[level]
    return MatchAssessment(
        job_id=job.job_id,
        overall_score=overall_score,
        hard_match_score=hard_score,
        preference_fit_score=preference_score,
        evidence_score=evidence_score,
        growth_value_score=growth_score,
        risk_score=risk_score,
        recommendation_level=level,  # type: ignore[arg-type]
        recommendation_reason=_recommendation_reason(level, matched_terms, hard_missing, risks, clarity_score),
        risks=risks,
        missing_evidence=hard_missing[:6],
        weak_evidence=weak_evidence[:6],
        next_action=next_action,
    )


def _structured_jd(job: JobOpportunity) -> JsonDict:
    raw_text = job.jd_text
    jd_analysis = {
        "raw_text": raw_text,
        "basic": {
            "岗位名": job.title,
            "公司名": job.company,
            "地点": job.location,
        },
        "category": job.industry,
    }
    structured = build_structured_jd(jd_analysis, None, _matching_services())
    structured.setdefault("raw_text", raw_text)
    structured.setdefault("job_title", job.title)
    structured.setdefault("location", job.location)
    if not structured.get("industry_background") and job.industry and job.industry != "待确认":
        structured["industry_background"] = [job.industry]
    return structured


def _hard_match_score(structured: JsonDict, evidence_items: list[ResumeEvidence]) -> tuple[int, list[str]]:
    hard_requirements = _candidate_hard_requirements(structured)
    if not hard_requirements:
        return 72, []
    missing = [item for item in hard_requirements if not _has_evidence_for(item, evidence_items)]
    matched = len(hard_requirements) - len(missing)
    score = 38 + round(matched / max(len(hard_requirements), 1) * 62)
    if any(_looks_like_degree_requirement(item) for item in missing):
        score = min(score, 58)
    return _clamp_int(score), missing


def _evidence_score(structured: JsonDict, evidence_items: list[ResumeEvidence]) -> tuple[int, list[str], list[str]]:
    requirements = _core_terms(structured)
    if not requirements:
        return (48 if not evidence_items else 62), [], []
    matched: list[str] = []
    weak: list[str] = []
    weighted = 0.0
    for requirement in requirements:
        best = _best_evidence(requirement, evidence_items)
        if best is None:
            weak.append(requirement)
            continue
        strength = best.confidence + (0.15 if best.metrics else 0.0)
        if strength >= 0.75:
            weighted += 1.0
            matched.append(requirement)
        else:
            weighted += 0.55
            weak.append(requirement)
    score = round(weighted / max(len(requirements), 1) * 100)
    return _clamp_int(score), weak, matched


def _growth_value_score(job: JobOpportunity, structured: JsonDict) -> int:
    text = _combined_jd_text(job, structured)
    hits = sum(1 for term in GROWTH_TERMS if text_contains(text, term))
    score = 48 + min(hits * 9, 36)
    if len(structured.get("core_responsibilities", []) or []) >= 3:
        score += 8
    if text_contains(text, "核心") or text_contains(text, "owner"):
        score += 6
    return _clamp_int(score)


def _risk_score(
    job: JobOpportunity,
    structured: JsonDict,
    preference_fit: JsonDict,
    clarity_risks: list[str],
) -> tuple[int, list[str]]:
    text = _combined_jd_text(job, structured)
    risks = list(clarity_risks)
    for label, terms in RISK_TERMS.items():
        if any(text_contains(text, term) for term in terms):
            risks.append(label)
    for warning in preference_fit.get("preference_warnings", []) or []:
        clean = normalize_text(warning)
        if clean:
            risks.append(f"偏好冲突：{clean}")
    if not structured.get("hard_requirements"):
        risks.append("硬性要求未写清，学历、经验或到岗条件需要人工核对")
    has_responsibility_marker = bool(re.search(r"responsibilit|职责|工作内容|负责", text, flags=re.I))
    if not structured.get("core_responsibilities") and not has_responsibility_marker:
        risks.append("核心职责缺少可执行描述，难以判断真实工作内容")
    risks = _unique(risks)
    score = min(100, len(risks) * 14)
    if any("偏好冲突" in item for item in risks):
        score += 10
    return _clamp_int(score), risks[:8]


def _jd_clarity(job: JobOpportunity, structured: JsonDict) -> tuple[int, list[str]]:
    risks: list[str] = []
    text = normalize_text(job.jd_text)
    score = 35
    if len(text) >= 120:
        score += 18
    elif len(text) < 60:
        risks.append("JD正文过短，职责、要求和筛选口径不足")
    if job.title and job.title != "示例岗位":
        score += 8
    else:
        risks.append("岗位名称不明确")
    if job.company and job.company != "示例公司":
        score += 6
    if job.location and job.location != "待确认":
        score += 6
    else:
        risks.append("工作地点未明确")
    if structured.get("hard_requirements"):
        score += 12
    if structured.get("core_responsibilities"):
        score += 15
    return _clamp_int(score), risks


def _weighted_score(
    *,
    hard_score: int,
    preference_score: int,
    evidence_score: int,
    growth_score: int,
    clarity_score: int,
    risk_score: int,
) -> int:
    return _clamp_int(
        hard_score * 0.24
        + evidence_score * 0.24
        + preference_score * 0.18
        + growth_score * 0.16
        + clarity_score * 0.10
        + (100 - risk_score) * 0.08
    )


def _recommendation_level(
    *,
    overall_score: int,
    hard_score: int,
    evidence_score: int,
    preference_score: int,
    growth_score: int,
    risk_score: int,
    clarity_score: int,
) -> str:
    if hard_score < 45 or risk_score >= 76:
        return "not_recommended"
    if overall_score >= 82 and hard_score >= 70 and evidence_score >= 74 and preference_score >= 68 and risk_score <= 35:
        return "priority_apply"
    if overall_score >= 68 and evidence_score >= 55 and hard_score >= 62 and risk_score <= 58:
        return "apply_after_rewrite"
    if overall_score >= 56 and hard_score >= 55 and clarity_score >= 45:
        return "cautious_apply"
    if overall_score >= 45 or growth_score >= 68:
        return "backup"
    return "not_recommended"


def _sort_and_limit_priority(assessments: list[MatchAssessment]) -> list[MatchAssessment]:
    ranked = sorted(
        assessments,
        key=lambda item: (
            item.recommendation_level == "priority_apply",
            item.overall_score,
            item.hard_match_score,
            item.evidence_score,
            -item.risk_score,
        ),
        reverse=True,
    )
    priority_indexes = [index for index, item in enumerate(ranked) if item.recommendation_level == "priority_apply"]
    if len(priority_indexes) == len(ranked) and len(ranked) > 1:
        downgrade_from = max(1, len(ranked) // 3)
        for index in priority_indexes[downgrade_from:]:
            item = ranked[index]
            ranked[index] = MatchAssessment(
                **{
                    **asdict(item),
                    "recommendation_level": "apply_after_rewrite",
                    "next_action": NEXT_ACTION_BY_LEVEL["apply_after_rewrite"],
                    "recommendation_reason": f"{item.recommendation_reason}；批量筛选中仅保留最高优先级岗位为立即投递，其余建议先做简历定向微调。",
                }
            )
    return ranked


def _recommendation_reason(
    level: str,
    matched_terms: list[str],
    missing: list[str],
    risks: list[str],
    clarity_score: int,
) -> str:
    matched_text = "、".join(matched_terms[:3]) if matched_terms else "暂无强证据命中"
    if level == "priority_apply":
        return f"硬性条件和简历证据较稳，已看到可支撑的关键点：{matched_text}。"
    if level == "apply_after_rewrite":
        return f"岗位值得尝试，但投递前应把简历证据写得更贴近 JD；当前可用证据：{matched_text}。"
    if level == "cautious_apply":
        weak = "、".join(missing[:2]) if missing else (risks[0] if risks else "证据强度不足")
        return f"可以谨慎尝试，但需要先核对或补强：{weak}。"
    if level == "backup":
        risk = risks[0] if risks else "排序分不足以进入优先投递"
        return f"机会质量或匹配证据一般，适合作为备选；主要原因：{risk}。"
    blocker = "、".join(missing[:2]) if missing else (risks[0] if risks else "硬性匹配和证据不足")
    if clarity_score < 45:
        blocker = f"{blocker}；JD描述清晰度较低"
    return f"不建议投入优先投递成本，关键阻碍是：{blocker}。"


def _core_terms(structured: JsonDict) -> list[str]:
    terms: list[str] = []
    terms.extend(str(item) for item in structured.get("skills", []) or [])
    terms.extend(str(item) for item in structured.get("tools", []) or [])
    terms.extend(str(item) for item in structured.get("core_responsibilities", []) or [])
    terms.extend(_candidate_hard_requirements(structured))
    if not terms:
        hints = structured.get("template_hints") or {}
        terms.extend(str(item) for item in hints.get("skills", []) or [])
        terms.extend(str(item) for item in hints.get("responsibilities", []) or [])
    return _unique([normalize_text(item) for item in terms if normalize_text(item)])[:12]


def _candidate_hard_requirements(structured: JsonDict) -> list[str]:
    blocked_types = {"location", "salary", "company", "job_title"}
    blocked_text = ("工作地点", "地点", "城市", "薪资", "薪酬", "公司")
    output: list[str] = []
    for item in structured.get("hard_requirements", []) or []:
        if not isinstance(item, dict):
            continue
        requirement_type = str(item.get("type") or "")
        text = normalize_text(item.get("text") or item.get("requirement") or "")
        if not text or requirement_type in blocked_types:
            continue
        if any(label in text for label in blocked_text):
            continue
        output.append(text)
    return _unique(output)


def _best_evidence(requirement: str, evidence_items: list[ResumeEvidence]) -> ResumeEvidence | None:
    best: tuple[float, ResumeEvidence] | None = None
    for item in evidence_items:
        score = _evidence_match_strength(requirement, item)
        if score <= 0:
            continue
        combined = score * 0.65 + item.confidence * 0.35
        if best is None or combined > best[0]:
            best = (combined, item)
    return best[1] if best else None


def _has_evidence_for(requirement: str, evidence_items: list[ResumeEvidence]) -> bool:
    evidence = _best_evidence(requirement, evidence_items)
    return bool(evidence and _evidence_match_strength(requirement, evidence) >= 0.35 and evidence.confidence >= 0.45)


def _evidence_match_strength(requirement: str, evidence: ResumeEvidence) -> float:
    requirement = normalize_text(requirement)
    text = normalize_text(" ".join([evidence.original_text, " ".join(evidence.skills)]))
    if not requirement or not text:
        return 0.0
    if text_contains(text, requirement):
        return 1.0
    tokens = [item for item in re.split(r"[/,，、\s;；]+", requirement) if len(item) >= 2]
    if not tokens:
        return 0.0
    hits = sum(1 for token in tokens if text_contains(text, token))
    return min(1.0, hits / max(len(tokens), 1) + (0.18 if hits else 0.0))


def _combined_jd_text(job: JobOpportunity, structured: JsonDict) -> str:
    parts = [
        job.title,
        job.company,
        job.location,
        job.industry,
        job.jd_text,
        " ".join(str(item) for item in structured.get("skills", []) or []),
        " ".join(str(item) for item in structured.get("tools", []) or []),
        " ".join(str(item) for item in structured.get("core_responsibilities", []) or []),
    ]
    return normalize_text(" ".join(parts))


def _looks_like_degree_requirement(value: str) -> bool:
    return bool(re.search(r"本科|硕士|博士|bachelor|master|degree", value, flags=re.I))


def _extract_metrics(value: Any) -> list[str]:
    return re.findall(r"\d+(?:\.\d+)?%|\d+\s*(?:人|次|天|周|月|年|万元|小时|个)", str(value or ""))


def _string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        raw = value
    else:
        raw = re.split(r"[,，、;\s]+", str(value or ""))
    return _unique([normalize_text(item) for item in raw if normalize_text(item)])


def _dict_or_empty(value: Any) -> JsonDict:
    return value if isinstance(value, dict) else {}


def _first_line(value: str) -> str:
    for line in normalize_multiline_text(value).splitlines():
        if line.strip():
            return line.strip()[:60]
    return ""


def _unique(items: list[str]) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = normalize_text(item)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
    return output


def _clamp_int(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return int(max(minimum, min(maximum, round(value))))


def _clamp_float(value: Any, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        number = default
    return max(0.0, min(1.0, number))
