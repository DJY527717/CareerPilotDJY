from __future__ import annotations

import re
from statistics import mean
from typing import Any

from .jd_parser import MATCH_TOOL_ALIASES
from .keyword_coverage import requirement_best_evidence
from .schema import JsonDict, MatchingServices, SCORE_WEIGHTS, ScoreBreakdown, clamp_int, unique_items


EVIDENCE_STATUS_SCORE = {
    "STRONG": 95,
    "MEDIUM": 72,
    "WEAK": 42,
    "MISSING": 0,
}


def _score_records(records: list[JsonDict], default: int) -> int:
    if not records:
        return default
    weighted_total = 0.0
    weight_total = 0.0
    for item in records:
        importance = str(item.get("importance", "MEDIUM"))
        weight = 1.35 if importance == "HIGH" else 1.0 if importance == "MEDIUM" else 0.65
        weighted_total += EVIDENCE_STATUS_SCORE.get(str(item.get("evidence_status", "MISSING")), 0) * weight
        weight_total += weight
    return clamp_int(weighted_total / max(weight_total, 1.0))


def _matched_from_records(records: list[JsonDict]) -> list[JsonDict]:
    matched: list[JsonDict] = []
    for item in records:
        if item.get("evidence_status") not in {"STRONG", "MEDIUM"}:
            continue
        evidence = (item.get("matched_evidence") or [{}])[0]
        matched.append(
            {
                "jd_requirement": item.get("requirement", ""),
                "resume_evidence": evidence.get("source_text", ""),
                "match_type": item.get("match_type", ""),
                "strength": evidence.get("strength", 0),
                "explanation": evidence.get("explanation", ""),
            }
        )
    return matched


def _missing_from_records(records: list[JsonDict]) -> list[JsonDict]:
    missing: list[JsonDict] = []
    for item in records:
        if item.get("evidence_status") != "MISSING":
            continue
        missing.append(
            {
                "requirement": item.get("requirement", ""),
                "importance": item.get("importance", "MEDIUM"),
                "reason": item.get("missing_reason", "No reliable resume evidence was found."),
                "improvement_suggestion": "Only add this point after confirming a real experience, project, course artifact, or portfolio evidence.",
            }
        )
    return missing


def _weak_from_records(records: list[JsonDict]) -> list[JsonDict]:
    weak: list[JsonDict] = []
    for item in records:
        if item.get("evidence_status") != "WEAK":
            continue
        evidence = (item.get("matched_evidence") or [{}])[0]
        weak.append(
            {
                "requirement": item.get("requirement", ""),
                "current_evidence": evidence.get("source_text", ""),
                "problem": "Related evidence exists, but it lacks enough task, action, tool, or outcome detail.",
                "rewrite_suggestion": "Ask the user to add truthful context before turning this into a stronger bullet.",
            }
        )
    return weak


def _calculate_from_evidence_map(
    jd_structured: JsonDict,
    resume_structured: JsonDict,
    keyword_coverage: JsonDict,
    requirement_evidence_map: list[JsonDict],
    services: MatchingServices,
) -> JsonDict:
    by_category: dict[str, list[JsonDict]] = {}
    for item in requirement_evidence_map:
        by_category.setdefault(str(item.get("category", "")), []).append(item)
    hard_records = by_category.get("hard_requirement", [])
    core_records = by_category.get("core_responsibility", [])
    skill_records = by_category.get("skill", []) + by_category.get("tool", [])
    project_records = by_category.get("project_experience", [])
    industry_records = by_category.get("industry_background", [])
    hard_score = _score_records(hard_records, 78)
    core_score = _score_records(core_records, 70)
    skill_score = _score_records(skill_records, 68)
    project_score = _score_records(project_records, score_project_experience(jd_structured, resume_structured, services, fast=True))
    industry_score = _score_records(industry_records, 72)
    evidence_quality_score = score_evidence_quality(resume_structured)
    score_breakdown: ScoreBreakdown = {
        "hard_requirements_score": hard_score,
        "core_responsibility_score": core_score,
        "skill_tool_score": skill_score,
        "project_experience_score": project_score,
        "industry_background_score": industry_score,
        "evidence_quality_score": evidence_quality_score,
    }
    ceiling_reasons: list[str] = []
    high_risk_hard = [item.get("requirement", "") for item in hard_records if item.get("risk_level") == "HIGH"]
    if high_risk_hard:
        ceiling_reasons.append("High-risk hard requirements are missing; overall_score capped at 70.")
    weighted_score = sum(score_breakdown[key] * weight for key, weight in SCORE_WEIGHTS.items())
    overall_score = clamp_int(weighted_score)
    if high_risk_hard:
        overall_score = min(overall_score, 70)
    must_have_missing = [
        item.get("requirement", "")
        for item in skill_records
        if item.get("importance") == "HIGH" and item.get("evidence_status") == "MISSING"
    ]
    tool_keys = {services.normalize_text(tool) for tool in MATCH_TOOL_ALIASES}
    missing_must_have_tools = [keyword for keyword in must_have_missing if services.normalize_text(keyword) in tool_keys]
    if len(missing_must_have_tools) >= 2 and skill_score < 30:
        overall_score = min(overall_score, 42)
        ceiling_reasons.append("Multiple must-have tools are missing; overall_score capped at 42.")
    if jd_structured.get("job_family") == "software_engineering" and skill_score < 20 and core_score < 45:
        overall_score = min(overall_score, 40)
        ceiling_reasons.append("Engineering role lacks implementation evidence; overall_score capped at 40.")
    return {
        "overall_score": overall_score,
        "score_breakdown": score_breakdown,
        "hard_missing": _missing_from_records(hard_records),
        "hard_weak": _weak_from_records(hard_records),
        "core_matched": _matched_from_records(core_records),
        "core_missing": _missing_from_records(core_records),
        "core_weak": _weak_from_records(core_records),
        "skill_matched": _matched_from_records(skill_records),
        "skill_missing": _missing_from_records(skill_records),
        "skill_weak": _weak_from_records(skill_records),
        "ceiling_reasons": ceiling_reasons,
    }


def score_hard_requirements(jd_structured: JsonDict, resume_structured: JsonDict, resume_text: str, services: MatchingServices) -> tuple[int, list[JsonDict], list[JsonDict], list[str]]:
    requirements = jd_structured.get("hard_requirements", [])
    if not requirements:
        return 78, [], [], []
    clean_resume = services.normalize_text(resume_text)
    results: list[JsonDict] = []
    missing: list[JsonDict] = []
    ceilings: list[str] = []
    for req in requirements:
        req_type = req.get("type", "general")
        text = req.get("text", "")
        score = 75
        reason = "简历未清晰写出，但也未看到明显冲突。"
        if req_type == "education":
            if "博士" in text:
                ok = any(term in clean_resume for term in ["博士", "PhD"])
            elif "硕士" in text or "研究生" in text:
                ok = any(term in clean_resume for term in ["硕士", "研究生", "博士", "master", "phd"])
            elif "本科" in text:
                ok = any(term in clean_resume for term in ["本科", "学士", "硕士", "研究生", "博士", "bachelor", "master"])
            else:
                ok = any(term in clean_resume for term in ["本科", "硕士", "研究生", "博士", "大专"])
            score = 100 if ok else 30
            reason = "学历线索匹配。" if ok else "没有看到对应学历线索。"
            if not ok:
                ceilings.append("学历要求明显不满足或未写清，overall_score 最多 70")
        elif req_type == "experience":
            years = [int(x) for x in re.findall(r"\d+", text)]
            has_work_or_project = bool(resume_structured.get("work_experience") or resume_structured.get("project_experience"))
            if "经验不限" in text or "应届" in text or "校招" in text or "实习" in text:
                score = 90 if has_work_or_project else 75
                reason = "岗位接受学生/早期经历，项目或实习可替代。"
            elif years and max(years) <= 3 and has_work_or_project:
                score = 75
                reason = "工作年限要求不高，实习/项目经历可部分替代，但建议写清时长和角色。"
            elif years and max(years) >= 4 and not services.text_contains(clean_resume, "年"):
                score = 35
                reason = "工作年限要求较高，简历中没有足够替代证据。"
                ceilings.append("工作年限差距过大且无替代经历，overall_score 最多 65")
            else:
                score = 60 if has_work_or_project else 35
                reason = "有经历基础，但年限或成熟度表达不清。"
        elif req_type == "language":
            ok = any(services.text_contains(clean_resume, term) for term in services.english_evidence_terms)
            score = 100 if ok else 20
            reason = "有英文/语言证据。" if ok else "JD 明确提到语言要求，但简历未看到证据。"
            if not ok:
                ceilings.append("语言等强限制不满足，overall_score 最多 60")
        elif req_type == "certificate":
            aliases = re.findall(r"CPA|CFA|FRM|基金从业|证券从业|法考|法律职业资格|证书|资格", text, flags=re.I)
            ok = any(services.text_contains(clean_resume, alias) for alias in aliases)
            score = 100 if ok else 20
            reason = "证书/资格线索匹配。" if ok else "JD 明确要求证书或资格，简历未看到。"
            if not ok and any(marker in text for marker in ["必须", "需", "要求", "持有"]):
                ceilings.append("关键证书缺失且 JD 明确必须，overall_score 最多 70")
        elif req_type == "location":
            score = 75
            reason = "地点通常需要人工确认；简历未写清时按不确定处理。"
        elif req_type == "availability":
            ok = bool(re.search(r"每周\s*\d\s*天|到岗|实习|个月", clean_resume))
            score = 90 if ok else 55
            reason = "可到岗/实习时长有线索。" if ok else "实习时长或到岗时间未写清。"
        result = {"requirement": text, "type": req_type, "score": int(score), "reason": reason}
        results.append(result)
        if score < 55:
            missing.append(result)
    average = sum(int(item["score"]) for item in results) / max(len(results), 1)
    return clamp_int(average), missing, [item for item in results if int(item["score"]) < 80], ceilings


def score_requirement_list(requirements: list[str], evidence_items: list[JsonDict], services: MatchingServices, *, fast: bool) -> tuple[int, list[JsonDict], list[JsonDict], list[JsonDict]]:
    if not requirements:
        return 70, [], [], []
    scores: list[int] = []
    matched: list[JsonDict] = []
    missing: list[JsonDict] = []
    weak: list[JsonDict] = []
    for req in requirements:
        item, strength, match_type = requirement_best_evidence(req, evidence_items, services, fast=fast)
        if match_type == "EXACT_MATCH" and strength >= 0.68:
            score = 100
        elif match_type in {"SEMANTIC_MATCH", "EVIDENCE_MATCH"} and strength >= 0.56:
            score = 75
        elif match_type != "MISSING" and strength >= 0.38:
            score = 50
        elif match_type != "MISSING":
            score = 30
        else:
            score = 0
        scores.append(score)
        if score >= 55 and item:
            matched.append(
                {
                    "jd_requirement": req,
                    "resume_evidence": item.get("source_text", ""),
                    "match_type": match_type,
                    "strength": round(float(max(0, min(1, strength))), 2),
                    "explanation": "有简历原文证据支撑该要求。" if score >= 75 else "方向相关，但证据还不够完整。",
                }
            )
        elif item and score > 0:
            weak.append(
                {
                    "requirement": req,
                    "current_evidence": item.get("source_text", ""),
                    "problem": "有相关表达，但缺少明确场景、动作、工具或结果。",
                    "rewrite_suggestion": f"把这段经历改成：围绕「{req}」，说明任务背景、你采取的动作、使用的方法/工具和量化结果。",
                }
            )
        else:
            missing.append(
                {
                    "requirement": req,
                    "importance": "HIGH",
                    "reason": "没有找到能直接支撑该 JD 要求的简历证据。",
                    "improvement_suggestion": f"补充一条真实经历，明确写出与「{req}」相关的任务、动作、交付物和结果。",
                }
            )
    return clamp_int(sum(scores) / max(len(scores), 1)), matched, missing, weak


def score_skill_tools(jd_structured: JsonDict, resume_text: str, evidence_items: list[JsonDict], keyword_coverage: JsonDict, services: MatchingServices, *, fast: bool) -> tuple[int, list[JsonDict], list[JsonDict], list[JsonDict]]:
    requirements = unique_items(jd_structured.get("skills", []) + jd_structured.get("tools", []), services, 20)
    if not requirements:
        return 68, [], [], []
    exact_keywords = {item["keyword"] for item in keyword_coverage.get("exact_matches", [])}
    semantic_keywords = {item["keyword"] for item in keyword_coverage.get("semantic_matches", [])}
    evidence_keywords = {item["keyword"] for item in keyword_coverage.get("evidence_matches", [])}
    scores: list[int] = []
    matched: list[JsonDict] = []
    missing: list[JsonDict] = []
    weak: list[JsonDict] = []
    for keyword in requirements:
        item, strength, match_type = requirement_best_evidence(keyword, evidence_items, services, fast=fast)
        is_tool_requirement = services.normalize_text(keyword) in MATCH_TOOL_ALIASES
        if keyword in exact_keywords and item and float(item.get("strength", 0)) >= 0.55:
            score = 100
        elif is_tool_requirement:
            score = 0
        elif keyword in semantic_keywords or keyword in evidence_keywords:
            score = 80 if item and float(item.get("strength", 0)) >= 0.48 else 65
        elif keyword in exact_keywords:
            score = 50
        else:
            score = 0
        scores.append(score)
        if score >= 70 and item:
            matched.append(
                {
                    "jd_requirement": keyword,
                    "resume_evidence": item.get("source_text", ""),
                    "match_type": match_type if match_type != "MISSING" else "EVIDENCE_MATCH",
                    "strength": round(float(max(0, min(1, strength))), 2),
                    "explanation": "技能/工具不仅出现，还能在经历中看到使用场景。",
                }
            )
        elif score > 0:
            weak.append(
                {
                    "requirement": keyword,
                    "current_evidence": item.get("source_text", "") if item else "技能栏或简历文本中提到过，但缺少使用场景。",
                    "problem": "关键词有覆盖，但项目证据不足。",
                    "rewrite_suggestion": f"补一条使用 {keyword} 完成任务、产出结果的经历句。",
                }
            )
        else:
            missing.append(
                {
                    "requirement": keyword,
                    "importance": "HIGH" if keyword in keyword_coverage.get("must_have_keywords", []) else "MEDIUM",
                    "reason": "没有找到直接或语义等价的能力证据。",
                    "improvement_suggestion": f"如果真实具备 {keyword}，请把它写进项目动作和交付结果，而不是只放在技能清单。",
                }
            )
    return clamp_int(sum(scores) / max(len(scores), 1)), matched, missing, weak


def score_project_experience(jd_structured: JsonDict, resume_structured: JsonDict, services: MatchingServices, *, fast: bool) -> int:
    evidence_items = resume_structured.get("evidence_items", [])
    project_like = [
        item for item in evidence_items
        if item.get("scenario") in services.resume_project_sections
        or "项目" in str(item.get("source_text", ""))
        or "实习" in str(item.get("source_text", ""))
        or ("负责" in str(item.get("source_text", "")) and float(item.get("strength", 0)) >= 0.55)
    ]
    if not project_like:
        return 20 if evidence_items else 8
    requirements = jd_structured.get("core_responsibilities", []) + jd_structured.get("project_experience", [])
    strong_related = 0
    related = 0
    strong_sources: set[str] = set()
    related_sources: set[str] = set()
    for req in requirements[:10]:
        item, strength, _match_type = requirement_best_evidence(req, project_like, services, fast=fast)
        if item and strength >= 0.62:
            strong_related += 1
            strong_sources.add(str(item.get("source_text", "")))
        elif item and strength >= 0.38:
            related += 1
            related_sources.add(str(item.get("source_text", "")))
    quantified = sum(1 for item in project_like if re.search(r"\d", str(item.get("source_text", ""))))
    has_result = any(any(services.text_contains(str(item.get("source_text", "")), term) for term in services.resume_result_terms) for item in project_like)
    if strong_related >= 2 and len(strong_sources) >= 2:
        base = 92
    elif strong_related >= 1:
        base = 82 if quantified or has_result else 64
    elif related and len(related_sources) >= 2:
        base = 66
    elif related:
        base = 62
    else:
        base = 38
    if quantified:
        base += min(8, quantified * 3)
    return clamp_int(base)


def score_industry_background(jd_structured: JsonDict, resume_structured: JsonDict, resume_text: str, services: MatchingServices) -> int:
    jd_industries = jd_structured.get("industry_background", [])
    if not jd_industries:
        return 72
    resume_industries = resume_structured.get("industry_experience", [])
    if set(jd_industries) & set(resume_industries):
        return 94
    jd_text = " ".join(jd_industries)
    if any(services.capability_overlap_similarity(jd_text, item) >= 0.4 for item in resume_industries):
        return 76
    if any(services.text_contains(resume_text, item) for item in jd_industries):
        return 70
    if resume_structured.get("project_experience"):
        return 52
    return 18


def score_evidence_quality(resume_structured: JsonDict) -> int:
    evidence_items = resume_structured.get("evidence_items", [])
    if not evidence_items:
        return 10
    strengths = [float(item.get("strength", 0)) for item in evidence_items]
    strong = sum(1 for value in strengths if value >= 0.72)
    quantified = len(resume_structured.get("achievements", []))
    score = mean(strengths[:12]) * 82 + min(strong * 4, 12) + min(quantified * 3, 12)
    return clamp_int(score)


def apply_hard_ceilings(score: int, ceiling_reasons: list[str]) -> int:
    ceiling = 100
    for reason in ceiling_reasons:
        if "最多 60" in reason:
            ceiling = min(ceiling, 60)
        elif "最多 65" in reason:
            ceiling = min(ceiling, 65)
        elif "最多 70" in reason:
            ceiling = min(ceiling, 70)
    return min(score, ceiling)


def calculate_match_scores(
    jd_structured: JsonDict,
    resume_structured: JsonDict,
    resume_text: str,
    keyword_coverage: JsonDict,
    services: MatchingServices,
    *,
    fast: bool,
    requirement_evidence_map: list[JsonDict] | None = None,
) -> JsonDict:
    if requirement_evidence_map:
        return _calculate_from_evidence_map(jd_structured, resume_structured, keyword_coverage, requirement_evidence_map, services)
    evidence_items = resume_structured.get("evidence_items", [])
    hard_score, hard_missing, hard_weak, ceiling_reasons = score_hard_requirements(jd_structured, resume_structured, resume_text, services)
    core_score, core_matched, core_missing, core_weak = score_requirement_list(jd_structured.get("core_responsibilities", []), evidence_items, services, fast=fast)
    skill_score, skill_matched, skill_missing, skill_weak = score_skill_tools(jd_structured, resume_text, evidence_items, keyword_coverage, services, fast=fast)
    project_score = score_project_experience(jd_structured, resume_structured, services, fast=fast)
    industry_score = score_industry_background(jd_structured, resume_structured, resume_text, services)
    evidence_quality_score = score_evidence_quality(resume_structured)
    score_breakdown: ScoreBreakdown = {
        "hard_requirements_score": hard_score,
        "core_responsibility_score": core_score,
        "skill_tool_score": skill_score,
        "project_experience_score": project_score,
        "industry_background_score": industry_score,
        "evidence_quality_score": evidence_quality_score,
    }
    weighted_score = sum(score_breakdown[key] * weight for key, weight in SCORE_WEIGHTS.items())
    overall_score = apply_hard_ceilings(clamp_int(weighted_score), ceiling_reasons)
    must_have_missing = [keyword for keyword in keyword_coverage.get("must_have_keywords", []) if keyword in keyword_coverage.get("missing_keywords", [])]
    tool_keys = {services.normalize_text(tool) for tool in MATCH_TOOL_ALIASES}
    missing_must_have_tools = [keyword for keyword in must_have_missing if services.normalize_text(keyword) in tool_keys]
    if len(missing_must_have_tools) >= 2 and skill_score < 30:
        overall_score = min(overall_score, 42)
        ceiling_reasons.append("关键技能/工具缺失较多，overall_score 最多 42")
    if jd_structured.get("job_family") == "software_engineering" and skill_score < 20 and core_score < 45:
        overall_score = min(overall_score, 40)
        ceiling_reasons.append("研发岗位缺少工程实现证据，overall_score 最多 40")
    return {
        "overall_score": overall_score,
        "score_breakdown": score_breakdown,
        "hard_missing": hard_missing,
        "hard_weak": hard_weak,
        "core_matched": core_matched,
        "core_missing": core_missing,
        "core_weak": core_weak,
        "skill_matched": skill_matched,
        "skill_missing": skill_missing,
        "skill_weak": skill_weak,
        "ceiling_reasons": ceiling_reasons,
    }
