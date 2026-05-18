from __future__ import annotations

from .evidence_mapper import build_requirement_evidence_map
from .jd_parser import build_structured_jd
from .keyword_coverage import build_keyword_coverage
from .legacy import legacy_skill_coverage_match
from .preference_fit import apply_preference_ceilings, calculate_preference_fit, has_meaningful_preferences
from .resume_parser import build_structured_resume
from .schema import JsonDict, MatchingServices, json_safe, recommendation_level, unique_items
from .scoring import calculate_match_scores


def rewrite_suggestions(weak: list[JsonDict], missing: list[JsonDict]) -> list[JsonDict]:
    suggestions: list[JsonDict] = []
    for item in weak[:4]:
        suggestions.append(
            {
                "target_requirement": item.get("requirement", ""),
                "original_text": item.get("current_evidence", ""),
                "suggested_text": item.get("rewrite_suggestion", ""),
                "reason": item.get("problem", ""),
            }
        )
    for item in missing[:3]:
        suggestions.append(
            {
                "target_requirement": item.get("requirement", ""),
                "original_text": "",
                "suggested_text": "",
                "reason": item.get("reason", "") or "Missing requirements need real evidence first; no copyable resume bullet is generated.",
            }
        )
    return suggestions[:7]


def interview_focus(jd_structured: JsonDict, missing: list[JsonDict], weak: list[JsonDict], matched: list[JsonDict]) -> list[JsonDict]:
    focus: list[JsonDict] = []
    for item in (missing[:4] + weak[:3]):
        req = item.get("requirement", "")
        if not req:
            continue
        focus.append(
            {
                "topic": req,
                "why_likely_asked": "这是 JD 明确要求但当前简历证据不足的点，面试中容易被追问是否真的做过。",
                "prep_suggestion": f"准备 1 个真实案例，按背景、任务、动作、工具/方法、结果复盘来讲清「{req}」。",
            }
        )
    for item in matched[:2]:
        req = item.get("jd_requirement", "")
        focus.append(
            {
                "topic": req,
                "why_likely_asked": "这是你和岗位较匹配的证据点，面试官可能要求展开细节。",
                "prep_suggestion": "准备项目细节、关键决策、量化结果和如果重做会如何优化。",
            }
        )
    if not focus:
        for req in jd_structured.get("core_responsibilities", [])[:3]:
            focus.append(
                {
                    "topic": req,
                    "why_likely_asked": "该职责属于岗位核心任务。",
                    "prep_suggestion": "准备一段最接近的项目经历，并说明你的个人贡献和结果。",
                }
            )
    return focus[:7]


def match_confidence(jd_text: str, resume_text: str, evidence_items: list[JsonDict], matched_count: int, services: MatchingServices) -> float:
    value = 0.35
    value += min(len(services.normalize_text(jd_text)) / 1200, 1) * 0.18
    value += min(len(services.normalize_text(resume_text)) / 1600, 1) * 0.18
    value += min(len(evidence_items) / 12, 1) * 0.17
    value += min(matched_count / 8, 1) * 0.12
    return round(float(max(0.25, min(0.95, value))), 2)


def build_match_result(
    jd_analysis: JsonDict,
    jd_structured: JsonDict,
    resume_structured: JsonDict,
    resume_text: str,
    keyword_coverage: JsonDict,
    requirement_evidence_map: list[JsonDict],
    score_result: JsonDict,
    services: MatchingServices,
) -> JsonDict:
    overall_score = int(score_result["overall_score"])
    matched_evidence = unique_items(score_result.get("core_matched", []) + score_result.get("skill_matched", []), services, 12)
    missing_requirements = unique_items(
        [
            {
                "requirement": item.get("requirement", ""),
                "importance": "HIGH",
                "reason": item.get("reason", ""),
                "improvement_suggestion": "先确认是否真实满足；若满足，把证书/学历/语言/年限/到岗信息明确写进简历。",
            }
            for item in score_result.get("hard_missing", [])
        ]
        + score_result.get("core_missing", [])
        + score_result.get("skill_missing", []),
        services,
        12,
    )
    weak_requirements = unique_items(
        [
            {
                "requirement": item.get("requirement", ""),
                "current_evidence": item.get("requirement", ""),
                "problem": item.get("reason", "硬性条件表达不够清楚。"),
                "rewrite_suggestion": "把该硬性条件用明确字段写出，避免招聘方猜测。",
            }
            for item in score_result.get("hard_weak", []) if int(item.get("score", 0)) >= 55
        ]
        + score_result.get("core_weak", [])
        + score_result.get("skill_weak", []),
        services,
        12,
    )
    level = recommendation_level(overall_score)
    primary_gap = missing_requirements[0]["requirement"] if missing_requirements else weak_requirements[0]["requirement"] if weak_requirements else ""
    reason_map = {
        "STRONG_APPLY": "简历中能看到多项核心职责和技能证据，硬性条件风险较低，值得优先投递。",
        "APPLY_WITH_REVISION": "整体方向匹配，但投递前应先补强关键证据和 JD 关键词表达。",
        "LOW_PRIORITY": "有一定相关性，但核心职责、项目证据或硬性条件仍有明显缺口，适合作为低优先级尝试。",
        "NOT_RECOMMENDED": "当前简历证据不足以支撑该岗位核心要求，不建议优先投入投递成本。",
    }
    recommendation_reason = reason_map[level]
    if primary_gap:
        recommendation_reason += f" 主要短板：{primary_gap}。"
    breakdown = score_result["score_breakdown"]
    notes = [
        f"硬性条件 {breakdown['hard_requirements_score']}/100，核心职责 {breakdown['core_responsibility_score']}/100，技能工具 {breakdown['skill_tool_score']}/100。",
        f"项目相似度 {breakdown['project_experience_score']}/100，行业背景 {breakdown['industry_background_score']}/100，证据质量 {breakdown['evidence_quality_score']}/100。",
    ]
    ceiling_reasons = score_result.get("ceiling_reasons", [])
    if ceiling_reasons:
        notes.append("硬性条件惩罚：" + "；".join(ceiling_reasons[:3]))
    if keyword_coverage.get("missing_keywords"):
        notes.append("ATS 优化优先补词：" + " / ".join(keyword_coverage["missing_keywords"][:6]))
    evidence_items = resume_structured.get("evidence_items", [])
    return {
        "overall_score": overall_score,
        "score": overall_score,
        "recommendation_level": level,
        "recommendation_reason": recommendation_reason,
        "job_family": jd_structured.get("job_family", "general"),
        "score_breakdown": breakdown,
        "score_breakdown_notes": notes,
        "matched_evidence": matched_evidence,
        "missing_requirements": missing_requirements,
        "weak_requirements": weak_requirements,
        "keyword_coverage": keyword_coverage,
        "requirement_evidence_map": requirement_evidence_map,
        "resume_rewrite_suggestions": rewrite_suggestions(weak_requirements, missing_requirements),
        "interview_focus": interview_focus(jd_structured, missing_requirements, weak_requirements, matched_evidence),
        "confidence": match_confidence(jd_analysis.get("raw_text", ""), resume_text, evidence_items, len(matched_evidence), services),
        "parsed_jd": jd_structured,
        "parsed_resume": resume_structured,
    }


def add_legacy_compat_fields(result: JsonDict, legacy: JsonDict, services: MatchingServices) -> JsonDict:
    matched_evidence = result.get("matched_evidence", [])
    missing_requirements = result.get("missing_requirements", [])
    weak_requirements = result.get("weak_requirements", [])
    keyword_coverage = result.get("keyword_coverage", {})
    result["score"] = int(result.get("overall_score", 0))
    result["matched_skills"] = unique_items([item.get("jd_requirement", "") for item in matched_evidence], services, 16)
    result["direct_matched_skills"] = unique_items([item.get("jd_requirement", "") for item in matched_evidence if item.get("match_type") == "EXACT_MATCH"], services)
    result["related_matched_skills"] = unique_items([item.get("jd_requirement", "") for item in matched_evidence if item.get("match_type") != "EXACT_MATCH"], services)
    result["missing_skills"] = unique_items(
        [item.get("requirement", "") for item in missing_requirements if item.get("importance") in {"HIGH", "MEDIUM"}]
        + keyword_coverage.get("missing_keywords", []),
        services,
        16,
    )
    result["hard_skill_gaps"] = unique_items([item.get("requirement", "") for item in missing_requirements if item.get("importance") == "HIGH"], services)
    result["evidence_skill_gaps"] = unique_items([item.get("requirement", "") for item in weak_requirements], services)
    result["skill_list_only_gaps"] = []
    result["expression_skill_gaps"] = unique_items(
        [
            item.get("requirement", "")
            for item in weak_requirements
            if "expression" in str(item.get("problem", "")).lower()
            or "表达" in str(item.get("problem", ""))
        ],
        services,
    )
    result["strengths"] = unique_items([f"{item.get('jd_requirement')}：{item.get('explanation')}" for item in matched_evidence[:5]], services, 8)
    result["gap_examples"] = unique_items([item.get("reason", "") for item in missing_requirements[:4]] + [item.get("problem", "") for item in weak_requirements[:4]], services, 8)
    result["semantic_score"] = 0
    result["coverage_score"] = int(round(float(keyword_coverage.get("coverage_rate", 0)) * 100))
    result["evidence_score"] = result.get("score_breakdown", {}).get("evidence_quality_score", 0)
    result["hard_requirement_score"] = result.get("score_breakdown", {}).get("hard_requirements_score", 0)
    result["core_gap_penalty"] = 0
    result["score_ceiling_reasons"] = result.get("score_breakdown_notes", [])[2:]
    result["evidence"] = [item.get("resume_evidence", "") for item in matched_evidence[:8] if item.get("resume_evidence")]
    result["hard_requirement_notes"] = [item.get("reason", "") for item in result.get("weak_requirements", [])[:6]]
    result["anchor_groups"] = []
    result["duplicate_removed"] = 0
    result["legacy_compat"] = legacy
    return result


def calculate_career_target_fit(jd_structured: JsonDict, preferences: JsonDict | None, services: MatchingServices) -> int:
    preferences = preferences or {}
    target_items = unique_items(
        services.split_preference_items(preferences.get("target_roles", []))
        + services.split_preference_items(preferences.get("career_target", []))
        + services.split_preference_items(preferences.get("job_keywords", [])),
        services,
        20,
    )
    if not target_items:
        return 70
    jd_text = services.normalize_text(
        " ".join(
            str(item or "")
            for item in [
                jd_structured.get("raw_text", ""),
                jd_structured.get("job_title", ""),
                jd_structured.get("job_family", ""),
            ]
        )
    )
    hits = [item for item in target_items if services.text_contains(jd_text, str(item))]
    if hits:
        return min(96, 72 + len(hits) * 8)
    semantic_hits = [
        item
        for item in target_items
        if services.capability_overlap_similarity(jd_text, str(item)) >= 0.38
    ]
    if semantic_hits:
        return min(86, 64 + len(semantic_hits) * 6)
    return 35


def calculate_final_rank_score(
    overall_score: int,
    career_target_fit_score: int,
    preference_fit_score: int,
    *,
    meaningful_preferences: bool,
    preference_fit: JsonDict | None = None,
    preferences: JsonDict | None = None,
) -> int:
    if not meaningful_preferences:
        return int(overall_score)
    raw_score = round(
        int(overall_score) * 0.60
        + int(career_target_fit_score) * 0.20
        + int(preference_fit_score) * 0.20
    )
    return apply_preference_ceilings(
        int(raw_score),
        overall_score=int(overall_score),
        career_target_fit_score=int(career_target_fit_score),
        preference_fit=preference_fit or {"preference_fit_score": int(preference_fit_score)},
        preferences=preferences,
    )


def generate_match_report(
    jd_analysis: JsonDict,
    resume_text: str,
    services: MatchingServices,
    profile_text: str | None = None,
    preferences: JsonDict | None = None,
    optional_job_family: str | None = None,
    *,
    fast: bool = False,
) -> JsonDict:
    resume_only, resume_duplicate_count = services.remove_duplicate_information(resume_text)
    resume_for_scoring = services.normalize_text(resume_only)
    selected_family = optional_job_family or (preferences or {}).get("job_family") or (preferences or {}).get("optional_job_family")
    jd_structured = build_structured_jd(jd_analysis, selected_family, services)
    resume_structured = build_structured_resume(resume_for_scoring, services)
    evidence_items = resume_structured.get("evidence_items", [])
    keyword_coverage = build_keyword_coverage(jd_structured, resume_for_scoring, evidence_items, services, fast=fast)
    requirement_evidence_map = build_requirement_evidence_map(jd_structured, resume_structured, keyword_coverage)
    score_result = calculate_match_scores(
        jd_structured,
        resume_structured,
        resume_for_scoring,
        keyword_coverage,
        services,
        fast=fast,
        requirement_evidence_map=requirement_evidence_map,
    )
    result = build_match_result(jd_analysis, jd_structured, resume_structured, resume_for_scoring, keyword_coverage, requirement_evidence_map, score_result, services)
    # LEGACY ONLY: these fields are exported for historical compatibility and
    # must not feed current scoring, ranking, recommendations, or resume rewrites.
    legacy = legacy_skill_coverage_match(jd_analysis, resume_text, services, profile_text, preferences, fast=fast)
    result = add_legacy_compat_fields(result, legacy, services)
    result["duplicate_removed"] = int(result.get("duplicate_removed", 0)) + resume_duplicate_count
    overall_score = int(result.get("overall_score", 0) or 0)
    career_target_fit_score = calculate_career_target_fit(jd_structured, preferences, services)
    preference_fit = calculate_preference_fit(jd_structured, preferences)
    meaningful_preferences = has_meaningful_preferences(preferences)
    preference_fit_score = int(preference_fit.get("preference_fit_score", 70))
    final_rank_score = calculate_final_rank_score(
        overall_score,
        career_target_fit_score,
        preference_fit_score,
        meaningful_preferences=meaningful_preferences,
        preference_fit=preference_fit,
        preferences=preferences,
    )
    result.update(
        {
            "evidence_fit_score": overall_score,
            "career_target_fit_score": career_target_fit_score,
            "preference_fit_score": preference_fit_score,
            "industry_fit_score": int(preference_fit.get("industry_fit_score", 70)),
            "city_fit_score": int(preference_fit.get("city_fit_score", 70)),
            "salary_fit_score": int(preference_fit.get("salary_fit_score", 70)),
            "salary_fit_level": str(preference_fit.get("salary_fit_level", "UNKNOWN")),
            "salary_mismatch_warnings": preference_fit.get("salary_mismatch_warnings", []),
            "preference_warnings": preference_fit.get("preference_warnings", []),
            "final_rank_score": int(final_rank_score),
        }
    )
    return json_safe(result)
