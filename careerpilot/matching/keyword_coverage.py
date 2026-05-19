from __future__ import annotations

from .jd_parser import MATCH_TOOL_ALIASES
from .schema import JsonDict, MatchingServices, unique_items


STRONG_REQUIREMENT_MARKERS = ["必须", "熟练", "掌握", "精通", "要求", "必备", "优先", "加分", "需要", "需具备"]


def keyword_aliases(keyword: str, services: MatchingServices) -> list[str]:
    clean = services.normalize_text(keyword)
    aliases = [clean]
    if clean in services.skill_aliases:
        aliases.extend(services.skill_aliases.get(clean, []))
    if clean in MATCH_TOOL_ALIASES:
        aliases.extend(MATCH_TOOL_ALIASES[clean])
    for skill, skill_aliases in services.skill_aliases.items():
        if clean == skill or clean in services.split_preference_items(skill_aliases):
            aliases.extend([skill] + skill_aliases)
            break
    for tool, tool_aliases in MATCH_TOOL_ALIASES.items():
        if clean == tool or clean in services.split_preference_items(tool_aliases):
            aliases.extend([tool] + tool_aliases)
            break
    return unique_items(services.split_preference_items(aliases), services, 16)


def expanded_keyword_aliases(keyword: str, services: MatchingServices) -> list[str]:
    clean = services.normalize_text(keyword)
    aliases = keyword_aliases(keyword, services)
    for group in services.semantic_groups_for_terms(clean, aliases):
        aliases.extend(services.semantic_capability_groups.get(group, []))
    return unique_items(services.split_preference_items(aliases), services, 24)


def requirement_best_evidence(
    requirement: str,
    evidence_items: list[JsonDict],
    services: MatchingServices,
    *,
    fast: bool,
) -> tuple[JsonDict | None, float, str]:
    if not evidence_items:
        return None, 0.0, "MISSING"
    req = services.normalize_text(requirement)
    aliases = keyword_aliases(req, services)
    best_item: JsonDict | None = None
    best_score = 0.0
    best_type = "MISSING"
    semantic_fn = services.semantic_similarity_fast if fast else services.semantic_similarity
    req_groups = set(services.capability_group_hits(req)) | set(services.semantic_groups_for_terms(req, aliases))
    for item in evidence_items:
        line = str(item.get("source_text", ""))
        direct = any(services.text_contains(line, alias) for alias in aliases if len(alias) >= 2)
        line_groups = set(services.capability_group_hits(line))
        group_overlap = bool(req_groups and line_groups and req_groups & line_groups)
        similarity = semantic_fn(req, line)
        strength = float(item.get("strength", 0))
        score = similarity * 0.52 + strength * 0.34 + (0.18 if direct else 0) + (0.12 if group_overlap else 0)
        score = max(0.0, min(1.0, score))
        if score > best_score:
            best_score = score
            best_item = item
            best_type = "EXACT_MATCH" if direct else "SEMANTIC_MATCH" if group_overlap or similarity >= 0.22 else "EVIDENCE_MATCH" if strength >= 0.62 else "MISSING"
    if best_score < 0.24:
        best_type = "MISSING"
    return best_item, best_score, best_type


def classify_keyword_coverage(
    keyword: str,
    resume_text: str,
    evidence_items: list[JsonDict],
    services: MatchingServices,
    *,
    fast: bool,
) -> tuple[str, str]:
    aliases = keyword_aliases(keyword, services)
    clean_resume = services.normalize_text(resume_text)
    direct = [alias for alias in aliases if services.text_contains(clean_resume, alias)]
    if direct:
        return "EXACT_MATCH", direct[0]
    if services.normalize_text(keyword) in MATCH_TOOL_ALIASES:
        return "MISSING", ""
    item, strength, match_type = requirement_best_evidence(keyword, evidence_items, services, fast=fast)
    if match_type == "SEMANTIC_MATCH" and strength >= 0.46:
        return "SEMANTIC_MATCH", str(item.get("source_text", "")) if item else ""
    if match_type == "EVIDENCE_MATCH" and strength >= 0.56:
        return "EVIDENCE_MATCH", str(item.get("source_text", "")) if item else ""
    return "MISSING", ""


def build_keyword_coverage(
    jd_structured: JsonDict,
    resume_text: str,
    evidence_items: list[JsonDict],
    services: MatchingServices,
    *,
    fast: bool,
) -> JsonDict:
    must_have: list[str] = []
    jd_text = str(jd_structured.get("raw_text", "")) + " " + " ".join(req.get("text", "") for req in jd_structured.get("hard_requirements", []))
    for keyword in jd_structured.get("skills", []) + jd_structured.get("tools", []):
        aliases = keyword_aliases(keyword, services)
        if any(any(marker in jd_text[max(0, jd_text.find(alias) - 24): jd_text.find(alias) + len(alias) + 24] for marker in STRONG_REQUIREMENT_MARKERS) for alias in aliases if alias and alias in jd_text):
            must_have.append(keyword)
    must_have = unique_items(must_have, services, 18)
    important = unique_items([item for item in jd_structured.get("skills", []) + jd_structured.get("tools", []) if item not in must_have], services, 24)
    nice = unique_items([item for item in jd_structured.get("soft_skills", []) + jd_structured.get("industry_background", []) if item not in must_have and item not in important], services, 18)
    ordered_keywords = unique_items(must_have + important + nice, services, 36)

    exact_matches: list[JsonDict] = []
    semantic_matches: list[JsonDict] = []
    evidence_matches: list[JsonDict] = []
    missing_keywords: list[str] = []
    for keyword in ordered_keywords:
        status, evidence = classify_keyword_coverage(keyword, resume_text, evidence_items, services, fast=fast)
        payload = {"keyword": keyword, "evidence": evidence}
        if status == "EXACT_MATCH":
            exact_matches.append(payload)
        elif status == "SEMANTIC_MATCH":
            semantic_matches.append(payload)
        elif status == "EVIDENCE_MATCH":
            evidence_matches.append(payload)
        else:
            missing_keywords.append(keyword)
    covered_count = len(exact_matches) + len(semantic_matches) + len(evidence_matches)
    return {
        "coverage_rate": round(covered_count / max(len(ordered_keywords), 1), 2),
        "must_have_keywords": must_have,
        "important_keywords": important,
        "nice_to_have_keywords": nice,
        "exact_matches": exact_matches,
        "semantic_matches": semantic_matches,
        "evidence_matches": evidence_matches,
        "missing_keywords": missing_keywords,
    }
