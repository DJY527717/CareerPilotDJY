from __future__ import annotations

from typing import Any


JsonDict = dict[str, Any]


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm(value: Any) -> str:
    return "".join(_clean(value).lower().split())


def _unique_strings(items: list[Any], limit: int | None = None) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _clean(item)
        key = _norm(text)
        if not text or key in seen:
            continue
        seen.add(key)
        output.append(text)
        if limit and len(output) >= limit:
            break
    return output


def _coverage_lookup(keyword_coverage: JsonDict | None) -> dict[str, tuple[str, str]]:
    coverage = keyword_coverage or {}
    lookup: dict[str, tuple[str, str]] = {}
    for status, bucket in [
        ("EXACT_MATCH", "exact_matches"),
        ("SEMANTIC_MATCH", "semantic_matches"),
        ("EVIDENCE_MATCH", "evidence_matches"),
    ]:
        for item in coverage.get(bucket, []) or []:
            keyword = _clean(item.get("keyword"))
            if keyword:
                lookup[_norm(keyword)] = (status, _clean(item.get("evidence")))
    for keyword in coverage.get("missing_keywords", []) or []:
        lookup.setdefault(_norm(keyword), ("MISSING", ""))
    return lookup


def _evidence_type(item: JsonDict) -> str:
    scenario = _clean(item.get("scenario"))
    lower = scenario.lower()
    if any(token in scenario for token in ["教育", "学历", "学校"]) or "education" in lower:
        return "education"
    if any(token in scenario for token in ["项目", "科研", "论文"]) or "project" in lower:
        return "project_experience"
    if any(token in scenario for token in ["技能", "工具"]) or "skill" in lower:
        return "skill"
    if any(token in scenario for token in ["证书", "资格"]) or "cert" in lower:
        return "certification"
    return "work_experience"


def _candidate_evidence_items(parsed_resume: JsonDict) -> list[JsonDict]:
    items = list(parsed_resume.get("evidence_items", []) or [])
    for key, evidence_type in [
        ("education", "education"),
        ("work_experience", "work_experience"),
        ("project_experience", "project_experience"),
        ("skills", "skill"),
        ("certifications", "certification"),
    ]:
        for text in parsed_resume.get(key, []) or []:
            items.append(
                {
                    "source_text": _clean(text),
                    "normalized_capability": _clean(text),
                    "scenario": evidence_type,
                    "strength": 0.45 if evidence_type == "skill" else 0.55,
                }
            )
    return [item for item in items if _clean(item.get("source_text"))]


def _best_evidence(requirement: str, evidence_items: list[JsonDict], category: str = "") -> tuple[JsonDict | None, float, str]:
    req_norm = _norm(requirement)
    req_tokens = {token for token in _clean(requirement).lower().replace("/", " ").replace(",", " ").split() if len(token) >= 2}
    best_item: JsonDict | None = None
    best_score = 0.0
    best_type = "MISSING"
    for item in evidence_items:
        source = _clean(item.get("source_text"))
        capability = _clean(item.get("normalized_capability"))
        haystack = _norm(source + " " + capability)
        strength = float(item.get("strength", 0) or 0)
        exact = bool(req_norm and req_norm in haystack)
        token_overlap = bool(req_tokens and req_tokens & set(source.lower().split()))
        capability_overlap = bool(capability and (_norm(capability) in req_norm or req_norm in _norm(capability)))
        tools_used = [_norm(tool) for tool in item.get("tools_used", []) or []]
        tool_overlap = bool(category == "tool" and req_norm and (req_norm in tools_used or req_norm in haystack))
        related = exact or capability_overlap or token_overlap or tool_overlap
        if not related:
            continue
        score = min(1.0, strength * 0.44 + (0.38 if exact else 0) + (0.18 if capability_overlap else 0) + (0.14 if token_overlap else 0) + (0.26 if tool_overlap else 0))
        if score > best_score:
            best_item = item
            best_score = score
            if exact:
                best_type = "EXACT_MATCH"
            elif tool_overlap:
                best_type = "EVIDENCE_MATCH"
            elif capability_overlap or token_overlap:
                best_type = "SEMANTIC_MATCH"
            else:
                best_type = "MISSING"
    if best_score < 0.26:
        return None, 0.0, "MISSING"
    return best_item, best_score, best_type


def _status_from_strength(match_type: str, strength: float, keyword_status: str | None = None) -> str:
    status = keyword_status or match_type
    if match_type == "MISSING":
        return "MISSING"
    if status == "EXACT_MATCH" and strength >= 0.58:
        return "STRONG"
    if status in {"EXACT_MATCH", "SEMANTIC_MATCH", "EVIDENCE_MATCH"} and strength >= 0.48:
        return "MEDIUM"
    if status != "MISSING" and strength >= 0.26:
        return "WEAK"
    return "MISSING"


def _rewrite_potential(evidence_status: str, category: str) -> str:
    if evidence_status == "STRONG":
        return "HIGH"
    if evidence_status == "MEDIUM":
        return "MEDIUM"
    if evidence_status == "WEAK":
        return "LOW"
    return "NONE" if category in {"hard_requirement", "tool"} else "LOW"


def _risk_level(evidence_status: str, importance: str) -> str:
    if evidence_status == "MISSING" and importance == "HIGH":
        return "HIGH"
    if evidence_status in {"MISSING", "WEAK"}:
        return "MEDIUM"
    return "LOW"


def _requirement_records(parsed_jd: JsonDict, keyword_coverage: JsonDict | None) -> list[tuple[str, str, str, str]]:
    coverage = keyword_coverage or {}
    high_keywords = set(coverage.get("must_have_keywords", []) or [])
    medium_keywords = set(coverage.get("important_keywords", []) or [])
    records: list[tuple[str, str, str, str]] = []
    for req in parsed_jd.get("hard_requirements", []) or []:
        text = _clean(req.get("text") if isinstance(req, dict) else req)
        if text:
            records.append((text, "hard_requirement", "HIGH", "hard_requirement"))
    for text in parsed_jd.get("core_responsibilities", []) or []:
        records.append((_clean(text), "core_responsibility", "HIGH", "core_responsibility"))
    for category, key in [
        ("skill", "skills"),
        ("tool", "tools"),
        ("project_experience", "project_experience"),
        ("industry_background", "industry_background"),
        ("soft_skill", "soft_skills"),
        ("bonus", "bonus_requirements"),
    ]:
        for text in parsed_jd.get(key, []) or []:
            clean = _clean(text)
            importance = "HIGH" if clean in high_keywords else "MEDIUM" if clean in medium_keywords or category in {"skill", "tool"} else "LOW"
            records.append((clean, category, importance, key))
    return [(text, category, importance, source) for text, category, importance, source in records if text]


def build_requirement_evidence_map(
    parsed_jd: dict,
    parsed_resume: dict,
    keyword_coverage: dict | None = None,
) -> list[dict]:
    evidence_items = _candidate_evidence_items(parsed_resume)
    coverage_lookup = _coverage_lookup(keyword_coverage)
    output: list[JsonDict] = []
    seen: set[str] = set()
    for index, (requirement, category, importance, _source) in enumerate(_requirement_records(parsed_jd, keyword_coverage), start=1):
        key = f"{category}:{_norm(requirement)}"
        if key in seen:
            continue
        seen.add(key)
        keyword_status, keyword_evidence = coverage_lookup.get(_norm(requirement), ("", ""))
        best_item, strength, match_type = _best_evidence(requirement, evidence_items, category)
        if keyword_status and keyword_status != "MISSING":
            if category != "tool" or keyword_status == "EXACT_MATCH":
                match_type = keyword_status
            if keyword_evidence and (not best_item or keyword_evidence not in _clean(best_item.get("source_text"))):
                best_item = {
                    "source_text": keyword_evidence,
                    "normalized_capability": requirement,
                    "scenario": "skill",
                    "strength": max(strength, 0.5),
                }
                strength = max(strength, 0.5)
        evidence_status = _status_from_strength(match_type, strength, keyword_status or None)
        if category == "tool":
            has_direct_tool_evidence = bool(best_item and match_type in {"EXACT_MATCH", "EVIDENCE_MATCH"})
            if not has_direct_tool_evidence:
                evidence_status = "WEAK" if best_item and strength >= 0.35 else "MISSING"
            elif evidence_status == "MEDIUM" and match_type != "EXACT_MATCH":
                evidence_status = "WEAK"
        matched_evidence: list[JsonDict] = []
        if best_item and evidence_status != "MISSING":
            matched_evidence.append(
                {
                    "source_text": _clean(best_item.get("source_text")),
                    "normalized_capability": _clean(best_item.get("normalized_capability")) or requirement,
                    "evidence_type": _evidence_type(best_item),
                    "strength": round(float(max(0.0, min(1.0, strength))), 2),
                    "explanation": "Resume evidence supports this JD requirement." if evidence_status in {"STRONG", "MEDIUM"} else "Related evidence exists, but it is not yet specific enough.",
                }
            )
        missing_reason = "" if matched_evidence else "No reliable resume evidence was found for this JD requirement."
        output.append(
            {
                "requirement_id": f"REQ-{index:03d}",
                "requirement": requirement,
                "category": category,
                "importance": importance,
                "evidence_status": evidence_status,
                "match_type": match_type if evidence_status != "MISSING" else "MISSING",
                "matched_evidence": matched_evidence,
                "missing_reason": missing_reason,
                "rewrite_potential": _rewrite_potential(evidence_status, category),
                "risk_level": _risk_level(evidence_status, importance),
            }
        )
    return output
