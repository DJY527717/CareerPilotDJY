from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


JsonDict = dict[str, Any]
ScoreBreakdown = dict[str, int]

SCORE_WEIGHTS: dict[str, float] = {
    "hard_requirements_score": 0.20,
    "core_responsibility_score": 0.25,
    "skill_tool_score": 0.20,
    "project_experience_score": 0.20,
    "industry_background_score": 0.10,
    "evidence_quality_score": 0.05,
}

EMPTY_SCORE_BREAKDOWN: ScoreBreakdown = {
    "hard_requirements_score": 0,
    "core_responsibility_score": 0,
    "skill_tool_score": 0,
    "project_experience_score": 0,
    "industry_background_score": 0,
    "evidence_quality_score": 0,
}


@dataclass(frozen=True)
class MatchingServices:
    """App-provided helpers so matching modules stay free of Streamlit imports."""

    normalize_text: Callable[[Any], str]
    normalize_multiline_text: Callable[[Any], str]
    text_contains: Callable[[str, str], bool]
    score_keywords: Callable[[str, list[str]], int]
    split_preference_items: Callable[[list[Any]], list[str]]
    jd_skill_list: Callable[[JsonDict | None], list[str]]
    extract_job_title: Callable[[str], str]
    parse_resume_sections: Callable[[str], dict[str, list[str]]]
    normalize_resume_lines: Callable[[str], list[str]]
    sectioned_resume_evidence_lines: Callable[[str], list[tuple[str, str]]]
    first_matching_lines: Callable[[list[str], list[str], int], list[str]]
    resume_skill_hits: Callable[[str], list[str]]
    capability_group_hits: Callable[[str], dict[str, tuple[str, ...]]]
    semantic_groups_for_terms: Callable[[str, list[str] | None], list[str]]
    semantic_similarity_fast: Callable[[str, str], float]
    semantic_similarity: Callable[[str, str], float]
    capability_overlap_similarity: Callable[[str, str], float]
    remove_duplicate_information: Callable[[str | None], tuple[str, int]]
    legacy_matcher: Callable[..., JsonDict]
    skill_aliases: dict[str, list[str]]
    semantic_capability_groups: dict[str, list[str]]
    resume_action_terms: list[str]
    resume_result_terms: list[str]
    english_evidence_terms: list[str]
    resume_project_sections: set[str]
    resume_skill_list_sections: set[str]


def clamp_int(value: float, minimum: int = 0, maximum: int = 100) -> int:
    return int(max(minimum, min(maximum, round(value))))


def clamp_float(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return float(max(minimum, min(maximum, value)))


def json_safe(value: Any) -> Any:
    """Convert nested values into JSON-serializable Python primitives."""
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def unique_items(items: list[Any], services: MatchingServices, limit: int | None = None) -> list[Any]:
    output: list[Any] = []
    seen: set[str] = set()
    for item in items:
        if item is None:
            continue
        key = services.normalize_text(str(item))
        if not key or key in seen:
            continue
        seen.add(key)
        output.append(item)
        if limit and len(output) >= limit:
            break
    return output


def recommendation_level(score: int) -> str:
    if score >= 85:
        return "STRONG_APPLY"
    if score >= 70:
        return "APPLY_WITH_REVISION"
    if score >= 55:
        return "LOW_PRIORITY"
    return "NOT_RECOMMENDED"
