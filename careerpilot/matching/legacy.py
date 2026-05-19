from __future__ import annotations

from .schema import JsonDict, MatchingServices


def legacy_skill_coverage_match(
    jd_analysis: JsonDict,
    resume_text: str,
    services: MatchingServices,
    profile_text: str | None = None,
    preferences: JsonDict | None = None,
    *,
    fast: bool = False,
) -> JsonDict:
    """LEGACY ONLY: run previous skill-coverage scorer for compatibility fields only."""
    try:
        return services.legacy_matcher(jd_analysis, resume_text, profile_text, preferences, fast=fast)
    except Exception:
        return {}
