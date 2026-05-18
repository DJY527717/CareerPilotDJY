from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import careerpilot.matching.report_builder as report_builder
from careerpilot.matching.preference_fit import has_meaningful_preferences
from careerpilot.matching.report_builder import calculate_career_target_fit, calculate_final_rank_score
from careerpilot.matching.ranking import sort_batch_rank_rows
from careerpilot.matching.schema import MatchingServices


def _services() -> MatchingServices:
    def normalize_text(text: str) -> str:
        return re.sub(r"\s+", " ", str(text).lower()).strip()

    def split_preference_items(value):
        if not value:
            return []
        if isinstance(value, str):
            return [item.strip() for item in re.split(r"[,，/、\n]", value) if item.strip()]
        return [str(item).strip() for item in value if str(item).strip()]

    def text_contains(text: str, token: str) -> bool:
        return normalize_text(token) in normalize_text(text)

    return MatchingServices(
        normalize_text=normalize_text,
        normalize_multiline_text=lambda text: str(text),
        text_contains=text_contains,
        score_keywords=lambda text, keywords: 0,
        capability_overlap_similarity=lambda left, right: 0.0,
        split_preference_items=split_preference_items,
        jd_skill_list=lambda jd: [],
        extract_job_title=lambda text: "",
        parse_resume_sections=lambda text: {},
        normalize_resume_lines=lambda text: [],
        sectioned_resume_evidence_lines=lambda text: [],
        first_matching_lines=lambda lines, keywords, limit=3: [],
        resume_skill_hits=lambda text: [],
        capability_group_hits=lambda text: {},
        semantic_groups_for_terms=lambda text, terms=None: [],
        semantic_similarity_fast=lambda left, right: 0.0,
        semantic_similarity=lambda left, right: 0.0,
        remove_duplicate_information=lambda text: (text, 0),
        legacy_matcher=lambda *args, **kwargs: {},
        skill_aliases={},
        semantic_capability_groups={},
        resume_action_terms=[],
        resume_result_terms=[],
        english_evidence_terms=[],
        resume_project_sections=set(),
        resume_skill_list_sections=set(),
    )


def test_career_target_preferences_make_ranking_meaningful() -> None:
    assert has_meaningful_preferences({"target_roles": ["Data Analyst"]}) is True
    assert has_meaningful_preferences({"career_target": "Data Analyst"}) is True
    assert has_meaningful_preferences({"job_keywords": "SQL"}) is True
    assert has_meaningful_preferences({}) is False


def test_final_rank_score_uses_formula_only_when_preferences_exist() -> None:
    assert calculate_final_rank_score(
        80,
        60,
        90,
        meaningful_preferences=True,
        preference_fit={"preference_fit_score": 90},
        preferences={"target_roles": ["Data Analyst"]},
    ) == 78
    assert calculate_final_rank_score(
        80,
        10,
        10,
        meaningful_preferences=False,
        preference_fit={"preference_fit_score": 10},
        preferences=None,
    ) == 80


def test_career_target_mismatch_caps_final_rank_score_at_60() -> None:
    jd = {
        "raw_text": "Backend Java intern owning service APIs",
        "job_title": "Backend Intern",
        "job_family": "software_engineering",
    }

    career_score = calculate_career_target_fit(jd, {"target_roles": ["Data Analyst"]}, _services())

    assert career_score == 35
    assert calculate_final_rank_score(
        90,
        career_score,
        90,
        meaningful_preferences=True,
        preference_fit={"preference_fit_score": 90},
        preferences={"target_roles": ["Data Analyst"]},
    ) == 60


def test_strict_salary_and_city_preferences_cap_final_rank_score() -> None:
    salary_capped = calculate_final_rank_score(
        90,
        70,
        70,
        meaningful_preferences=True,
        preference_fit={"preference_fit_score": 70, "salary_fit_score": 40, "city_fit_score": 70},
        preferences={"target_salary": {"min": 25000, "strict": True}},
    )
    city_capped = calculate_final_rank_score(
        90,
        70,
        70,
        meaningful_preferences=True,
        preference_fit={"preference_fit_score": 70, "salary_fit_score": 70, "city_fit_score": 45},
        preferences={"target_cities": ["上海"], "strict_city_match": True},
    )

    assert salary_capped == 50
    assert city_capped == 55


def test_low_evidence_fit_caps_final_rank_score_at_70() -> None:
    assert calculate_final_rank_score(
        54,
        96,
        96,
        meaningful_preferences=True,
        preference_fit={"preference_fit_score": 96, "salary_fit_score": 70, "city_fit_score": 70},
        preferences={"target_roles": ["Data Analyst"]},
    ) == 70


def test_batch_sorting_uses_final_rank_score_not_overall_score() -> None:
    df = pd.DataFrame(
        [
            {"job": "high overall only", "overall_score": 95, "final_rank_score": 41, "高价值": "是"},
            {"job": "best target rank", "overall_score": 62, "final_rank_score": 88, "高价值": "否"},
            {"job": "middle target rank", "overall_score": 81, "final_rank_score": 70, "高价值": "否"},
        ]
    )

    sorted_df = sort_batch_rank_rows(df)

    assert list(sorted_df["job"]) == ["best target rank", "middle target rank", "high overall only"]


def test_batch_sorting_does_not_fallback_to_intention_score() -> None:
    df = pd.DataFrame(
        [
            {"job": "legacy low intention", "意向匹配度": 1, "岗位推荐分": 1},
            {"job": "legacy high intention", "意向匹配度": 99, "岗位推荐分": 99},
        ]
    )

    sorted_df = sort_batch_rank_rows(df)

    assert list(sorted_df["job"]) == ["legacy low intention", "legacy high intention"]


def test_app_batch_views_filter_and_sort_only_by_final_rank_score() -> None:
    app_source = Path("app.py").read_text(encoding="utf-8")

    assert 'rank_col = "final_rank_score" if "final_rank_score" in view_df.columns else "意向匹配度"' not in app_source
    assert 'if min_score and "意向匹配度" in view_df.columns' not in app_source
    assert "def batch_rank_score_column" in app_source


def test_salary_city_and_industry_preferences_do_not_change_overall_score(monkeypatch) -> None:
    def fixed_score_result(*args, **kwargs):
        return {
            "overall_score": 80,
            "score_breakdown": {
                "hard_requirements_score": 80,
                "core_responsibility_score": 80,
                "skill_tool_score": 80,
                "project_experience_score": 80,
                "industry_background_score": 80,
                "evidence_quality_score": 80,
            },
            "core_matched": [],
            "skill_matched": [],
            "hard_missing": [],
            "core_missing": [],
            "skill_missing": [],
            "hard_weak": [],
            "core_weak": [],
            "skill_weak": [],
            "ceiling_reasons": [],
        }

    jd_structured = {
        "raw_text": "Internet operations role in Beijing with monthly salary 8k-10k.",
        "job_title": "Operations Analyst",
        "job_family": "operations",
        "location": "Beijing",
        "industry_background": ["Internet"],
        "salary": {
            "period": "monthly",
            "min_monthly": 8000,
            "max_monthly": 10000,
            "salary_type": "monthly",
            "confidence": 1.0,
        },
    }
    monkeypatch.setattr(report_builder, "legacy_skill_coverage_match", lambda *args, **kwargs: {"overall_score": 1, "final_rank_score": 99})
    monkeypatch.setattr(report_builder, "build_structured_jd", lambda *args, **kwargs: jd_structured)
    monkeypatch.setattr(report_builder, "build_structured_resume", lambda *args, **kwargs: {"evidence_items": []})
    monkeypatch.setattr(report_builder, "build_keyword_coverage", lambda *args, **kwargs: {"coverage_rate": 0.0})
    monkeypatch.setattr(report_builder, "build_requirement_evidence_map", lambda *args, **kwargs: [])
    monkeypatch.setattr(report_builder, "calculate_match_scores", fixed_score_result)

    no_preferences = report_builder.generate_match_report({"raw_text": "JD"}, "resume", _services())
    strict_preferences = report_builder.generate_match_report(
        {"raw_text": "JD"},
        "resume",
        _services(),
        preferences={
            "preferred_industries": ["Finance"],
            "target_cities": ["Shanghai"],
            "strict_city_match": True,
            "target_salary": {"min": 25000, "period": "monthly", "strict": True},
        },
    )

    assert no_preferences["overall_score"] == strict_preferences["overall_score"] == 80
    assert no_preferences["evidence_fit_score"] == strict_preferences["evidence_fit_score"] == 80
    assert no_preferences["final_rank_score"] == 80
    assert strict_preferences["final_rank_score"] == 50
    assert strict_preferences["final_rank_score"] != strict_preferences["overall_score"]
    assert strict_preferences["legacy_compat"]["final_rank_score"] == 99
