from __future__ import annotations

from careerpilot.matching.report_builder import add_legacy_compat_fields
from careerpilot.matching.schema import MatchingServices


def _services() -> MatchingServices:
    def normalize_text(value: object) -> str:
        return "".join(str(value or "").strip().lower().split())

    def split_items(value: object) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [item for item in str(value or "").replace("/", " ").split() if item]

    return MatchingServices(
        normalize_text=normalize_text,
        normalize_multiline_text=lambda value: str(value or ""),
        text_contains=lambda text, keyword: normalize_text(keyword) in normalize_text(text),
        score_keywords=lambda text, keywords: 0,
        split_preference_items=split_items,
        jd_skill_list=lambda jd: [],
        extract_job_title=lambda text: "",
        parse_resume_sections=lambda text: {},
        normalize_resume_lines=lambda text: [],
        sectioned_resume_evidence_lines=lambda text: [],
        first_matching_lines=lambda lines, keywords, limit: [],
        resume_skill_hits=lambda text: [],
        capability_group_hits=lambda text: {},
        semantic_groups_for_terms=lambda text, aliases=None: [],
        semantic_similarity_fast=lambda left, right: 0.0,
        semantic_similarity=lambda left, right: 0.0,
        capability_overlap_similarity=lambda left, right: 0.0,
        remove_duplicate_information=lambda text: (str(text or ""), 0),
        legacy_matcher=lambda *args, **kwargs: {},
        skill_aliases={},
        semantic_capability_groups={},
        resume_action_terms=[],
        resume_result_terms=[],
        english_evidence_terms=[],
        resume_project_sections=set(),
        resume_skill_list_sections=set(),
    )


def test_legacy_compat_fields_do_not_merge_legacy_skill_results() -> None:
    result = {
        "overall_score": 73,
        "final_rank_score": 71,
        "target_revision_feasibility_score": 42,
        "matched_evidence": [
            {
                "jd_requirement": "SQL",
                "match_type": "EXACT_MATCH",
                "resume_evidence": "Used SQL in a dashboard project.",
                "explanation": "Project evidence supports SQL.",
            }
        ],
        "missing_requirements": [
            {"requirement": "Python", "importance": "HIGH", "reason": "No Python project evidence."},
            {"requirement": "Tableau", "importance": "LOW", "reason": "Nice-to-have only."},
        ],
        "weak_requirements": [
            {"requirement": "Business analysis", "problem": "Related evidence exists, but expression is vague."}
        ],
        "keyword_coverage": {
            "coverage_rate": 0.5,
            "missing_keywords": ["Power BI"],
        },
        "score_breakdown": {
            "evidence_quality_score": 61,
            "hard_requirements_score": 52,
        },
        "score_breakdown_notes": ["note 1", "note 2", "new ceiling only"],
    }
    legacy = {
        "matched_skills": ["LegacyMatched"],
        "missing_skills": ["LegacyMissing"],
        "hard_skill_gaps": ["LegacyHardGap"],
        "evidence_skill_gaps": ["LegacyEvidenceGap"],
        "expression_skill_gaps": ["LegacyExpressionGap"],
        "gap_examples": ["Legacy gap example"],
        "score_ceiling_reasons": ["Legacy ceiling"],
        "anchor_groups": ["LegacyAnchor"],
        "semantic_score": 99,
        "core_gap_penalty": 88,
        "duplicate_removed": 7,
        "overall_score": 1,
        "final_rank_score": 2,
        "target_revision_feasibility_score": 100,
    }

    output = add_legacy_compat_fields(result, legacy, _services())

    assert output["score"] == output["overall_score"] == 73
    assert output["final_rank_score"] == 71
    assert output["target_revision_feasibility_score"] == 42
    assert output["matched_skills"] == ["SQL"]
    assert output["missing_skills"] == ["Python", "Power BI"]
    assert output["hard_skill_gaps"] == ["Python"]
    assert output["evidence_skill_gaps"] == ["Business analysis"]
    assert output["expression_skill_gaps"] == ["Business analysis"]
    assert output["gap_examples"] == ["No Python project evidence.", "Nice-to-have only.", "Related evidence exists, but expression is vague."]
    assert output["score_ceiling_reasons"] == ["new ceiling only"]
    assert output["anchor_groups"] == []
    assert output["semantic_score"] == 0
    assert output["core_gap_penalty"] == 0
    assert output["duplicate_removed"] == 0
    assert output["legacy_compat"] == legacy

    serialized = repr(
        {
            key: output[key]
            for key in [
                "matched_skills",
                "missing_skills",
                "hard_skill_gaps",
                "evidence_skill_gaps",
                "expression_skill_gaps",
                "gap_examples",
                "score_ceiling_reasons",
                "anchor_groups",
            ]
        }
    )
    assert "Legacy" not in serialized
