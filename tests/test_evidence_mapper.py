from __future__ import annotations

from careerpilot.matching.evidence_mapper import (
    _best_evidence,
    _status_from_strength,
    build_requirement_evidence_map,
)


def test_best_evidence_keeps_high_strength_unrelated_text_missing() -> None:
    item, strength, match_type = _best_evidence(
        "Python",
        [
            {
                "source_text": "Led campus debate club operations and organized weekly events.",
                "normalized_capability": "event leadership",
                "scenario": "project_experience",
                "strength": 0.99,
            }
        ],
        "skill",
    )

    assert item is None
    assert strength == 0.0
    assert match_type == "MISSING"


def test_status_from_strength_never_upgrades_missing_match_type() -> None:
    assert _status_from_strength("MISSING", 0.99, "EVIDENCE_MATCH") == "MISSING"


def test_tool_requirement_without_direct_tool_evidence_cannot_reach_medium() -> None:
    evidence_map = build_requirement_evidence_map(
        {"tools": ["Python"]},
        {
            "evidence_items": [
                {
                    "source_text": "Built SQL retention dashboard and reviewed weekly cohorts.",
                    "normalized_capability": "data analysis",
                    "scenario": "project_experience",
                    "strength": 0.95,
                    "tools_used": ["SQL"],
                }
            ]
        },
        {
            "important_keywords": ["Python"],
            "evidence_matches": [{"keyword": "Python", "evidence": "Built SQL retention dashboard."}],
        },
    )

    python_record = evidence_map[0]
    assert python_record["requirement"] == "Python"
    assert python_record["category"] == "tool"
    assert python_record["evidence_status"] != "MEDIUM"
    assert python_record["match_type"] != "EVIDENCE_MATCH"


def test_high_quality_unrelated_resume_experience_does_not_match_data_tools() -> None:
    evidence_map = build_requirement_evidence_map(
        {"tools": ["SQL", "Python"], "skills": ["A/B testing"]},
        {
            "evidence_items": [
                {
                    "source_text": "Led a 20-person student association, planned weekly events, and coordinated sponsors.",
                    "normalized_capability": "event leadership and stakeholder coordination",
                    "scenario": "project_experience",
                    "strength": 0.99,
                    "tools_used": [],
                }
            ]
        },
        {"important_keywords": ["SQL", "Python", "A/B testing"]},
    )

    records = {item["requirement"]: item for item in evidence_map}
    for requirement in ["SQL", "Python", "A/B testing"]:
        assert records[requirement]["evidence_status"] == "MISSING"
        assert records[requirement]["match_type"] == "MISSING"
        assert records[requirement]["matched_evidence"] == []


def test_tool_requirement_with_direct_tool_evidence_can_be_medium() -> None:
    evidence_map = build_requirement_evidence_map(
        {"tools": ["Python"]},
        {
            "evidence_items": [
                {
                    "source_text": "Used Python to clean user data and generate weekly reports.",
                    "normalized_capability": "Python data cleaning",
                    "scenario": "project_experience",
                    "strength": 0.78,
                    "tools_used": ["Python"],
                }
            ]
        },
        {"important_keywords": ["Python"]},
    )

    python_record = evidence_map[0]
    assert python_record["match_type"] == "EXACT_MATCH"
    assert python_record["evidence_status"] in {"STRONG", "MEDIUM"}
    assert python_record["matched_evidence"][0]["source_text"].startswith("Used Python")
