from __future__ import annotations

from careerpilot.matching.resume_revision import calculate_revision_feasibility, generate_targeted_resume_revision


def _record(
    requirement: str,
    status: str,
    *,
    category: str = "skill",
    importance: str = "HIGH",
    evidence: str = "",
    strength: float = 0.8,
) -> dict:
    return {
        "requirement": requirement,
        "category": category,
        "importance": importance,
        "evidence_status": status,
        "match_type": "EXACT_MATCH" if status in {"STRONG", "MEDIUM"} else "MISSING",
        "matched_evidence": [
            {
                "source_text": evidence,
                "evidence_type": "project_experience",
                "strength": strength,
            }
        ]
        if evidence
        else [],
        "missing_reason": f"No evidence for {requirement}.",
        "rewrite_potential": "HIGH" if status == "STRONG" else "MEDIUM" if status == "MEDIUM" else "NONE",
    }


def test_not_safe_target_does_not_generate_summary_or_bullets() -> None:
    revision = generate_targeted_resume_revision(
        {"job_title": "Data Analyst"},
        {"summary": ["Current summary"]},
        [
            _record("SQL", "MISSING"),
            _record("Python", "MISSING"),
            _record("Dashboarding", "MISSING"),
        ],
        match_result={"overall_score": 32, "recommendation_level": "NOT_RECOMMENDED"},
    )

    assert revision["revision_level"] == "NOT_SAFE_TO_TARGET"
    assert revision["target_revision_feasibility_score"] < 35
    assert revision["summary_revision"]["suggested_text"] == ""
    assert revision["advantage_cards"] == []
    assert revision["experience_bullet_rewrites"] == []
    assert [item["requirement"] for item in revision["missing_experience_warnings"]] == [
        "SQL",
        "Python",
        "Dashboarding",
    ]
    assert not any(item.get("suggested_bullet") for item in revision["experience_bullet_rewrites"])


def test_legacy_match_fields_do_not_raise_revision_feasibility() -> None:
    revision = generate_targeted_resume_revision(
        {"job_title": "Data Analyst"},
        {"summary": ["General campus experience"]},
        [
            _record("SQL", "MISSING"),
            _record("Python", "MISSING"),
            _record("A/B testing", "MISSING"),
        ],
        match_result={
            "overall_score": 88,
            "target_revision_feasibility_score": 100,
            "legacy_compat": {"matched_skills": ["SQL", "Python", "A/B testing"]},
        },
    )

    assert revision["revision_level"] == "NOT_SAFE_TO_TARGET"
    assert revision["target_revision_feasibility_score"] < 35
    assert revision["target_revision_feasibility_score"] != 100
    assert revision["summary_revision"]["suggested_text"] == ""


def test_calculate_revision_feasibility_ignores_match_result_overall_score() -> None:
    evidence_map = [
        _record("SQL", "MISSING"),
        _record("Python", "MISSING"),
        _record("Dashboarding", "MISSING"),
    ]

    without_match = calculate_revision_feasibility(evidence_map)
    with_match = calculate_revision_feasibility(
        evidence_map,
        match_result={"overall_score": 100, "target_revision_feasibility_score": 100},
    )

    assert with_match == without_match
    assert with_match["revision_level"] == "NOT_SAFE_TO_TARGET"


def test_limited_customization_uses_only_strong_evidence_for_bullets() -> None:
    revision = generate_targeted_resume_revision(
        {"job_title": "Product Analyst"},
        {"summary": ["Product and data background"]},
        [
            _record(
                "SQL analysis",
                "STRONG",
                evidence="Built SQL retention dashboard with weekly cohort review.",
                strength=0.91,
            ),
            _record(
                "User research",
                "MEDIUM",
                evidence="Assisted with interview notes.",
                strength=0.62,
            ),
            _record("Experiment design", "MISSING"),
        ],
        match_result={"overall_score": 54, "recommendation_level": "NOT_RECOMMENDED"},
    )

    assert revision["revision_level"] == "LIMITED_CUSTOMIZATION"
    suggested = [item["suggested_bullet"] for item in revision["experience_bullet_rewrites"]]
    assert len(suggested) == 1
    assert "SQL analysis" in suggested[0]
    assert "User research" not in repr(revision["experience_bullet_rewrites"])
    assert [item["requirement"] for item in revision["missing_experience_warnings"]] == ["Experiment design"]
    assert "Experiment design" not in repr(revision["experience_bullet_rewrites"])


def test_advantage_cards_require_bound_resume_evidence() -> None:
    revision = generate_targeted_resume_revision(
        {"job_title": "Operations Analyst"},
        {"summary": ["Operations background"]},
        [
            _record("Operations review", "STRONG"),
            _record(
                "Conversion analysis",
                "STRONG",
                evidence="Reviewed conversion funnel and improved signup completion by 12%.",
                strength=0.88,
            ),
        ],
        match_result={"overall_score": 82, "recommendation_level": "APPLY_WITH_REVISION"},
    )

    assert revision["revision_level"] == "HIGHLY_CUSTOMIZABLE"
    assert revision["advantage_cards"] == [
        {
            "target_requirement": "Conversion analysis",
            "resume_evidence": "Reviewed conversion funnel and improved signup completion by 12%.",
            "evidence_strength": 0.88,
            "suggested_expression": "Use this as a supported proof point for: Conversion analysis",
        }
    ]
    for card in revision["advantage_cards"]:
        assert card["target_requirement"]
        assert card["resume_evidence"]
        assert card["evidence_strength"] > 0


def test_no_advantage_cards_means_no_positioning_or_summary_claim() -> None:
    revision = generate_targeted_resume_revision(
        {"job_title": "Project Coordinator"},
        {"summary": ["General background"]},
        [
            _record("Stakeholder coordination", "STRONG"),
            _record("Project tracking", "STRONG"),
        ],
        match_result={"overall_score": 90, "recommendation_level": "STRONG_APPLY"},
    )

    assert revision["advantage_cards"] == []
    assert revision["target_positioning"] == ""
    assert revision["summary_revision"]["suggested_text"] == ""
    assert revision["experience_bullet_rewrites"] == []
