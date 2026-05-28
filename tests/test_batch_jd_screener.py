from __future__ import annotations

from careerpilot_api.batch_screener import _sort_and_limit_priority, screen_batch_jds_for_api
from careerpilot_api.schemas import MatchAssessment


def _payload() -> dict:
    return {
        "jobs": [
            {
                "job_id": "job-data",
                "title": "Data Analyst Intern",
                "company": "Example Company",
                "location": "Shanghai",
                "industry": "Internet",
                "jd_text": (
                    "Responsibilities: build SQL dashboards, analyze retention, "
                    "support product review, join core project with mentor. "
                    "Requirements: must have SQL and data analysis."
                ),
            },
            {
                "job_id": "job-risky-sales",
                "title": "Sales Operations Intern",
                "company": "Example Company",
                "location": "Beijing",
                "industry": "Sales",
                "jd_text": (
                    "Need sales KPI, cold calls, frequent travel and unpaid internship. "
                    "Other tasks assigned by leader."
                ),
            },
        ],
        "resume_evidence": [
            {
                "evidence_id": "evidence-sql-dashboard",
                "section": "项目经历",
                "original_text": "Built SQL retention dashboard and improved weekly review efficiency by 20%.",
                "skills": ["SQL", "data analysis", "dashboard"],
                "metrics": ["20%"],
                "confidence": 0.95,
            }
        ],
        "preferences": {
            "target_cities": ["Shanghai"],
            "preferred_industries": ["Internet"],
        },
    }


def test_batch_jd_screener_returns_match_assessments_ranked_by_hr_logic() -> None:
    assessments = screen_batch_jds_for_api(_payload())

    assert [item.job_id for item in assessments] == ["job-data", "job-risky-sales"]
    assert assessments[0].recommendation_level in {"priority_apply", "apply_after_rewrite"}
    assert assessments[0].next_action in {"立即投递", "修改简历后投递"}
    assert assessments[0].evidence_score > assessments[1].evidence_score
    assert assessments[0].preference_fit_score > assessments[1].preference_fit_score
    assert assessments[1].risk_score > assessments[0].risk_score
    assert assessments[1].recommendation_level == "not_recommended"
    assert assessments[1].next_action == "不建议投递"


def test_batch_jd_screener_risks_are_specific() -> None:
    risky = screen_batch_jds_for_api(_payload())[1]

    assert any("薪资" in risk or "待遇" in risk for risk in risky.risks)
    assert any("销售" in risk or "业绩" in risk for risk in risky.risks)
    assert all(risk != "存在风险" for risk in risky.risks)


def test_batch_jd_screener_does_not_mark_every_job_priority_apply() -> None:
    assessments = [
        MatchAssessment(
            job_id=f"job-{index}",
            overall_score=90 - index,
            hard_match_score=88,
            preference_fit_score=90,
            evidence_score=86,
            growth_value_score=82,
            risk_score=12,
            recommendation_level="priority_apply",
            recommendation_reason="示例高匹配岗位。",
            risks=[],
            missing_evidence=[],
            weak_evidence=[],
            next_action="立即投递",
        )
        for index in range(3)
    ]

    ranked = _sort_and_limit_priority(assessments)

    assert any(item.recommendation_level == "priority_apply" for item in ranked)
    assert any(item.recommendation_level != "priority_apply" for item in ranked)
    assert {item.next_action for item in ranked} >= {"立即投递", "修改简历后投递"}
