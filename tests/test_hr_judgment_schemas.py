from __future__ import annotations

from dataclasses import fields
from pathlib import Path

from careerpilot_api.schemas import (
    ApplicationStrategy,
    JobOpportunity,
    MatchAssessment,
    ResumeEvidence,
    RewriteSuggestion,
    to_jsonable,
)


EXPECTED_FIELDS = {
    JobOpportunity: [
        "job_id",
        "title",
        "company",
        "location",
        "industry",
        "jd_text",
        "source",
        "captured_at",
    ],
    ResumeEvidence: [
        "evidence_id",
        "section",
        "original_text",
        "skills",
        "metrics",
        "confidence",
    ],
    MatchAssessment: [
        "job_id",
        "overall_score",
        "hard_match_score",
        "preference_fit_score",
        "evidence_score",
        "growth_value_score",
        "risk_score",
        "recommendation_level",
        "recommendation_reason",
        "risks",
        "missing_evidence",
        "weak_evidence",
        "next_action",
    ],
    RewriteSuggestion: [
        "target_job_id",
        "original_text",
        "rewritten_text",
        "reason",
        "jd_keywords",
        "strengthened_requirement",
        "evidence_needed",
        "needs_user_evidence",
        "evidence_status",
        "allowed_to_apply",
        "user_input_required",
        "confidence",
        "recommended_jobs_after_rewrite",
    ],
    ApplicationStrategy: [
        "job_id",
        "priority",
        "application_status",
        "preparation_status",
        "timing",
        "required_preparation",
        "follow_up_action",
        "reason",
        "planned_apply_date",
        "follow_up_reminder",
        "interview_stage",
        "review_result",
    ],
}


def test_hr_judgment_dataclasses_use_shared_snake_case_fields() -> None:
    for schema, expected in EXPECTED_FIELDS.items():
        assert [field.name for field in fields(schema)] == expected


def test_hr_judgment_objects_are_jsonable_without_personal_data() -> None:
    opportunity = JobOpportunity(
        job_id="job-demo-001",
        title="数据分析实习生",
        company="示例公司",
        location="上海",
        industry="互联网",
        jd_text="负责指标看板、SQL 取数和业务复盘支持。",
        source="示例岗位池",
        captured_at="2026-05-27T00:00:00+08:00",
    )
    evidence = ResumeEvidence(
        evidence_id="evidence-demo-001",
        section="项目经历",
        original_text="参与课程项目，使用 SQL 整理订单样例数据并输出看板。",
        skills=["SQL", "数据看板"],
        metrics=["样例订单数据"],
        confidence=0.72,
    )
    assessment = MatchAssessment(
        job_id=opportunity.job_id,
        overall_score=76,
        hard_match_score=80,
        preference_fit_score=70,
        evidence_score=74,
        growth_value_score=78,
        risk_score=32,
        recommendation_level="apply_after_rewrite",
        recommendation_reason="岗位职责与示例项目存在交集，但还需要补充更明确的结果证据。",
        risks=["成果指标偏弱"],
        missing_evidence=["业务影响"],
        weak_evidence=[evidence.evidence_id],
        next_action="先补充项目背景、个人动作和可验证结果，再投递。",
    )
    rewrite = RewriteSuggestion(
        target_job_id=opportunity.job_id,
        original_text=evidence.original_text,
        rewritten_text="建议围绕 SQL 取数、看板输出和复盘支持补充真实细节后再改写。",
        reason="当前证据可支持方向，但不应夸大业务结果。",
        jd_keywords=["SQL", "指标看板", "业务复盘"],
        strengthened_requirement="SQL 取数和指标看板建设",
        evidence_needed=["真实项目背景", "个人负责范围", "可验证结果"],
        needs_user_evidence=True,
        evidence_status="needs_user_evidence",
        allowed_to_apply=False,
        user_input_required=["真实项目或经历", "个人负责范围", "可验证结果"],
        confidence=0.68,
        recommended_jobs_after_rewrite=[opportunity.job_id],
    )
    strategy = ApplicationStrategy(
        job_id=opportunity.job_id,
        priority="after_resume_update",
        application_status="not_applied",
        preparation_status="needs_rewrite",
        timing="简历补强后投递",
        required_preparation=["补充项目结果", "准备 SQL 项目追问"],
        follow_up_action="补强后重新运行匹配评估。",
        reason="岗位有相关性，但证据仍需增强。",
    )

    payload = to_jsonable([opportunity, evidence, assessment, rewrite, strategy])

    assert payload[0]["job_id"] == "job-demo-001"
    assert payload[2]["recommendation_level"] == "apply_after_rewrite"
    assert payload[3]["recommended_jobs_after_rewrite"] == ["job-demo-001"]
    assert "真实个人" not in str(payload)


def test_frontend_types_include_same_core_fields() -> None:
    types_source = Path("web/src/types.ts").read_text(encoding="utf-8")

    for expected in EXPECTED_FIELDS.values():
        for field_name in expected:
            assert f"{field_name}:" in types_source or f"{field_name}?:" in types_source
