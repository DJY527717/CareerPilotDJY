from pathlib import Path

from careerpilot_api.schemas import AUTHENTICITY_FALLBACK_MESSAGE, AUTHENTICITY_RULES, RewriteSuggestion


ROOT = Path(__file__).resolve().parents[1]


def read_source(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_authenticity_rules_block_resume_hallucination() -> None:
    joined = "\n".join(AUTHENTICITY_RULES)

    assert "不得编造公司、学校、项目、奖项、证书、实习、指标、成果" in joined
    assert "不得把“熟悉”改成“主导”" in joined
    assert "不得把课程作业包装成企业项目" in joined
    assert "不得虚构量化指标" in joined
    assert "不得把JD关键词直接塞进简历" in joined
    assert AUTHENTICITY_FALLBACK_MESSAGE == "需要补充真实项目或经历，不能直接写入简历。"


def test_rewrite_suggestion_has_apply_boundary_fields() -> None:
    suggestion = RewriteSuggestion(
        target_job_id="job-demo-001",
        original_text="简历中尚未出现该岗位要求的可验证经历。",
        rewritten_text=AUTHENTICITY_FALLBACK_MESSAGE,
        reason="证据不足时只能给出补充建议，不能生成可直接写入简历的表达。",
        jd_keywords=["SQL"],
        strengthened_requirement="SQL 取数",
        evidence_needed=["真实项目或经历"],
        needs_user_evidence=True,
        evidence_status="needs_user_evidence",
        allowed_to_apply=False,
        user_input_required=["真实项目或经历", "个人负责范围", "可验证结果"],
        confidence=0.36,
        recommended_jobs_after_rewrite=[],
    )

    assert suggestion.allowed_to_apply is False
    assert suggestion.evidence_status == "needs_user_evidence"
    assert suggestion.user_input_required


def test_frontend_rewrite_flow_hides_apply_when_evidence_is_insufficient() -> None:
    source = read_source("web/src/rewriteSuggestions.ts")
    panel = read_source("web/src/components/resume/ResumeSuggestionPanel.tsx")

    assert "allowed_to_apply" in source
    assert "evidence_status" in source
    assert "user_input_required" in source
    assert "需要补充真实项目或经历，不能直接写入简历" in source
    assert ".filter((suggestion) => suggestion.allowed_to_apply)" in panel
    assert "补充证据后再生成" in panel
    assert "source_excerpt={suggestion.allowed_to_apply ? suggestion.original_text : \"\"}" in panel


def test_demo_resume_does_not_provide_fake_metrics() -> None:
    source = read_source("web/src/pages/resume/ResumeMatchPage.tsx")

    assert "不提供虚构指标" in source
    assert "真实简历需由用户补充可证明结果" in source
