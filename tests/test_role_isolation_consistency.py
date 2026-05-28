from pathlib import Path

from careerpilot_api.product_roles import PRODUCT_ROLES, WEB_PRODUCT_PERSONA
from careerpilot_api.schemas import ApplicationStrategy, MatchAssessment, RewriteSuggestion


ROOT = Path(__file__).resolve().parents[1]


def read_source(relative_path: str) -> str:
    return (ROOT / relative_path).read_text(encoding="utf-8")


def test_role_config_is_not_single_global_prompt() -> None:
    assert hasattr(WEB_PRODUCT_PERSONA, "shared_principles")
    assert not hasattr(WEB_PRODUCT_PERSONA, "global_principles")
    assert len(PRODUCT_ROLES) == 5
    assert set(PRODUCT_ROLES) == {
        "batch_jd_screener",
        "batch_result_analyst",
        "resume_match_reviewer",
        "resume_rewrite_advisor",
        "application_strategy_advisor",
    }


def test_role_outputs_have_separate_contracts() -> None:
    match_fields = set(MatchAssessment.__dataclass_fields__)
    rewrite_fields = set(RewriteSuggestion.__dataclass_fields__)
    strategy_fields = set(ApplicationStrategy.__dataclass_fields__)

    assert "recommendation_level" in match_fields
    assert "rewritten_text" in rewrite_fields
    assert "application_status" in strategy_fields
    assert "preparation_status" in strategy_fields
    assert "rewritten_text" not in match_fields
    assert "application_status" not in match_fields
    assert "recommendation_level" not in strategy_fields


def test_new_web_code_does_not_use_streamlit_or_test_ids() -> None:
    source = "\n".join(
        read_source(path)
        for path in [
            "web/src/pages/jd/BatchJDResultsPage.tsx",
            "web/src/pages/decision/ApplicationStrategyPage.tsx",
            "web/src/components/resume/ResumeSuggestionPanel.tsx",
            "web/src/rewriteSuggestions.ts",
        ]
    )
    assert "streamlit" not in source.lower()
    assert "data-testid" not in source
    assert " st." not in source


def test_rewrite_flow_keeps_suggestions_separate_from_resume_mutation() -> None:
    source = read_source("web/src/rewriteSuggestions.ts")
    panel = read_source("web/src/components/resume/ResumeSuggestionPanel.tsx")

    assert "需要补充真实项目或经历，不能直接写入简历" in source
    assert "needs_user_evidence" in source
    assert "allowed_to_apply" in source
    assert "evidence_status" in source
    assert "setResumeText" not in panel
    assert "应用到简历草稿" in panel
    assert "补充证据后再生成" in panel
    assert "仅保存建议" in panel


def test_default_mock_data_uses_anonymous_placeholders() -> None:
    source = "\n".join(
        read_source(path)
        for path in [
            "web/src/mockData.ts",
            "web/src/batchMockData.ts",
            "web/src/pages/resume/ResumeMatchPage.tsx",
            "careerpilot_api/mock_service.py",
        ]
    )
    blocked_terms = ["示例用户", "示例候选人", "上海", "北京", "本科，管理类", "example@"]
    for term in blocked_terms:
        assert term not in source
