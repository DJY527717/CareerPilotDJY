from __future__ import annotations

from careerpilot_api.product_roles import get_product_roles
from careerpilot_api.role_boundaries import (
    EVIDENCE_FALLBACK_MESSAGE,
    ROLE_BOUNDARIES,
    get_role_boundaries,
    get_role_boundary,
)


EXPECTED_CORE_ROLES = {
    "batch_jd_screener",
    "batch_result_analyst",
    "resume_match_reviewer",
    "resume_rewrite_advisor",
    "application_strategy_advisor",
}


def test_all_core_roles_have_input_output_boundaries() -> None:
    assert set(ROLE_BOUNDARIES) == EXPECTED_CORE_ROLES
    for role_id, boundary in ROLE_BOUNDARIES.items():
        assert boundary.role_id == role_id
        assert boundary.allowed_inputs
        assert boundary.blocked_inputs
        assert boundary.required_outputs
        assert boundary.forbidden_outputs
        assert boundary.evidence_required


def test_role_specific_input_boundaries_are_explicit() -> None:
    assert "简历改写草稿" in ROLE_BOUNDARIES["batch_jd_screener"].blocked_inputs
    assert "筛选后的结构化MatchAssessment结果" in ROLE_BOUNDARIES["batch_result_analyst"].allowed_inputs
    assert "不得重新判断岗位适不适合" in ROLE_BOUNDARIES["batch_result_analyst"].forbidden_outputs
    assert "投递状态" in ROLE_BOUNDARIES["resume_rewrite_advisor"].blocked_inputs
    assert "不得读取投递状态作为改写依据" in ROLE_BOUNDARIES["resume_rewrite_advisor"].forbidden_outputs
    assert "匹配结果" in ROLE_BOUNDARIES["application_strategy_advisor"].allowed_inputs
    assert "简历准备度" in ROLE_BOUNDARIES["application_strategy_advisor"].allowed_inputs
    assert "用户求职偏好" in ROLE_BOUNDARIES["application_strategy_advisor"].allowed_inputs
    assert "不得修改用户投递状态" in ROLE_BOUNDARIES["application_strategy_advisor"].forbidden_outputs


def test_every_role_requires_evidence_and_blocks_fabrication() -> None:
    for boundary in ROLE_BOUNDARIES.values():
        assert any("证据来源" in item for item in boundary.required_outputs)
        assert any("证据来源" in item for item in boundary.evidence_required)
        assert any(EVIDENCE_FALLBACK_MESSAGE in item for item in boundary.evidence_required)
        assert any(EVIDENCE_FALLBACK_MESSAGE in item for item in boundary.forbidden_outputs)
        assert any("不得编造简历经历" in item for item in boundary.forbidden_outputs)


def test_role_boundary_lookup_and_payload_shape() -> None:
    assert get_role_boundary("resume_match_reviewer") == ROLE_BOUNDARIES["resume_match_reviewer"]
    assert get_role_boundary("unknown_role") is None

    payload = get_role_boundaries()
    assert payload["evidence_fallback_message"] == EVIDENCE_FALLBACK_MESSAGE
    assert set(payload["boundaries"]) == EXPECTED_CORE_ROLES


def test_product_roles_payload_includes_role_boundaries() -> None:
    payload = get_product_roles()

    assert set(payload["role_boundaries"]["boundaries"]) == EXPECTED_CORE_ROLES
