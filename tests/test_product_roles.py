from __future__ import annotations

from careerpilot_api.product_roles import PRODUCT_ROLES, WEB_PRODUCT_PERSONA, get_product_roles


EXPECTED_ROLE_IDS = {
    "batch_jd_screener",
    "batch_result_analyst",
    "resume_match_reviewer",
    "resume_rewrite_advisor",
    "application_strategy_advisor",
}


def test_web_product_persona_is_career_advisor_not_single_global_role() -> None:
    assert WEB_PRODUCT_PERSONA.description == "冷静、专业、有证据、懂招聘、会排序的求职参谋。"
    assert any("不同功能使用对应 role_id" in item for item in WEB_PRODUCT_PERSONA.shared_principles)
    assert any("不得直接修改用户简历、求职偏好、投递状态" in item for item in WEB_PRODUCT_PERSONA.shared_forbidden_behaviors)


def test_required_role_ids_are_defined_with_function_specific_roles() -> None:
    assert set(PRODUCT_ROLES) == EXPECTED_ROLE_IDS

    assert PRODUCT_ROLES["batch_jd_screener"].role_name == "资深HR + 猎头"
    assert PRODUCT_ROLES["batch_result_analyst"].role_name == "数据分析师"
    assert PRODUCT_ROLES["resume_match_reviewer"].role_name == "HR筛选官"
    assert PRODUCT_ROLES["resume_rewrite_advisor"].role_name == "简历顾问 + ATS优化顾问"
    assert PRODUCT_ROLES["application_strategy_advisor"].role_name == "求职策略顾问"


def test_every_role_has_complete_guardrails_and_evidence_sources() -> None:
    for role_id, role in PRODUCT_ROLES.items():
        assert role.role_id == role_id
        assert role.applies_to
        assert role.input_data
        assert role.output_goal
        assert role.judgment_principles
        assert role.forbidden_behaviors
        assert role.required_evidence_sources
        assert any("不得" in item for item in role.forbidden_behaviors)
        assert any("不得直接修改用户简历、求职偏好或投递状态" in item for item in role.forbidden_behaviors)
        assert any("没有证据来源" in item for item in role.forbidden_behaviors)
        assert any("对应" in item for item in role.required_evidence_sources)


def test_product_roles_payload_is_json_serializable_shape() -> None:
    payload = get_product_roles()

    assert payload["persona"]["id"] == "careerpilot_web_persona"
    assert set(payload["roles"]) == EXPECTED_ROLE_IDS
    assert payload["roles"]["resume_match_reviewer"]["role_name"] == "HR筛选官"
