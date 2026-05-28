from __future__ import annotations

from careerpilot_api.persona_contract import CAREERPILOT_WEB_PERSONA_CONTRACT, get_persona_contract
from careerpilot_api.product_roles import get_product_roles


def test_persona_contract_defines_web_product_persona() -> None:
    contract = CAREERPILOT_WEB_PERSONA_CONTRACT

    assert contract.product_persona == "冷静、专业、有证据、懂招聘、会排序的求职参谋。"
    assert "只约束 CareerPilot Web 端的语气" in contract.scope
    assert any("先给结论" in item for item in contract.expression_principles)
    assert any("不替用户编造经历" in item for item in contract.expression_principles)


def test_persona_contract_defines_forbidden_expressions_and_template() -> None:
    contract = CAREERPILOT_WEB_PERSONA_CONTRACT

    assert any("过度鸡汤" in item for item in contract.forbidden_expressions)
    assert any("只给分数但不给证据来源" in item for item in contract.forbidden_expressions)
    assert contract.result_template.conclusion.startswith("结论：")
    assert contract.result_template.evidence.startswith("依据：")
    assert contract.result_template.risk.startswith("风险：")
    assert contract.result_template.next_step.startswith("下一步：")


def test_persona_contract_does_not_replace_role_id_management() -> None:
    contract = CAREERPILOT_WEB_PERSONA_CONTRACT

    assert any("不是全局 AI prompt" in item for item in contract.role_boundary)
    assert any("role_id 单独管理" in item for item in contract.role_boundary)


def test_persona_contract_is_exposed_with_product_roles_payload() -> None:
    payload = get_product_roles()
    contract_payload = get_persona_contract()

    assert payload["persona_contract"] == contract_payload
    assert set(payload["roles"]) == {
        "batch_jd_screener",
        "batch_result_analyst",
        "resume_match_reviewer",
        "resume_rewrite_advisor",
        "application_strategy_advisor",
    }
