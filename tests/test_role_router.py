from __future__ import annotations

from careerpilot_api.role_router import ROLE_ROUTES, SAFE_FALLBACK_ROUTE, get_role_routes, resolve_role_route


EXPECTED_TASK_ROUTES = {
    "batch_jd_screening": "batch_jd_screener",
    "batch_result_dashboard": "batch_result_analyst",
    "single_jd_analysis": "jd_hr_analyst",
    "resume_match": "resume_match_reviewer",
    "resume_rewrite": "resume_rewrite_advisor",
    "application_strategy": "application_strategy_advisor",
    "interview_review": "interview_review_advisor",
    "report_summary": "report_analyst",
}


def test_task_type_maps_to_expected_role_id() -> None:
    assert {task_type: route.role_id for task_type, route in ROLE_ROUTES.items()} == EXPECTED_TASK_ROUTES


def test_every_route_declares_data_and_state_boundaries() -> None:
    for route in ROLE_ROUTES.values():
        assert route.allowed_read_data
        assert route.forbidden_read_data
        assert route.allowed_outputs
        assert route.forbidden_state_changes
        assert any("不得编造简历经历" in item for item in route.forbidden_outputs)
        assert any("不得修改用户原始简历" in item for item in route.forbidden_state_changes)
        assert any("不得覆盖用户求职偏好" in item for item in route.forbidden_state_changes)
        assert any("不得自动改变投递状态" in item for item in route.forbidden_state_changes)


def test_cross_role_guardrails_are_explicit() -> None:
    assert any("不得直接改简历" in item for item in ROLE_ROUTES["batch_jd_screening"].forbidden_state_changes)
    assert any("不得直接改变投递状态" in item for item in ROLE_ROUTES["resume_rewrite"].forbidden_state_changes)
    assert any("不得输出大量主观HR长评" in item for item in ROLE_ROUTES["batch_result_dashboard"].forbidden_outputs)
    assert any("不得覆盖用户偏好" in item for item in ROLE_ROUTES["application_strategy"].forbidden_state_changes)


def test_unknown_task_uses_restricted_fallback_role() -> None:
    route = resolve_role_route("unknown_task")

    assert route == SAFE_FALLBACK_ROUTE
    assert route.role_id == "general_product_advisor"
    assert route.allowed_outputs == ("普通说明", "需要选择明确功能的提示", "可用task_type列表")
    assert any("不得给出最终投递结论" in item for item in route.forbidden_outputs)
    assert any("不得给出最终简历改写结论" in item for item in route.forbidden_outputs)


def test_role_routes_payload_is_json_serializable_shape() -> None:
    payload = get_role_routes()

    assert set(payload["routes"]) == set(EXPECTED_TASK_ROUTES)
    assert payload["routes"]["resume_match"]["role_id"] == "resume_match_reviewer"
    assert payload["fallback"]["role_id"] == "general_product_advisor"
