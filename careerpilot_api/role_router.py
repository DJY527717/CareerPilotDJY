"""Role routing table for CareerPilot Web tasks.

The router only selects the professional role and its data boundary. It does
not execute prompts, mutate user state, or replace role-specific product
configs in product_roles.py.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


TaskType = Literal[
    "batch_jd_screening",
    "batch_result_dashboard",
    "single_jd_analysis",
    "resume_match",
    "resume_rewrite",
    "application_strategy",
    "interview_review",
    "report_summary",
]

RouteRoleId = Literal[
    "batch_jd_screener",
    "batch_result_analyst",
    "jd_hr_analyst",
    "resume_match_reviewer",
    "resume_rewrite_advisor",
    "application_strategy_advisor",
    "interview_review_advisor",
    "report_analyst",
    "general_product_advisor",
]


COMMON_FORBIDDEN_STATE_CHANGES = (
    "不得修改用户原始简历",
    "不得覆盖用户求职偏好",
    "不得自动改变投递状态",
    "不得写入面试进度或复盘结论",
)

COMMON_FORBIDDEN_OUTPUTS = (
    "不得编造简历经历、项目成果、指标、证书或岗位要求",
    "不得输出没有证据来源的结论",
)


@dataclass(frozen=True)
class RoleRoute:
    task_type: str
    role_id: RouteRoleId
    allowed_read_data: tuple[str, ...]
    forbidden_read_data: tuple[str, ...]
    allowed_outputs: tuple[str, ...]
    forbidden_state_changes: tuple[str, ...]
    forbidden_outputs: tuple[str, ...] = COMMON_FORBIDDEN_OUTPUTS


ROLE_ROUTES: dict[TaskType, RoleRoute] = {
    "batch_jd_screening": RoleRoute(
        task_type="batch_jd_screening",
        role_id="batch_jd_screener",
        allowed_read_data=("JD列表", "当前简历证据", "用户求职偏好", "历史匹配评分"),
        forbidden_read_data=("用户联系方式", "未授权的完整简历原文", "面试私密记录"),
        allowed_outputs=("MatchAssessment列表", "岗位推荐等级", "岗位风险", "下一步动作"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES + ("不得直接改简历",),
    ),
    "batch_result_dashboard": RoleRoute(
        task_type="batch_result_dashboard",
        role_id="batch_result_analyst",
        allowed_read_data=("批量筛选结果", "岗位分组统计", "缺失证据统计", "偏好冲突统计"),
        forbidden_read_data=("完整简历隐私内容", "用户联系方式", "未参与本轮分析的投递记录"),
        allowed_outputs=("看板结论", "分组数量", "Top岗位摘要", "风险岗位摘要", "能力短板汇总"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS + ("不得输出大量主观HR长评",),
    ),
    "single_jd_analysis": RoleRoute(
        task_type="single_jd_analysis",
        role_id="jd_hr_analyst",
        allowed_read_data=("单条JD原文", "结构化JD字段", "用户偏好摘要"),
        forbidden_read_data=("完整简历原文", "投递状态", "面试记录"),
        allowed_outputs=("JD职责拆解", "硬性要求", "关键词", "岗位风险", "信息缺口"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
    ),
    "resume_match": RoleRoute(
        task_type="resume_match",
        role_id="resume_match_reviewer",
        allowed_read_data=("目标JD", "简历片段", "ResumeEvidence", "用户偏好摘要"),
        forbidden_read_data=("无关岗位池", "未授权投递记录", "用户联系方式"),
        allowed_outputs=("匹配评分", "证据映射", "风险", "缺失证据", "面试追问建议"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
    ),
    "resume_rewrite": RoleRoute(
        task_type="resume_rewrite",
        role_id="resume_rewrite_advisor",
        allowed_read_data=("目标JD", "简历原文片段", "ResumeEvidence", "匹配缺口", "相似JD要求"),
        forbidden_read_data=("投递状态写入权限", "无关面试记录", "用户联系方式"),
        allowed_outputs=("RewriteSuggestion列表", "证据补充提示", "改写后可优先投递JD建议"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES + ("不得直接改变投递状态",),
    ),
    "application_strategy": RoleRoute(
        task_type="application_strategy",
        role_id="application_strategy_advisor",
        allowed_read_data=("MatchAssessment", "ApplicationStrategy", "用户偏好摘要", "用户确认的投递状态"),
        forbidden_read_data=("未授权完整简历", "用户联系方式", "与当前岗位无关的私密面试记录"),
        allowed_outputs=("投递优先级", "准备状态", "下一步动作", "跟进建议", "策略原因"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES + ("不得覆盖用户偏好",),
    ),
    "interview_review": RoleRoute(
        task_type="interview_review",
        role_id="interview_review_advisor",
        allowed_read_data=("用户输入的面试记录", "目标JD", "用户复盘备注"),
        forbidden_read_data=("无关简历全文", "未授权投递列表", "用户联系方式"),
        allowed_outputs=("面试复盘建议", "追问风险", "补充准备项", "后续跟进建议"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
    ),
    "report_summary": RoleRoute(
        task_type="report_summary",
        role_id="report_analyst",
        allowed_read_data=("结构化分析结果", "用户选择的报告范围", "已确认的岗位或面试记录"),
        forbidden_read_data=("用户联系方式", "未确认的完整简历私密内容", "无关投递记录"),
        allowed_outputs=("报告摘要", "关键指标", "风险摘要", "行动项摘要"),
        forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
    ),
}


SAFE_FALLBACK_ROUTE = RoleRoute(
    task_type="unknown",
    role_id="general_product_advisor",
    allowed_read_data=("当前任务名称", "公开的产品说明", "用户本轮显式提供的问题"),
    forbidden_read_data=("完整简历", "用户偏好", "投递状态", "面试记录", "岗位池详情"),
    allowed_outputs=("普通说明", "需要选择明确功能的提示", "可用task_type列表"),
    forbidden_state_changes=COMMON_FORBIDDEN_STATE_CHANGES,
    forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
    + (
        "不得给出最终投递结论",
        "不得给出最终简历改写结论",
        "不得给出岗位排序或录用概率判断",
    ),
)


def resolve_role_route(task_type: str) -> RoleRoute:
    """Return an explicit route or a restricted fallback route."""
    return ROLE_ROUTES.get(task_type, SAFE_FALLBACK_ROUTE)  # type: ignore[arg-type]


def get_role_routes() -> dict[str, Any]:
    """Return a JSON-serializable role routing table."""
    return {
        "routes": {task_type: asdict(route) for task_type, route in ROLE_ROUTES.items()},
        "fallback": asdict(SAFE_FALLBACK_ROUTE),
    }
