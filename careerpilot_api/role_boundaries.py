"""Input and output boundaries for CareerPilot Web professional roles.

These boundaries keep each role focused on its own evidence and outputs. They
are contracts, not execution prompts, and should be checked before a role is
allowed to produce product judgment.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


CoreRoleId = Literal[
    "batch_jd_screener",
    "batch_result_analyst",
    "resume_match_reviewer",
    "resume_rewrite_advisor",
    "application_strategy_advisor",
]

EVIDENCE_FALLBACK_MESSAGE = "证据不足，需要用户补充"


@dataclass(frozen=True)
class RoleBoundary:
    role_id: CoreRoleId
    allowed_inputs: tuple[str, ...]
    blocked_inputs: tuple[str, ...]
    required_outputs: tuple[str, ...]
    forbidden_outputs: tuple[str, ...]
    evidence_required: tuple[str, ...]


COMMON_FORBIDDEN_OUTPUTS = (
    "不得输出没有证据来源的建议",
    "不得编造简历经历、项目成果、业务指标、证书或岗位要求",
    f"证据不足时必须输出“{EVIDENCE_FALLBACK_MESSAGE}”",
)

COMMON_EVIDENCE_REQUIRED = (
    "每条建议必须绑定至少一个证据来源",
    "证据来源必须来自JD、简历片段、用户偏好、匹配结果或用户确认的状态记录",
    f"证据不足时必须保留占位结论：“{EVIDENCE_FALLBACK_MESSAGE}”",
)


ROLE_BOUNDARIES: dict[CoreRoleId, RoleBoundary] = {
    "batch_jd_screener": RoleBoundary(
        role_id="batch_jd_screener",
        allowed_inputs=("JD列表", "ResumeEvidence简历证据", "用户求职偏好", "历史匹配和排序结果"),
        blocked_inputs=("简历改写草稿", "未授权完整简历原文", "投递状态写入权限", "用户联系方式"),
        required_outputs=("MatchAssessment列表", "recommendation_level", "recommendation_reason", "risks", "next_action", "证据来源"),
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
        + (
            "不得直接修改或生成简历改写内容",
            "不得把所有岗位都评为高优先级",
        ),
        evidence_required=("对应JD", "对应简历证据", "对应用户偏好项") + COMMON_EVIDENCE_REQUIRED,
    ),
    "batch_result_analyst": RoleBoundary(
        role_id="batch_result_analyst",
        allowed_inputs=("筛选后的结构化MatchAssessment结果", "岗位分组统计", "风险和短板统计", "偏好冲突统计"),
        blocked_inputs=("原始完整简历", "未筛选JD全文池", "简历改写草稿", "投递状态写入权限"),
        required_outputs=("一句话看板结论", "分组数量", "Top岗位摘要", "风险岗位摘要", "能力短板汇总", "证据来源"),
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
        + (
            "不得重新判断岗位适不适合",
            "不得输出大量主观HR长评",
            "不得改写原MatchAssessment推荐等级",
        ),
        evidence_required=("对应筛选结果", "对应统计字段", "对应岗位分组") + COMMON_EVIDENCE_REQUIRED,
    ),
    "resume_match_reviewer": RoleBoundary(
        role_id="resume_match_reviewer",
        allowed_inputs=("目标JD", "ResumeEvidence简历证据", "简历片段", "用户偏好摘要", "匹配评分上下文"),
        blocked_inputs=("简历改写草稿", "投递状态写入权限", "无关岗位池", "用户联系方式"),
        required_outputs=("匹配判断", "证据映射", "风险", "missing_evidence", "weak_evidence", "next_action", "证据来源"),
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
        + (
            "不得直接修改用户简历",
            "不得把技能清单直接当作项目经验证据",
        ),
        evidence_required=("对应JD要求", "对应简历片段", "对应证据强度") + COMMON_EVIDENCE_REQUIRED,
    ),
    "resume_rewrite_advisor": RoleBoundary(
        role_id="resume_rewrite_advisor",
        allowed_inputs=("目标JD", "简历片段", "匹配短板", "ResumeEvidence简历证据", "JD关键词"),
        blocked_inputs=("投递状态", "投递状态写入权限", "用户偏好覆盖权限", "无关面试记录"),
        required_outputs=("RewriteSuggestion列表", "original_text", "rewritten_text", "reason", "jd_keywords", "evidence_needed", "证据来源"),
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
        + (
            "不得读取投递状态作为改写依据",
            "不得直接覆盖用户原始简历",
            "不得把缺失证据写成既成经历",
        ),
        evidence_required=("对应目标JD要求", "对应简历原文片段", "对应匹配短板") + COMMON_EVIDENCE_REQUIRED,
    ),
    "application_strategy_advisor": RoleBoundary(
        role_id="application_strategy_advisor",
        allowed_inputs=("匹配结果", "简历准备度", "用户求职偏好", "用户确认的投递状态", "岗位风险"),
        blocked_inputs=("投递状态写入权限", "偏好覆盖权限", "简历改写草稿全文", "用户联系方式"),
        required_outputs=("ApplicationStrategy", "投递优先级", "准备状态", "下一步动作", "策略原因", "证据来源"),
        forbidden_outputs=COMMON_FORBIDDEN_OUTPUTS
        + (
            "不得修改用户投递状态",
            "不得覆盖用户偏好",
            "不得承诺面试、录用或投递成功率",
        ),
        evidence_required=("对应MatchAssessment", "对应简历准备度", "对应用户偏好项", "对应用户确认状态") + COMMON_EVIDENCE_REQUIRED,
    ),
}


def get_role_boundary(role_id: str) -> RoleBoundary | None:
    """Return the boundary contract for a core role when one exists."""
    return ROLE_BOUNDARIES.get(role_id)  # type: ignore[arg-type]


def get_role_boundaries() -> dict[str, Any]:
    """Return JSON-serializable role boundary contracts."""
    return {
        "evidence_fallback_message": EVIDENCE_FALLBACK_MESSAGE,
        "boundaries": {role_id: asdict(boundary) for role_id, boundary in ROLE_BOUNDARIES.items()},
    }
