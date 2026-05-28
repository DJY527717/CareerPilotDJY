"""Schemas for the CareerPilot Web API."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal


WorkspaceKey = Literal["settings", "jd", "resume", "decision", "report"]
ModeKey = Literal[
    "settings-profile",
    "settings-resume",
    "settings-preferences",
    "jd-single",
    "jd-batch",
    "jd-monitor",
    "resume-match",
    "resume-rewrite",
    "resume-gap",
    "decision-offer",
    "decision-internship",
    "decision-pipeline",
    "report-interview",
    "report-dashboard",
]
Priority = Literal["高", "中", "低"]
RiskLevel = Literal["low", "medium", "high"]
HRDecision = Literal["recommend_interview", "maybe", "not_recommended"]
RecommendationLevel = Literal[
    "priority_apply",
    "apply_after_rewrite",
    "cautious_apply",
    "backup",
    "not_recommended",
]
ApplicationPriority = Literal["today_priority", "weekly_focus", "after_resume_update", "observe_only", "skip"]
PreparationStatus = Literal["ready", "needs_rewrite", "missing_evidence", "preference_conflict", "insufficient_jd_info"]
ApplicationStatus = Literal["not_applied", "ready_to_apply", "applied", "followed_up", "interviewing", "closed"]
EvidenceStatus = Literal["direct_rewrite_allowed", "needs_user_evidence"]

AUTHENTICITY_RULES: tuple[str, ...] = (
    "不得编造公司、学校、项目、奖项、证书、实习、指标、成果。",
    "不得把“熟悉”改成“主导”，除非原文有证据。",
    "不得把课程作业包装成企业项目，除非用户明确提供。",
    "不得虚构量化指标。",
    "不得把JD关键词直接塞进简历，除非简历中有对应经历支撑。",
)

AUTHENTICITY_FALLBACK_MESSAGE = "需要补充真实项目或经历，不能直接写入简历。"

RECOMMENDATION_LEVEL_LABELS: dict[str, str] = {
    "priority_apply": "优先投递",
    "apply_after_rewrite": "改简历后投",
    "cautious_apply": "谨慎投递",
    "backup": "备选观察",
    "not_recommended": "不建议投",
}

PREPARATION_STATUS_LABELS: dict[str, str] = {
    "ready": "简历已适配",
    "needs_rewrite": "简历需修改",
    "missing_evidence": "缺少项目证据",
    "preference_conflict": "偏好冲突",
    "insufficient_jd_info": "JD信息不足",
}

APPLICATION_PRIORITY_LABELS: dict[str, str] = {
    "today_priority": "今日优先",
    "weekly_focus": "本周重点",
    "after_resume_update": "改简历后投",
    "observe_only": "仅观察",
    "skip": "不建议投",
}

RECOMMENDATION_DECISION_NOTE = "推荐等级应结合分数、风险、偏好和证据强度修正，不只由分数决定。"


@dataclass(frozen=True)
class ModeSchema:
    key: ModeKey
    label: str


@dataclass(frozen=True)
class WorkspaceSchema:
    key: WorkspaceKey
    label: str
    title: str
    subtitle: str
    status: str
    modes: list[ModeSchema]


@dataclass(frozen=True)
class UserSummarySchema:
    name: str
    resume_name: str
    target: str
    location: str
    stage: str


@dataclass(frozen=True)
class JobCardSchema:
    id: str
    title: str
    company: str
    location: str
    track: str
    match_score: int
    salary: str
    highlights: list[str]


@dataclass(frozen=True)
class JobOpportunity:
    job_id: str
    title: str
    company: str
    location: str
    industry: str
    jd_text: str
    source: str
    captured_at: str


@dataclass(frozen=True)
class ResumeEvidence:
    evidence_id: str
    section: str
    original_text: str
    skills: list[str]
    metrics: list[str]
    confidence: float


@dataclass(frozen=True)
class MatchAssessment:
    job_id: str
    overall_score: int
    hard_match_score: int
    preference_fit_score: int
    evidence_score: int
    growth_value_score: int
    risk_score: int
    recommendation_level: RecommendationLevel
    recommendation_reason: str
    risks: list[str]
    missing_evidence: list[str]
    weak_evidence: list[str]
    next_action: str


@dataclass(frozen=True)
class RewriteSuggestion:
    target_job_id: str
    original_text: str
    rewritten_text: str
    reason: str
    jd_keywords: list[str]
    strengthened_requirement: str
    evidence_needed: list[str]
    needs_user_evidence: bool
    evidence_status: EvidenceStatus
    allowed_to_apply: bool
    user_input_required: list[str]
    confidence: float
    recommended_jobs_after_rewrite: list[str]


@dataclass(frozen=True)
class ApplicationStrategy:
    job_id: str
    priority: ApplicationPriority
    application_status: ApplicationStatus
    preparation_status: PreparationStatus
    timing: str
    required_preparation: list[str]
    follow_up_action: str
    reason: str
    planned_apply_date: str | None = None
    follow_up_reminder: str | None = None
    interview_stage: str | None = None
    review_result: str | None = None


@dataclass(frozen=True)
class ScoreItemSchema:
    id: str
    label: str
    value: int
    note: str


@dataclass(frozen=True)
class ActionItemSchema:
    id: str
    title: str
    detail: str
    priority: Priority


@dataclass(frozen=True)
class ResumeParseRequest:
    resume_text: str
    file_name: str | None = None


@dataclass(frozen=True)
class ResumeProfile:
    basicInfo: dict[str, str]
    education: list[str]
    workExperience: list[str]
    projects: list[str]
    skills: list[str]
    certificates: list[str]
    languages: list[str]
    rawText: str


@dataclass(frozen=True)
class ResumeParseResponse:
    profile: ResumeProfile
    parsingNotes: list[str]


@dataclass(frozen=True)
class JDAnalyzeRequest:
    jd_text: str


@dataclass(frozen=True)
class JDAnalysis:
    title: str
    company: str
    location: str
    seniority: str
    responsibilities: list[str]
    requiredSkills: list[str]
    preferredSkills: list[str]
    experienceRequirements: list[str]
    educationRequirements: list[str]
    keywords: list[str]


@dataclass(frozen=True)
class JDAnalyzeResponse:
    analysis: JDAnalysis
    analysisNotes: list[str]


@dataclass(frozen=True)
class ResumeMatchRequest:
    resume_text: str
    jd_text: str


@dataclass(frozen=True)
class ResumeMatchResponse:
    overallScore: int
    requirementFitScore: int
    skillFitScore: int
    experienceRelevanceScore: int
    achievementEvidenceScore: int
    resumeQualityScore: int
    keywordMatchScore: int
    riskLevel: RiskLevel
    hrDecision: HRDecision
    hrSummary: str
    strengths: list[str]
    gaps: list[str]
    risks: list[str]
    interviewRecommendation: str
    likelyInterviewQuestions: list[str]
    resumeRewriteSuggestions: list[str]
    recommendedActions: list[str]


@dataclass(frozen=True)
class ApiEnvelope:
    ok: bool
    data: Any


def to_jsonable(value: Any) -> Any:
    """Convert dataclasses and nested values into JSON-serializable data."""
    if hasattr(value, "__dataclass_fields__"):
        return asdict(value)
    if isinstance(value, list):
        return [to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: to_jsonable(item) for key, item in value.items()}
    return value
