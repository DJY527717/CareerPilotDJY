import type { ApplicationPriority, LegacyRecommendationLevel, PreparationStatus } from "../types";

export const recommendationLevelLabels: Record<LegacyRecommendationLevel, string> = {
  priority_apply: "优先投递",
  apply_after_rewrite: "改简历后投",
  cautious_apply: "谨慎投递",
  backup: "备选观察",
  not_recommended: "不建议投",
};

export const recommendationLevelTone: Record<LegacyRecommendationLevel, string> = {
  priority_apply: "priority",
  apply_after_rewrite: "rewrite",
  cautious_apply: "caution",
  backup: "backup",
  not_recommended: "reject",
};

export const preparationStatusLabels: Record<PreparationStatus, string> = {
  ready: "简历已适配",
  needs_rewrite: "简历需修改",
  missing_evidence: "缺少项目证据",
  preference_conflict: "偏好冲突",
  insufficient_jd_info: "JD信息不足",
};

export const preparationStatusTone: Record<PreparationStatus, string> = {
  ready: "ready",
  needs_rewrite: "needs-rewrite",
  missing_evidence: "missing-evidence",
  preference_conflict: "preference-conflict",
  insufficient_jd_info: "insufficient-jd-info",
};

export const applicationPriorityLabels: Record<ApplicationPriority, string> = {
  today_priority: "今日优先",
  weekly_focus: "本周重点",
  after_resume_update: "改简历后投",
  observe_only: "仅观察",
  skip: "不建议投",
};

export const applicationPriorityTone: Record<ApplicationPriority, string> = {
  today_priority: "priority",
  weekly_focus: "focus",
  after_resume_update: "rewrite",
  observe_only: "observe",
  skip: "reject",
};

export const recommendationDecisionNote = "推荐等级应结合分数、风险、偏好和证据强度修正，不只由分数决定。";
