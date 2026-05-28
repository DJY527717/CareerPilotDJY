import type { LegacyRecommendationLevel } from "../types";

export type ResultActionId =
  | "view_evidence"
  | "add_priority_apply"
  | "apply_after_rewrite"
  | "generate_rewrite"
  | "save_backup"
  | "mark_skip"
  | "view_risk"
  | "enter_application_strategy"
  | "export_results"
  | "back_to_jobs";

export const resultActionLabels: Record<ResultActionId, string> = {
  view_evidence: "查看证据",
  add_priority_apply: "加入优先投递",
  apply_after_rewrite: "改简历后投",
  generate_rewrite: "生成改写建议",
  save_backup: "保存到备选",
  mark_skip: "标记为不投",
  view_risk: "查看风险",
  enter_application_strategy: "进入投递策略",
  export_results: "导出结果",
  back_to_jobs: "返回岗位列表",
};

export const primaryActionByRecommendation: Record<LegacyRecommendationLevel, ResultActionId> = {
  priority_apply: "add_priority_apply",
  apply_after_rewrite: "generate_rewrite",
  cautious_apply: "view_risk",
  backup: "save_backup",
  not_recommended: "mark_skip",
};

export function primaryActionForRecommendation(level: LegacyRecommendationLevel): ResultActionId {
  return primaryActionByRecommendation[level];
}
