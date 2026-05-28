import { batchJobOpportunities, batchMatchAssessments } from "./mockData";
import type { JDAnalyzeResponse, ResumeMatchResponse, ResumeParseResponse, RewriteSuggestion } from "./types";

const fallbackKeywords = ["岗位关键词待确认"];
const userInputRequired = ["真实项目或经历", "个人负责范围", "可验证结果"];

export const authenticityRules = [
  "不得编造公司、学校、项目、奖项、证书、实习、指标、成果。",
  "不得把“熟悉”改成“主导”，除非原文有证据。",
  "不得把课程作业包装成企业项目，除非用户明确提供。",
  "不得虚构量化指标。",
  "不得把JD关键词直接塞进简历，除非简历中有对应经历支撑。",
];

export const insufficientEvidenceMessage = "需要补充真实项目或经历，不能直接写入简历。";

type RewriteInput = {
  targetJobId: string;
  jdResult: JDAnalyzeResponse | null;
  matchResult: ResumeMatchResponse;
  parseResult: ResumeParseResponse;
};

function unique(items: string[], limit = 8): string[] {
  const seen = new Set<string>();
  const output: string[] = [];
  items.forEach((item) => {
    const clean = item.trim();
    if (!clean || seen.has(clean)) return;
    seen.add(clean);
    output.push(clean);
  });
  return output.slice(0, limit);
}

function firstMeaningful(items: string[], fallback: string): string {
  return items.find((item) => item.trim() && !item.includes("未识别") && !item.includes("尚未")) ?? fallback;
}

function hasResumeEvidence(parseResult: ResumeParseResponse, keyword: string): boolean {
  const text = [
    ...parseResult.profile.workExperience,
    ...parseResult.profile.projects,
    parseResult.profile.rawText,
  ].join("\n");
  const normalized = text.toLowerCase();
  const normalizedKeyword = keyword.toLowerCase();
  const hasKeyword = normalized.includes(normalizedKeyword);
  const hasExperienceSignal = ["负责", "参与", "协同", "完成", "输出", "分析", "建设", "整理", "支持", "项目", "经历"].some((term) =>
    normalized.includes(term.toLowerCase()),
  );
  return hasKeyword && hasExperienceSignal;
}

function recommendedJobsAfterRewrite(keywords: string[], targetJobId: string): string[] {
  const normalized = keywords.map((keyword) => keyword.toLowerCase());
  const ranked = batchMatchAssessments
    .filter((assessment) => assessment.recommendationLevel === "priority_apply" || assessment.recommendationLevel === "apply_after_rewrite")
    .map((assessment) => {
      const job = batchJobOpportunities.find((item) => item.jobId === assessment.jobId);
      const haystack = `${job?.title ?? ""} ${job?.industry ?? ""} ${job?.jdText ?? ""}`.toLowerCase();
      const keywordHits = normalized.filter((keyword) => haystack.includes(keyword)).length;
      return {
        jobId: assessment.jobId,
        score: assessment.overallScore + keywordHits * 8 + (assessment.jobId === targetJobId ? 10 : 0),
      };
    })
    .sort((left, right) => right.score - left.score);
  return unique(ranked.map((item) => item.jobId), 4);
}

export function buildRewriteSuggestions({
  targetJobId,
  jdResult,
  matchResult,
  parseResult,
}: RewriteInput): RewriteSuggestion[] {
  const jdKeywords = unique(
    [
      ...(jdResult?.analysis.requiredSkills ?? []),
      ...(jdResult?.analysis.keywords ?? []),
      ...(jdResult?.analysis.responsibilities ?? []),
    ],
    6,
  );
  const keywords = jdKeywords.length ? jdKeywords : fallbackKeywords;
  const primaryRequirement = firstMeaningful(jdResult?.analysis.responsibilities ?? [], keywords[0]);
  const primaryOriginal = firstMeaningful(
    [...parseResult.profile.projects, ...parseResult.profile.workExperience],
    "当前简历还没有可直接支撑该岗位要求的项目表达。",
  );
  const missing = unique([...matchResult.gaps, ...matchResult.risks, ...keywords], 6);
  const supportedKeywords = unique(keywords.filter((keyword) => hasResumeEvidence(parseResult, keyword)), 4);
  const primaryNeedsEvidence = supportedKeywords.length === 0;
  const primaryAllowed = !primaryNeedsEvidence;
  const recommendedJobs = recommendedJobsAfterRewrite(keywords, targetJobId);

  const primary: RewriteSuggestion = {
    targetJobId,
    originalText: primaryOriginal,
    rewrittenText: primaryNeedsEvidence
      ? `${insufficientEvidenceMessage}请先补充项目背景、个人负责范围、使用的方法或工具、可验证结果，再生成目标JD表达。`
      : `建议围绕「${primaryRequirement}」重排这段经历，只保留简历中已有证据能支撑的关键词：${supportedKeywords.join("、")}；不得新增未提供的公司、项目、指标或成果。`,
    reason: "改写目标是提高目标JD和相似JD的关键词命中、证据清晰度和岗位要求覆盖率，而不是单纯润色文字。",
    jdKeywords: primaryAllowed ? supportedKeywords : keywords.slice(0, 4),
    strengthenedRequirement: primaryRequirement,
    evidenceNeeded: primaryNeedsEvidence ? userInputRequired : ["可验证结果", "业务背景"],
    needsUserEvidence: primaryNeedsEvidence,
    evidenceStatus: primaryAllowed ? "direct_rewrite_allowed" : "needs_user_evidence",
    evidence_status: primaryAllowed ? "direct_rewrite_allowed" : "needs_user_evidence",
    allowedToApply: primaryAllowed,
    userInputRequired: primaryAllowed ? [] : userInputRequired,
    confidence: primaryNeedsEvidence ? 0.42 : 0.78,
    recommendedJobsAfterRewrite: recommendedJobs,
  };

  const gapRequirement = firstMeaningful(missing, "目标岗位核心要求");
  const gapKeyword = keywords.find((keyword) => !hasResumeEvidence(parseResult, keyword)) ?? keywords[0];
  const gap: RewriteSuggestion = {
    targetJobId,
    originalText: "简历中该要求的证据较弱或尚未出现。",
    rewrittenText: `${insufficientEvidenceMessage}如果确实做过，请补充任务场景、使用工具、你的动作和结果；课程作业只能按课程作业表述，不能包装成企业项目。`,
    reason: "缺证据的能力如果直接写成优势，容易在HR筛选或面试追问中被判定为不可信。",
    jdKeywords: unique([gapKeyword, ...keywords], 4),
    strengthenedRequirement: gapRequirement,
    evidenceNeeded: ["真实项目或课程作业原始材料", "个人负责范围", "可验证结果或交付物"],
    needsUserEvidence: true,
    evidenceStatus: "needs_user_evidence",
    evidence_status: "needs_user_evidence",
    allowedToApply: false,
    userInputRequired,
    confidence: 0.36,
    recommendedJobsAfterRewrite: recommendedJobs.filter((jobId) => jobId !== targetJobId).slice(0, 3),
  };

  return [primary, gap];
}

export function jobTitleForId(jobId: string): string {
  return batchJobOpportunities.find((job) => job.jobId === jobId)?.title ?? jobId;
}
