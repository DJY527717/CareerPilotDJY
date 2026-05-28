import type {
  ApplicationPipelineItem,
  ApplicationStrategy,
  BatchJobRankingResult,
  DashboardMetric,
  GapAnalysisItem,
  InterviewReportSummary,
  JobAnalysisResult,
  JobCard,
  JobOpportunity,
  JobPosting,
  JobPreferenceSummary,
  JobTrendSummary,
  MatchAssessment,
  OpportunityDecisionResult,
  ResumeEvidenceItem,
  ResumeMatchResponse,
  ResumeMatchResult,
  ResumeParseResponse,
  ResumeRewriteSuggestion,
  ResumeSummary,
  SettingsSummary,
  UserProfile,
  UserProfileSummary,
  WorkspaceConfig,
} from "./types";

// This file remains the page-display mock data source.
// Future real API responses should be converted through web/src/api/adapters.ts
// before entering page state, so pages continue to consume camelCase data.

const baseJobPostings: JobPosting[] = [
  {
    id: "job-demo-analysis",
    title: "数据分析岗位",
    companyDisplayName: "示例公司",
    locationDisplayName: "示例城市",
    workMode: "混合办公",
    seniority: "入门/成长型",
    coreSkills: ["SQL", "数据看板", "业务分析", "项目协作"],
    sourceType: "demo",
    industry: "通用业务分析",
    jdText: "负责数据整理、指标看板维护、业务复盘支持和跨角色沟通协作。",
  },
  {
    id: "job-demo-ops",
    title: "产品运营岗位",
    companyDisplayName: "示例公司",
    locationDisplayName: "示例城市",
    workMode: "混合办公",
    seniority: "入门/成长型",
    coreSkills: ["用户研究", "产品运营", "活动复盘", "项目协作"],
    sourceType: "demo",
    industry: "通用产品运营",
    jdText: "支持用户研究、运营活动复盘、需求整理和跨团队协作。",
  },
  {
    id: "job-demo-research",
    title: "行业研究岗位",
    companyDisplayName: "示例公司",
    locationDisplayName: "远程或待确认",
    workMode: "远程协作",
    seniority: "入门/成长型",
    coreSkills: ["资料整理", "结构化分析", "报告输出"],
    sourceType: "demo",
    industry: "通用研究支持",
    jdText: "整理公开资料，输出行业分析、竞品研究和结构化报告。",
  },
  {
    id: "job-demo-sales-ops",
    title: "销售运营岗位",
    companyDisplayName: "示例公司",
    locationDisplayName: "示例城市",
    workMode: "现场办公",
    seniority: "入门/成长型",
    coreSkills: ["线索整理", "客户沟通", "指标跟进"],
    sourceType: "demo",
    industry: "通用运营支持",
    jdText: "承担销售线索整理、客户沟通支持和业绩指标跟进。",
  },
  {
    id: "job-demo-general",
    title: "综合项目助理",
    companyDisplayName: "示例公司",
    locationDisplayName: "待确认",
    workMode: "待确认",
    seniority: "入门/成长型",
    coreSkills: ["资料整理", "会议支持", "项目协作"],
    sourceType: "demo",
    industry: "通用项目支持",
    jdText: "协助完成资料整理、会议支持和其他项目事项。",
  },
];

export const mockUserProfileSummary: UserProfileSummary = {
  displayName: "示例用户",
  loginState: "anonymous",
  profileStatus: "尚未登录，当前为演示数据",
  dataSource: "mock",
};

export const mockResumeSummary: ResumeSummary = {
  id: "resume-demo-empty",
  displayName: "尚未选择简历",
  status: "not_selected",
  evidenceCount: 0,
  lastUpdatedLabel: "待用户上传或粘贴",
};

export const mockJobPreferenceSummary: JobPreferenceSummary = {
  targetRoles: ["尚未设置求职目标"],
  targetIndustries: ["通用行业"],
  locationDisplayName: "示例城市或远程",
  workModes: ["混合办公", "远程协作"],
  excludedSignals: ["职责过泛", "硬性要求不清晰"],
};

export const mockSettingsSummary: SettingsSummary = {
  user: mockUserProfileSummary,
  currentResume: mockResumeSummary,
  preferences: mockJobPreferenceSummary,
  nextActions: ["补充基础档案", "上传或粘贴简历", "确认目标岗位和求职偏好"],
  roleBoundaryNotes: [
    {
      id: "boundary-settings-001",
      module: "settings",
      professionalLens: "senior_hr",
      allowedInputs: ["用户主动填写的档案", "用户主动提供的简历摘要", "用户确认的偏好"],
      blockedOutputs: ["自动生成真实个人信息", "覆盖用户偏好", "替用户确认投递状态"],
      note: "设置模块只维护上下文，不输出最终投递或改写结论。",
    },
  ],
};

export const mockJobAnalysisResult: JobAnalysisResult = {
  id: "jd-analysis-demo-001",
  job: baseJobPostings[0],
  recommendationLevel: "recommend",
  riskLevel: "medium",
  professionalLens: "senior_hr",
  judgement: {
    id: "judgement-jd-001",
    module: "jd",
    professionalLens: "senior_hr",
    conclusion: "岗位职责与数据整理、看板维护和业务复盘相关，适合作为后续匹配的目标JD示例。",
    evidenceRefs: ["job-demo-analysis"],
    riskLevel: "medium",
    nextAction: "补充完整JD后再做简历匹配。",
  },
  responsibilities: ["数据整理", "指标看板维护", "业务复盘支持"],
  hardRequirements: ["SQL或同类数据处理能力", "清晰表达和项目协作能力"],
  keywords: ["SQL", "数据看板", "业务分析", "项目协作"],
  concerns: ["当前为示例JD，职责边界和团队要求仍需用户确认。"],
  nextAction: "粘贴真实JD或继续使用示例进行演示。",
};

export const mockBatchJobRankingResults: BatchJobRankingResult[] = [
  {
    id: "ranking-demo-analysis",
    job: baseJobPostings[0],
    matchScore: 86,
    recommendationLevel: "strongly_recommend",
    riskLevel: "low",
    priority: "P0",
    professionalLens: "headhunter",
    reasons: ["核心技能和岗位职责较清晰", "机会价值适合优先进入候选池", "准备成本相对可控"],
    concerns: ["需要确认具体团队、汇报对象和实际数据权限。"],
    nextAction: "作为优先岗位，先完成简历证据核对。",
  },
  {
    id: "ranking-demo-ops",
    job: baseJobPostings[1],
    matchScore: 78,
    recommendationLevel: "recommend",
    riskLevel: "medium",
    priority: "P1",
    professionalLens: "senior_hr",
    reasons: ["产品运营和用户研究方向相关", "适合作为本周重点岗位"],
    concerns: ["用户研究和活动复盘证据需要更清晰。"],
    nextAction: "改写简历表达后再投递。",
  },
  {
    id: "ranking-demo-research",
    job: baseJobPostings[2],
    matchScore: 66,
    recommendationLevel: "cautious",
    riskLevel: "medium",
    priority: "P2",
    professionalLens: "headhunter",
    reasons: ["岗位有成长价值", "可作为补充方向观察"],
    concerns: ["行业偏好和简历证据不是最强，需要补充报告样例。"],
    nextAction: "补充作品或项目证据后再评估。",
  },
  {
    id: "ranking-demo-sales-ops",
    job: baseJobPostings[3],
    matchScore: 48,
    recommendationLevel: "not_recommend",
    riskLevel: "high",
    priority: "P2",
    professionalLens: "senior_hr",
    reasons: ["岗位信号与当前示例偏好差距较大"],
    concerns: ["销售指标和客户沟通占比可能较高。"],
    nextAction: "不进入当前优先投递列表。",
  },
  {
    id: "ranking-demo-general",
    job: baseJobPostings[4],
    matchScore: 44,
    recommendationLevel: "cautious",
    riskLevel: "high",
    priority: "P2",
    professionalLens: "data_analyst",
    reasons: ["样本中职责较泛，可作为低优先级观察项"],
    concerns: ["职责边界、地点和成长价值不清晰。"],
    nextAction: "仅保留观察，不占用优先投递额度。",
  },
];

export const mockJobTrendSummary: JobTrendSummary = {
  id: "trend-demo-001",
  title: "示例岗位池趋势",
  professionalLens: "data_analyst",
  sampleSize: mockBatchJobRankingResults.length,
  topSkills: ["SQL", "数据整理", "项目协作", "用户研究"],
  marketSignals: ["通用业务分析和产品运营岗位更常出现可迁移技能", "职责清晰的岗位更适合优先进入匹配流程"],
  concerns: ["示例样本量较小，只能用于展示看板结构。"],
};

const mockResumeEvidenceItems: ResumeEvidenceItem[] = [
  {
    id: "evidence-demo-001",
    section: "项目经历",
    sourceExcerpt: "示例项目中参与数据整理和看板维护，输出分析材料草稿。",
    supportedSkills: ["数据整理", "数据看板", "业务分析"],
    evidenceStatus: "weak",
    confidence: 0.62,
  },
  {
    id: "evidence-demo-002",
    section: "项目协作",
    sourceExcerpt: "协同不同角色整理问题清单和复盘材料。",
    supportedSkills: ["项目协作", "结构化表达"],
    evidenceStatus: "supported",
    confidence: 0.72,
  },
];

export const mockResumeMatchResult: ResumeMatchResult = {
  id: "resume-match-demo-001",
  targetJob: baseJobPostings[0],
  matchScore: 72,
  recommendationLevel: "recommend",
  riskLevel: "medium",
  professionalLens: "senior_hr",
  strengths: ["已有数据整理和项目协作相关线索", "可以围绕岗位关键词重新组织表达"],
  gaps: ["缺少可验证的业务结果", "SQL和看板能力仍需要更具体的场景证据"],
  evidence: mockResumeEvidenceItems,
  nextAction: "先补充真实项目细节，再生成可直接应用的改写。",
};

export const mockResumeRewriteSuggestions: ResumeRewriteSuggestion[] = [
  {
    id: "rewrite-demo-001",
    targetSection: "项目经历",
    issue: "表达与JD关键词相关，但证据边界还不够清楚。",
    suggestion: "只围绕已有数据整理、看板维护和复盘材料输出做结构化改写。",
    rewrittenExample: "参与示例项目的数据整理与看板维护，协助统一指标口径并输出业务复盘材料草稿。",
    reason: "这条改写没有新增公司、项目、指标或结果，只把已有事实按JD筛选逻辑重排。",
    evidenceStatus: "direct_rewrite_allowed",
    allowedToApply: true,
    userInputRequired: false,
    professionalLens: "resume_consultant",
    priority: "P0",
  },
  {
    id: "rewrite-demo-002",
    targetSection: "成果描述",
    issue: "缺少真实量化结果，不能直接写成提升、增长或节省。",
    suggestion: "请先补充真实可验证结果；没有结果时，只能保守写交付物和个人负责范围。",
    rewrittenExample: "证据不足，需要用户补充真实项目或经历，不能直接写入简历。",
    reason: "简历顾问角色不能把缺失证据包装成既成成果。",
    evidenceStatus: "needs_user_evidence",
    allowedToApply: false,
    userInputRequired: true,
    professionalLens: "resume_consultant",
    priority: "P0",
  },
];

export const mockGapAnalysisItems: GapAnalysisItem[] = [
  {
    id: "gap-demo-001",
    category: "证据强度",
    gap: "关键技能只出现在描述中，缺少任务、动作和结果链条。",
    impact: "HR可能认为技能停留在了解或协助层面。",
    suggestedAction: "补充真实场景、个人负责范围和交付物。",
    priority: "P0",
    evidenceStatus: "weak",
  },
  {
    id: "gap-demo-002",
    category: "岗位信息",
    gap: "JD职责边界还不完整。",
    impact: "会影响批量排序和投递优先级判断。",
    suggestedAction: "补充岗位职责、任职要求、地点和工作方式。",
    priority: "P1",
    evidenceStatus: "missing",
  },
];

export const mockOpportunityDecisionResults: OpportunityDecisionResult[] = [
  {
    id: "decision-demo-analysis",
    jobTitle: "数据分析岗位",
    recommendationLevel: "strongly_recommend",
    priority: "P0",
    estimatedReadiness: 82,
    keyReasons: ["岗位职责清晰", "示例简历存在相关证据线索", "准备成本较低"],
    keyRisks: ["仍需确认具体团队和真实业务指标要求"],
    nextActions: ["核对简历证据", "准备项目追问", "由用户确认是否投递"],
    professionalLens: "headhunter",
    professionalRole: "application_strategy_advisor",
  },
  {
    id: "decision-demo-ops",
    jobTitle: "产品运营岗位",
    recommendationLevel: "recommend",
    priority: "P1",
    estimatedReadiness: 68,
    keyReasons: ["方向相关", "有用户研究和协作空间"],
    keyRisks: ["简历表达需要更贴近运营指标和复盘产出"],
    nextActions: ["先生成改写建议", "补充真实运营证据", "再确认投递节奏"],
    professionalLens: "senior_hr",
    professionalRole: "application_strategy_advisor",
  },
];

export const mockApplicationPipeline: ApplicationPipelineItem[] = [
  {
    id: "pipeline-demo-001",
    jobTitle: "数据分析岗位",
    companyDisplayName: "示例公司",
    status: "not_applied",
    recommendedPriority: "P0",
    nextSuggestedAction: "用户确认后再标记投递，不由系统自动修改状态。",
    userConfirmed: false,
  },
  {
    id: "pipeline-demo-002",
    jobTitle: "产品运营岗位",
    companyDisplayName: "示例公司",
    status: "not_applied",
    recommendedPriority: "P1",
    nextSuggestedAction: "补充简历证据后再决定是否投递。",
    userConfirmed: false,
  },
];

export const mockInterviewReports: InterviewReportSummary[] = [
  {
    id: "interview-demo-001",
    title: "示例面试复盘",
    professionalLens: "data_analyst",
    readinessScore: 58,
    keyQuestions: ["请说明一个能证明岗位匹配度的真实项目案例。"],
    risks: ["面试记录不足，当前只能展示报告结构。"],
    nextActions: ["补充面试问题", "整理回答证据", "标记需要复盘的风险点"],
  },
];

export const mockDashboardMetrics: DashboardMetric[] = [
  {
    id: "metric-demo-priority",
    label: "优先岗位",
    value: 1,
    trendLabel: "示例岗位池中 P0 数量",
    professionalLens: "data_analyst",
  },
  {
    id: "metric-demo-evidence",
    label: "证据待补",
    value: 2,
    trendLabel: "需要用户补充真实证据的项目",
    professionalLens: "data_analyst",
  },
];

export const mockSampleResumeText = [
  "匿名示例档案",
  "求职目标：尚未设置求职目标",
  "教育背景：待补充",
  "项目经历：在通用示例项目中参与数据看板建设，整理业务指标口径，并输出分析材料草稿。",
  "项目协作经历：协同产品和运营角色整理问题清单和复盘材料。",
  "结果说明：此处仅提示写法，不提供虚构指标；可证明结果需要由用户补充。",
  "技能：SQL、Excel、数据分析、数据看板、业务报告、项目协作",
].join("\n");

export const mockSampleJdText = [
  "岗位名称：数据分析岗位",
  "公司：示例公司",
  "岗位职责：负责业务指标监控、数据看板建设、用户行为分析和业务分析报告输出。",
  "任职要求：本科及以上，熟练使用SQL和Excel，具备数据分析、跨团队项目协作和清晰汇报能力。",
  "加分项：有用户研究支持、产品运营或指标复盘经验。",
].join("\n");

export const userProfile: UserProfile = {
  name: mockUserProfileSummary.displayName,
  resumeName: "尚未选择简历",
  target: mockJobPreferenceSummary.targetRoles[0],
  location: mockJobPreferenceSummary.locationDisplayName,
  stage: "演示模式",
};

export const workspaces: WorkspaceConfig[] = [
  {
    key: "settings",
    label: "设置与资料",
    title: "设置与资料",
    subtitle: "维护通用档案、当前简历和求职偏好。",
    status: "演示模式",
    modes: [
      { key: "settings-profile", label: "个人档案" },
      { key: "settings-resume", label: "当前简历" },
      { key: "settings-preferences", label: "求职偏好" },
    ],
  },
  {
    key: "jd",
    label: "JD分析",
    title: "JD分析",
    subtitle: "拆解岗位职责、硬性要求和关键词。",
    status: "待确认",
    modes: [
      { key: "jd-single", label: "单条JD分析" },
      { key: "jd-batch", label: "批量JD筛选" },
      { key: "jd-monitor", label: "招聘趋势" },
    ],
  },
  {
    key: "resume",
    label: "简历优化",
    title: "简历解析与JD匹配",
    subtitle: "模拟资深HR初筛，判断简历证据、岗位风险和面试推进建议。",
    status: "核心阶段",
    modes: [
      { key: "resume-match", label: "简历解析与匹配" },
      { key: "resume-rewrite", label: "目标JD改简历" },
      { key: "resume-gap", label: "能力缺口" },
    ],
  },
  {
    key: "decision",
    label: "求职决策",
    title: "求职决策",
    subtitle: "比较机会质量和投递优先级。",
    status: "待确认",
    modes: [
      { key: "decision-offer", label: "机会评估" },
      { key: "decision-internship", label: "岗位比较" },
      { key: "decision-pipeline", label: "投递管理" },
    ],
  },
  {
    key: "report",
    label: "面试报告",
    title: "面试报告",
    subtitle: "沉淀面试复盘和后续行动。",
    status: "待确认",
    modes: [
      { key: "report-interview", label: "面试记录" },
      { key: "report-dashboard", label: "数据看板" },
    ],
  },
];

export const jobs: JobCard[] = mockBatchJobRankingResults.slice(0, 3).map((item) => ({
  id: item.job.id,
  title: item.job.title,
  company: item.job.companyDisplayName,
  location: item.job.locationDisplayName,
  track: item.job.industry ?? "通用岗位",
  matchScore: item.matchScore,
  salary: "待确认",
  highlights: item.job.coreSkills,
}));

export const batchJobOpportunities: JobOpportunity[] = baseJobPostings.map((job) => ({
  jobId: job.id,
  title: job.title,
  company: job.companyDisplayName,
  location: job.locationDisplayName,
  industry: job.industry ?? "通用岗位",
  jdText: job.jdText ?? "",
  source: "示例岗位池",
  capturedAt: "",
}));

export const batchMatchAssessments: MatchAssessment[] = mockBatchJobRankingResults.map((item) => ({
  jobId: item.job.id,
  overallScore: item.matchScore,
  hardMatchScore: item.priority === "P0" ? 82 : item.priority === "P1" ? 74 : 60,
  preferenceFitScore: item.riskLevel === "low" ? 88 : item.riskLevel === "medium" ? 68 : 48,
  evidenceScore: item.recommendationLevel === "strongly_recommend" ? 86 : item.recommendationLevel === "recommend" ? 70 : 42,
  growthValueScore: item.matchScore,
  riskScore: item.riskLevel === "low" ? 18 : item.riskLevel === "medium" ? 38 : 72,
  recommendationLevel:
    item.recommendationLevel === "strongly_recommend"
      ? "priority_apply"
      : item.recommendationLevel === "recommend"
        ? "apply_after_rewrite"
        : item.recommendationLevel === "cautious"
          ? "cautious_apply"
          : "not_recommended",
  recommendationReason: item.reasons[0] ?? "当前仅返回示例排序结论。",
  risks: item.concerns,
  missingEvidence: item.riskLevel === "low" ? [] : ["真实项目证据待补充"],
  weakEvidence: item.riskLevel === "high" ? ["职责边界", "成果证据"] : ["业务影响证据"],
  nextAction: item.nextAction,
}));

export const applicationStrategies: ApplicationStrategy[] = mockOpportunityDecisionResults.map((item) => ({
  jobId: item.id.replace("decision-demo-", "job-demo-"),
  priority: item.priority === "P0" ? "today_priority" : item.priority === "P1" ? "weekly_focus" : "observe_only",
  applicationStatus: "not_applied",
  preparationStatus: item.estimatedReadiness >= 80 ? "ready" : "needs_rewrite",
  timing: item.priority === "P0" ? "今天完成准备后由用户确认投递" : "本周内完成准备后再评估",
  requiredPreparation: item.nextActions,
  followUpAction: "只给出建议动作，不自动修改投递状态。",
  reason: item.keyReasons[0] ?? "示例策略结论。",
  plannedApplyDate: null,
  followUpReminder: null,
  interviewStage: null,
  reviewResult: null,
}));

export function applicationJobTitle(jobId: string): string {
  return batchJobOpportunities.find((job) => job.jobId === jobId)?.title ?? "演示岗位";
}

export function applicationCompany(jobId: string): string {
  return batchJobOpportunities.find((job) => job.jobId === jobId)?.company ?? "示例公司";
}

export const emptyResumeParse: ResumeParseResponse = {
  profile: {
    basicInfo: {
      candidateName: "匿名示例档案",
      fileName: "尚未选择简历",
      targetRole: mockJobPreferenceSummary.targetRoles[0],
      contact: "未在演示结果中展示个人联系方式",
    },
    education: ["尚未识别到明确内容"],
    workExperience: ["尚未识别到明确内容"],
    projects: ["尚未识别到明确内容"],
    skills: [],
    certificates: ["尚未识别到明确内容"],
    languages: ["尚未识别到明确语言能力"],
    rawText: "",
  },
  parsingNotes: ["粘贴简历后，系统会识别教育、经历、项目、技能和证书，并判断信息是否能支撑目标岗位。"],
};

export const emptyMatchResult: ResumeMatchResponse = {
  overallScore: 0,
  requirementFitScore: 0,
  skillFitScore: 0,
  experienceRelevanceScore: 0,
  achievementEvidenceScore: 0,
  resumeQualityScore: 0,
  keywordMatchScore: 0,
  riskLevel: "medium",
  hrDecision: "maybe",
  hrSummary: "尚未开始分析。粘贴简历和JD后，系统会从HR初筛视角输出匹配判断。",
  strengths: ["尚未上传简历"],
  gaps: ["尚未设置求职目标"],
  risks: ["尚未提供足够材料，无法判断岗位匹配风险。"],
  interviewRecommendation: "等待分析。",
  likelyInterviewQuestions: ["分析后将生成HR可能追问的问题。"],
  resumeRewriteSuggestions: ["分析后将生成简历优先改写建议。"],
  recommendedActions: ["粘贴简历文本和目标JD，点击开始分析。"],
};
