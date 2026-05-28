import type {
  RawApplicationPipelineItem,
  RawAuthSession,
  RawBatchJobRankingResult,
  RawBootstrapResponse,
  RawDashboardMetric,
  RawGapAnalysisItem,
  RawInterviewReportSummary,
  RawJobAnalysisResult,
  RawJobPosting,
  RawJobTrendSummary,
  RawOpportunityDecisionResult,
  RawResumeMatchResponse,
  RawResumeParseResponse,
  RawResumeRewriteSuggestion,
  RawSettingsSummary,
  RawWorkspace,
} from "./contracts";

const rawDemoJobPosting: RawJobPosting = {
  id: "job-demo-analysis",
  user_id: "demo-user",
  workspace_id: "demo-workspace",
  title: "Demo analyst role",
  company_display_name: "Demo company",
  location_display_name: "Demo city",
  work_mode: "Hybrid",
  seniority: "Entry",
  core_skills: ["SQL", "dashboarding", "business analysis", "collaboration"],
  source_type: "demo",
  jd_text: "Demo role focused on dashboard maintenance, metric review, and cross-functional analysis.",
  industry: "Business analysis",
  created_at: "",
  updated_at: "",
};

export const rawAuthSession: RawAuthSession = {
  authenticated: true,
  session_id: "demo-session",
  user: {
    id: "demo-user",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    email: "demo@example.invalid",
    display_name: "Demo user",
    role: "member",
    is_admin: false,
    default_workspace_id: "demo-workspace",
    created_at: "",
    updated_at: "",
  },
  selected_workspace_id: "demo-workspace",
  expires_at: "",
};

export const rawWorkspaces: RawWorkspace[] = [
  {
    workspace_id: "demo-workspace",
    owner_user_id: "demo-user",
    name: "Demo workspace",
    role: "owner",
    created_at: "",
    updated_at: "",
  },
];

export const rawSettingsSummary: RawSettingsSummary = {
  user: {
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    display_name: "Demo user",
    role: "viewer",
    is_admin: false,
    login_state: "anonymous",
    profile_status: "Demo profile only",
    data_source: "mock",
    created_at: "",
    updated_at: "",
  },
  current_resume: {
    id: "resume-demo-empty",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    display_name: "Demo resume",
    status: "not_selected",
    evidence_count: 0,
    last_updated_label: "Waiting for user input",
    uploaded_file: {
      uploaded_file_id: "demo-file",
      storage_key: "",
      file_name: "demo-resume.txt",
      mime_type: "text/plain",
      size_bytes: 0,
      user_id: "demo-user",
      workspace_id: "demo-workspace",
      created_at: "",
      updated_at: "",
    },
    created_at: "",
    updated_at: "",
  },
  preferences: {
    target_roles: ["Demo target role"],
    target_industries: ["General business"],
    location_display_name: "Demo city",
    work_modes: ["Hybrid"],
    excluded_signals: ["unclear responsibilities"],
  },
  next_actions: ["Add profile context", "Paste resume text", "Confirm target role"],
  role_boundary_notes: [
    {
      id: "boundary-settings-demo",
      module: "settings",
      professional_lens: "senior_hr",
      allowed_inputs: ["user-provided profile", "user-confirmed preferences"],
      blocked_outputs: ["real personal data generation", "automatic application status changes"],
      note: "Settings maintain context only.",
    },
  ],
  workspaces: [
    {
      id: "demo-workspace",
      name: "Demo workspace",
      role: "viewer",
      is_active: true,
      created_at: "",
      updated_at: "",
    },
  ],
  admin_metrics: {
    usage_events: [],
    daily_metrics: [],
  },
};

export const rawBootstrapResponse: RawBootstrapResponse = {
  me: {
    id: "demo-user",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    email: "demo@example.invalid",
    display_name: "Demo user",
    role: "viewer",
    is_admin: false,
    default_workspace_id: "demo-workspace",
    created_at: "",
    updated_at: "",
  },
  workspaces: rawSettingsSummary.workspaces,
  settings_summary: rawSettingsSummary,
};

export const rawJobAnalysisResult: RawJobAnalysisResult = {
  id: "jd-analysis-demo",
  user_id: "demo-user",
  workspace_id: "demo-workspace",
  job: rawDemoJobPosting,
  recommendation_level: "recommend",
  risk_level: "medium",
  professional_lens: "senior_hr",
  judgement: {
    id: "judgement-jd-demo",
    module: "jd",
    professional_lens: "senior_hr",
    conclusion: "Demo JD has usable analysis signals.",
    evidence_refs: ["job-demo-analysis"],
    risk_level: "medium",
    next_action: "Add verified resume evidence before applying.",
  },
  responsibilities: ["dashboard maintenance", "metric review", "analysis support"],
  hard_requirements: ["SQL", "structured communication"],
  keywords: ["SQL", "dashboarding", "business analysis"],
  concerns: ["Demo data only; confirm real JD details before decisions."],
  next_action: "Review JD evidence.",
};

export const rawBatchJobRankingResults: RawBatchJobRankingResult[] = [
  {
    id: "ranking-demo-analysis",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    job: rawDemoJobPosting,
    match_score: 86,
    recommendation_level: "strongly_recommend",
    risk_level: "low",
    priority: "P0",
    professional_lens: "headhunter",
    reasons: ["Clear role signals", "Low preparation cost"],
    concerns: ["Confirm team scope before applying."],
    next_action: "Verify resume evidence.",
  },
  {
    id: "ranking-demo-ops",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    job: {
      ...rawDemoJobPosting,
      id: "job-demo-ops",
      title: "Demo operations role",
      core_skills: ["operations", "research", "collaboration"],
      industry: "Operations",
    },
    match_score: 72,
    recommendation_level: "recommend",
    risk_level: "medium",
    priority: "P1",
    professional_lens: "senior_hr",
    reasons: ["Related direction", "Evidence needs review"],
    concerns: ["Needs more verified project evidence."],
    next_action: "Prepare resume evidence.",
  },
];

export const rawJobTrendSummary: RawJobTrendSummary = {
  id: "trend-demo",
  workspace_id: "demo-workspace",
  title: "Demo job pool trend",
  professional_lens: "data_analyst",
  sample_size: rawBatchJobRankingResults.length,
  top_skills: ["SQL", "analysis", "collaboration"],
  market_signals: ["Demo jobs emphasize structured evidence."],
  concerns: ["Small demo sample only."],
};

export const rawResumeParseResponse: RawResumeParseResponse = {
  profile: {
    basic_info: {
      candidateName: "Anonymous demo profile",
      fileName: "demo-resume.txt",
      targetRole: "Demo target role",
      contact: "Not shown in demo output",
    },
    education: ["Waiting for user input"],
    work_experience: ["Waiting for user input"],
    projects: ["Demo project evidence placeholder"],
    skills: ["SQL", "analysis"],
    certificates: [],
    languages: [],
    raw_text: "",
  },
  parsing_notes: ["Resume parsing is offline demo data until live API is connected."],
};

export const rawResumeMatchResponse: RawResumeMatchResponse = {
  overall_score: 72,
  requirement_fit_score: 70,
  skill_fit_score: 74,
  experience_relevance_score: 68,
  achievement_evidence_score: 52,
  resume_quality_score: 66,
  keyword_match_score: 71,
  risk_level: "medium",
  hr_decision: "maybe",
  hr_summary: "Offline demo result. Live API responses must pass through adapters.",
  strengths: ["analysis signal", "collaboration signal"],
  gaps: ["verified outcome evidence"],
  risks: ["Demo data is not a hiring decision."],
  interview_recommendation: "Add verified evidence before preparing interview points.",
  likely_interview_questions: ["Describe one verified project relevant to the target role."],
  resume_rewrite_suggestions: ["Use only user-provided evidence."],
  recommended_actions: ["Add real project evidence", "Confirm target JD details"],
};

export const rawResumeRewriteSuggestions: RawResumeRewriteSuggestion[] = [
  {
    id: "rewrite-demo",
    target_section: "Project experience",
    issue: "Evidence needs user confirmation.",
    suggestion: "Rewrite only with verified user-provided facts.",
    rewritten_example: "Demo rewrite placeholder based on provided evidence.",
    reason: "No new facts should be invented.",
    evidence_status: "needs_user_evidence",
    allowed_to_apply: false,
    user_input_required: true,
    professional_lens: "resume_consultant",
    priority: "P0",
  },
];

export const rawGapAnalysisItems: RawGapAnalysisItem[] = [
  {
    id: "gap-demo",
    category: "Evidence",
    gap: "Outcome evidence is not verified.",
    impact: "Match confidence remains limited.",
    suggested_action: "Add real task, action, and result evidence.",
    priority: "P0",
    evidence_status: "missing",
  },
];

export const rawOpportunityDecisionResults: RawOpportunityDecisionResult[] = [
  {
    id: "decision-demo-analysis",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    job_title: "Demo analyst role",
    recommendation_level: "recommend",
    priority: "P1",
    estimated_readiness: 70,
    key_reasons: ["Related role direction"],
    key_risks: ["Missing verified project evidence"],
    next_actions: ["Add resume evidence", "Confirm user decision before applying"],
    professional_lens: "headhunter",
    professional_role: "application_strategy_advisor",
  },
];

export const rawApplicationPipeline: RawApplicationPipelineItem[] = [
  {
    id: "pipeline-demo",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    job_title: "Demo analyst role",
    company_display_name: "Demo company",
    status: "not_applied",
    recommended_priority: "P1",
    next_suggested_action: "Wait for user confirmation before changing status.",
    user_confirmed: false,
  },
];

export const rawInterviewReports: RawInterviewReportSummary[] = [
  {
    id: "interview-demo",
    user_id: "demo-user",
    workspace_id: "demo-workspace",
    title: "Demo interview summary",
    professional_lens: "data_analyst",
    readiness_score: 58,
    key_questions: ["Add one real interview question before analysis."],
    risks: ["Demo report only."],
    next_actions: ["Add interview notes", "Review evidence"],
  },
];

export const rawDashboardMetrics: RawDashboardMetric[] = [
  {
    id: "metric-demo-priority",
    workspace_id: "demo-workspace",
    label: "Priority jobs",
    value: 1,
    trend_label: "Demo count only",
    professional_lens: "data_analyst",
  },
];
