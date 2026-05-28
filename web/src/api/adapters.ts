import type {
  RawAdminMetrics,
  RawApplicationPipelineItem,
  RawAuthResponse,
  RawAuthSession,
  RawBatchJobRankingResult,
  RawBootstrapResponse,
  RawCurrentUser,
  RawDailyMetric,
  RawDashboardMetric,
  RawGapAnalysisItem,
  RawInterviewReportSummary,
  RawJobAnalysisResult,
  RawJobPosting,
  RawJobPreferenceSummary,
  RawJobTrendSummary,
  RawOpportunityDecisionResult,
  RawProfessionalJudgement,
  RawResumeEvidenceItem,
  RawResumeMatchResponse,
  RawResumeMatchResult,
  RawResumeParseResponse,
  RawResumeRewriteSuggestion,
  RawResumeSummary,
  RawRoleBoundaryNote,
  RawSettingsSummary,
  RawUploadedFileMeta,
  RawUsageEvent,
  RawUserProfileSummary,
  RawWorkspace,
  RawWorkspaceSummary,
  RawWorkspaceSelectionResponse,
} from "./contracts";
import type {
  AdminMetrics,
  ApplicationPipelineItem,
  ApplicationStatus,
  AuthSession,
  BatchJobRankingResult,
  CurrentUser,
  DailyMetric,
  DashboardMetric,
  EvidenceStatus,
  GapAnalysisItem,
  HRDecision,
  InterviewReportSummary,
  JDAnalyzeResponse,
  JobAnalysisResult,
  JobPosting,
  JobPreferenceSummary,
  JobTrendSummary,
  ModuleKey,
  OpportunityDecisionResult,
  PriorityLevel,
  ProfessionalJudgement,
  ProfessionalLens,
  ProfessionalRoleId,
  RecommendationLevel,
  ResumeEvidenceItem,
  ResumeMatchResponse,
  ResumeMatchResult,
  ResumeParseResponse,
  ResumeRewriteSuggestion,
  ResumeSummary,
  RiskLevel,
  RoleBoundaryNote,
  SettingsSummary,
  UploadedFileMeta,
  UsageEvent,
  UsageEventType,
  UserProfile,
  UserProfileSummary,
  UserRole,
  Workspace,
  WorkspaceConfig,
  WorkspaceSummary,
} from "../types";

const DEFAULT_USER_ID = "demo-user";
const DEFAULT_WORKSPACE_ID = "demo-workspace";
const DEFAULT_TIMESTAMP = "";

function asObject(value: unknown): Record<string, unknown> {
  return value !== null && typeof value === "object" ? (value as Record<string, unknown>) : {};
}

function toStringValue(value: unknown, fallback = ""): string {
  return typeof value === "string" && value.trim() ? value : fallback;
}

function toNumberValue(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function toBooleanValue(value: unknown, fallback = false): boolean {
  return typeof value === "boolean" ? value : fallback;
}

function toStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.filter((item): item is string => typeof item === "string" && Boolean(item.trim())) : [];
}

function toRecordString(value: unknown): Record<string, string> {
  return Object.fromEntries(
    Object.entries(asObject(value)).filter((entry): entry is [string, string] => typeof entry[1] === "string"),
  );
}

function toUserRole(value: unknown): UserRole {
  return value === "owner" || value === "admin" || value === "member" || value === "viewer" ? value : "viewer";
}

function toRiskLevel(value: unknown): RiskLevel {
  return value === "low" || value === "medium" || value === "high" ? value : "medium";
}

function toRecommendationLevel(value: unknown): RecommendationLevel {
  return value === "strongly_recommend" || value === "recommend" || value === "cautious" || value === "not_recommend"
    ? value
    : "cautious";
}

function toPriorityLevel(value: unknown): PriorityLevel {
  return value === "P0" || value === "P1" || value === "P2" ? value : "P2";
}

function toProfessionalLens(value: unknown): ProfessionalLens {
  return value === "senior_hr" || value === "headhunter" || value === "resume_consultant" || value === "data_analyst"
    ? value
    : "senior_hr";
}

function toModuleKey(value: unknown): ModuleKey {
  return value === "settings" || value === "jd" || value === "resume" || value === "decision" || value === "report"
    ? value
    : "settings";
}

function toApplicationStatus(value: unknown): ApplicationStatus {
  return value === "not_applied" ||
    value === "ready_to_apply" ||
    value === "applied" ||
    value === "followed_up" ||
    value === "interviewing" ||
    value === "closed"
    ? value
    : "not_applied";
}

function toHrDecision(value: unknown): HRDecision {
  return value === "recommend_interview" || value === "not_recommended" || value === "maybe" ? value : "maybe";
}

function toUsageEventType(value: unknown): UsageEventType {
  return value === "bootstrap_viewed" ||
    value === "jd_analyzed" ||
    value === "jd_batch_screened" ||
    value === "resume_parsed" ||
    value === "resume_matched" ||
    value === "decision_evaluated" ||
    value === "report_viewed"
    ? value
    : "bootstrap_viewed";
}

function toProfessionalRole(value: unknown): ProfessionalRoleId | undefined {
  return value === "senior_hr" ||
    value === "headhunter" ||
    value === "resume_consultant" ||
    value === "data_analyst" ||
    value === "application_strategy_advisor" ||
    value === "interview_review_advisor" ||
    value === "report_analyst"
    ? value
    : undefined;
}

function toEvidenceSupportStatus(value: unknown): "supported" | "weak" | "missing" {
  return value === "supported" || value === "weak" || value === "missing" ? value : "missing";
}

function toRewriteEvidenceStatus(value: unknown): EvidenceStatus {
  return value === "direct_rewrite_allowed" || value === "needs_user_evidence" ? value : "needs_user_evidence";
}

export function adaptUploadedFileMeta(raw: RawUploadedFileMeta = {}): UploadedFileMeta {
  return {
    uploadedFileId: toStringValue(raw.uploaded_file_id, "demo-file"),
    storageKey: toStringValue(raw.storage_key),
    fileName: toStringValue(raw.file_name, "demo-file.txt"),
    mimeType: toStringValue(raw.mime_type, "text/plain"),
    sizeBytes: toNumberValue(raw.size_bytes),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptCurrentUser(raw: RawCurrentUser = {}): CurrentUser {
  const userId = toStringValue(raw.user_id ?? raw.id, DEFAULT_USER_ID);
  const workspaceId = toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID);
  const role = toUserRole(raw.role);
  return {
    id: userId,
    userId,
    workspaceId,
    email: toStringValue(raw.email, "demo@example.invalid"),
    displayName: toStringValue(raw.display_name, "Demo user"),
    role,
    isAdmin: toBooleanValue(raw.is_admin, role === "owner" || role === "admin"),
    defaultWorkspaceId: toStringValue(raw.default_workspace_id, workspaceId),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptAuthSession(raw: RawAuthSession = {}): AuthSession {
  const authenticated = toBooleanValue(raw.authenticated, Boolean(raw.user));
  return {
    authenticated,
    sessionId: toStringValue(raw.session_id),
    user: authenticated && raw.user ? adaptCurrentUser(raw.user) : null,
    selectedWorkspaceId: toStringValue(raw.selected_workspace_id, DEFAULT_WORKSPACE_ID),
    expiresAt: toStringValue(raw.expires_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptWorkspace(raw: RawWorkspace = {}): Workspace {
  return {
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    ownerUserId: toStringValue(raw.owner_user_id, DEFAULT_USER_ID),
    name: toStringValue(raw.name, "Demo workspace"),
    role: toUserRole(raw.role),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptWorkspaces(raw: unknown): Workspace[] {
  return Array.isArray(raw) ? raw.map((item) => adaptWorkspace(item as RawWorkspace)) : [];
}

export function adaptAuthResponse(raw: RawAuthResponse = {}): {
  session: AuthSession;
  user: CurrentUser | null;
  workspaces: Workspace[];
  authMode: string;
  validationErrors: string[];
} {
  return {
    session: adaptAuthSession(raw.session),
    user: raw.user ? adaptCurrentUser(raw.user) : null,
    workspaces: adaptWorkspaces(raw.workspaces),
    authMode: toStringValue(raw.auth_mode, "mock"),
    validationErrors: toStringArray(raw.validation_errors),
  };
}

export function adaptWorkspaceSelection(raw: RawWorkspaceSelectionResponse = {}): {
  selectedWorkspaceId: string;
  workspace: Workspace;
  selectionMode: string;
} {
  return {
    selectedWorkspaceId: toStringValue(raw.selected_workspace_id, DEFAULT_WORKSPACE_ID),
    workspace: adaptWorkspace(raw.workspace),
    selectionMode: toStringValue(raw.selection_mode, "mock"),
  };
}

export function adaptWorkspaceSummary(raw: RawWorkspaceSummary = {}): WorkspaceSummary {
  const role = toUserRole(raw.role);
  return {
    id: toStringValue(raw.id, DEFAULT_WORKSPACE_ID),
    name: toStringValue(raw.name, "Demo workspace"),
    role,
    isActive: toBooleanValue(raw.is_active, true),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptUserProfileSummary(raw: RawUserProfileSummary = {}): UserProfileSummary {
  const role = toUserRole(raw.role);
  return {
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    displayName: toStringValue(raw.display_name, "Demo user"),
    role,
    isAdmin: toBooleanValue(raw.is_admin, role === "owner" || role === "admin"),
    loginState: raw.login_state === "signed_in" ? "signed_in" : "anonymous",
    profileStatus: toStringValue(raw.profile_status, "Demo profile"),
    dataSource:
      raw.data_source === "user_input" || raw.data_source === "database" || raw.data_source === "session" ? raw.data_source : "mock",
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptResumeSummary(raw: RawResumeSummary = {}): ResumeSummary {
  return {
    id: toStringValue(raw.id, "resume-demo-empty"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    uploadedFile: raw.uploaded_file ? adaptUploadedFileMeta(raw.uploaded_file) : undefined,
    displayName: toStringValue(raw.display_name, "Demo resume"),
    status: raw.status === "ready" || raw.status === "needs_review" ? raw.status : "not_selected",
    evidenceCount: toNumberValue(raw.evidence_count),
    lastUpdatedLabel: toStringValue(raw.last_updated_label, "Waiting for input"),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptJobPreferenceSummary(raw: RawJobPreferenceSummary = {}): JobPreferenceSummary {
  return {
    targetRoles: toStringArray(raw.target_roles),
    targetIndustries: toStringArray(raw.target_industries),
    locationDisplayName: toStringValue(raw.location_display_name, "Demo city"),
    workModes: toStringArray(raw.work_modes),
    excludedSignals: toStringArray(raw.excluded_signals),
  };
}

function adaptRoleBoundaryNote(raw: RawRoleBoundaryNote = {}): RoleBoundaryNote {
  return {
    id: toStringValue(raw.id, "boundary-demo"),
    module: toModuleKey(raw.module),
    professionalLens: toProfessionalLens(raw.professional_lens),
    professionalRole: toProfessionalRole(raw.professional_role),
    allowedInputs: toStringArray(raw.allowed_inputs),
    blockedOutputs: toStringArray(raw.blocked_outputs),
    note: toStringValue(raw.note, "Demo role boundary"),
  };
}

export function adaptUsageEvent(raw: RawUsageEvent = {}): UsageEvent {
  return {
    id: toStringValue(raw.id, "usage-event-demo"),
    eventType: toUsageEventType(raw.event_type),
    module: toModuleKey(raw.module),
    occurredAt: toStringValue(raw.occurred_at, DEFAULT_TIMESTAMP),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

export function adaptDailyMetric(raw: RawDailyMetric = {}): DailyMetric {
  return {
    id: toStringValue(raw.id, "daily-metric-demo"),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    metricDate: toStringValue(raw.metric_date, DEFAULT_TIMESTAMP),
    activeUsers: toNumberValue(raw.active_users),
    usageEvents: toNumberValue(raw.usage_events),
  };
}

export function adaptAdminMetrics(raw: RawAdminMetrics = {}): AdminMetrics {
  return {
    usageEvents: Array.isArray(raw.usage_events) ? raw.usage_events.map(adaptUsageEvent) : [],
    dailyMetrics: Array.isArray(raw.daily_metrics) ? raw.daily_metrics.map(adaptDailyMetric) : [],
  };
}

export function adaptSettingsSummary(raw: RawSettingsSummary = {}): SettingsSummary {
  return {
    user: adaptUserProfileSummary(raw.user),
    currentResume: adaptResumeSummary(raw.current_resume),
    preferences: adaptJobPreferenceSummary(raw.preferences),
    nextActions: toStringArray(raw.next_actions),
    roleBoundaryNotes: Array.isArray(raw.role_boundary_notes) ? raw.role_boundary_notes.map(adaptRoleBoundaryNote) : [],
    workspaces: Array.isArray(raw.workspaces) ? raw.workspaces.map(adaptWorkspaceSummary) : [],
    adminMetrics: raw.admin_metrics ? adaptAdminMetrics(raw.admin_metrics) : undefined,
  };
}

export function adaptJobPosting(raw: RawJobPosting = {}): JobPosting {
  return {
    id: toStringValue(raw.id, "job-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    title: toStringValue(raw.title, "Demo job"),
    companyDisplayName: toStringValue(raw.company_display_name, "Demo company"),
    locationDisplayName: toStringValue(raw.location_display_name, "Demo city"),
    workMode: toStringValue(raw.work_mode, "Hybrid"),
    seniority: toStringValue(raw.seniority, "Entry"),
    coreSkills: toStringArray(raw.core_skills),
    sourceType:
      raw.source_type === "manual" || raw.source_type === "batch_import" || raw.source_type === "browser_capture"
        ? raw.source_type
        : "demo",
    jdText: typeof raw.jd_text === "string" ? raw.jd_text : undefined,
    industry: typeof raw.industry === "string" ? raw.industry : undefined,
    uploadedFile: raw.uploaded_file ? adaptUploadedFileMeta(raw.uploaded_file) : undefined,
    createdAt: toStringValue(raw.created_at, DEFAULT_TIMESTAMP),
    updatedAt: toStringValue(raw.updated_at, DEFAULT_TIMESTAMP),
  };
}

function adaptProfessionalJudgement(raw: RawProfessionalJudgement = {}): ProfessionalJudgement {
  return {
    id: toStringValue(raw.id, "judgement-demo"),
    module: toModuleKey(raw.module),
    professionalLens: toProfessionalLens(raw.professional_lens),
    professionalRole: toProfessionalRole(raw.professional_role),
    conclusion: toStringValue(raw.conclusion, "Waiting for user input"),
    evidenceRefs: toStringArray(raw.evidence_refs),
    riskLevel: toRiskLevel(raw.risk_level),
    nextAction: toStringValue(raw.next_action, "Review the evidence boundary"),
  };
}

export function adaptJobAnalysisResult(raw: RawJobAnalysisResult = {}): JobAnalysisResult {
  return {
    id: toStringValue(raw.id, "jd-analysis-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    job: adaptJobPosting(raw.job),
    recommendationLevel: toRecommendationLevel(raw.recommendation_level),
    riskLevel: toRiskLevel(raw.risk_level),
    professionalLens: toProfessionalLens(raw.professional_lens),
    judgement: adaptProfessionalJudgement(raw.judgement),
    responsibilities: toStringArray(raw.responsibilities),
    hardRequirements: toStringArray(raw.hard_requirements),
    keywords: toStringArray(raw.keywords),
    concerns: toStringArray(raw.concerns),
    nextAction: toStringValue(raw.next_action, "Review next action"),
  };
}

export function adaptBatchJobRankingResult(raw: RawBatchJobRankingResult = {}): BatchJobRankingResult {
  return {
    id: toStringValue(raw.id, "ranking-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    job: adaptJobPosting(raw.job),
    matchScore: toNumberValue(raw.match_score),
    recommendationLevel: toRecommendationLevel(raw.recommendation_level),
    riskLevel: toRiskLevel(raw.risk_level),
    priority: toPriorityLevel(raw.priority),
    professionalLens: toProfessionalLens(raw.professional_lens),
    reasons: toStringArray(raw.reasons),
    concerns: toStringArray(raw.concerns),
    nextAction: toStringValue(raw.next_action, "Review this job"),
  };
}

export function adaptBatchJobRankingResults(raw: unknown): BatchJobRankingResult[] {
  return Array.isArray(raw) ? raw.map((item) => adaptBatchJobRankingResult(item as RawBatchJobRankingResult)) : [];
}

export function adaptJobTrendSummary(raw: RawJobTrendSummary = {}): JobTrendSummary {
  return {
    id: toStringValue(raw.id, "trend-demo"),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    title: toStringValue(raw.title, "Demo job trend"),
    professionalLens: "data_analyst",
    sampleSize: toNumberValue(raw.sample_size),
    topSkills: toStringArray(raw.top_skills),
    marketSignals: toStringArray(raw.market_signals),
    concerns: toStringArray(raw.concerns),
  };
}

function adaptResumeEvidenceItem(raw: RawResumeEvidenceItem = {}): ResumeEvidenceItem {
  return {
    id: toStringValue(raw.id, "evidence-demo"),
    section: toStringValue(raw.section, "Pending section"),
    sourceExcerpt: toStringValue(raw.source_excerpt, "Waiting for user input"),
    supportedSkills: toStringArray(raw.supported_skills),
    evidenceStatus: toEvidenceSupportStatus(raw.evidence_status),
    confidence: toNumberValue(raw.confidence),
  };
}

export function adaptResumeParseResponse(raw: RawResumeParseResponse = {}): ResumeParseResponse {
  const profile = raw.profile ?? {};
  return {
    profile: {
      basicInfo: toRecordString(profile.basic_info),
      education: toStringArray(profile.education),
      workExperience: toStringArray(profile.work_experience),
      projects: toStringArray(profile.projects),
      skills: toStringArray(profile.skills),
      certificates: toStringArray(profile.certificates),
      languages: toStringArray(profile.languages),
      rawText: toStringValue(profile.raw_text),
    },
    parsingNotes: toStringArray(raw.parsing_notes),
  };
}

export function adaptResumeMatchResult(raw: RawResumeMatchResult = {}): ResumeMatchResult {
  return {
    id: toStringValue(raw.id, "resume-match-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    targetJob: adaptJobPosting(raw.target_job),
    matchScore: toNumberValue(raw.match_score),
    recommendationLevel: toRecommendationLevel(raw.recommendation_level),
    riskLevel: toRiskLevel(raw.risk_level),
    professionalLens: "senior_hr",
    strengths: toStringArray(raw.strengths),
    gaps: toStringArray(raw.gaps),
    evidence: Array.isArray(raw.evidence) ? raw.evidence.map(adaptResumeEvidenceItem) : [],
    nextAction: toStringValue(raw.next_action, "Review resume evidence"),
  };
}

export function adaptResumeMatchResponse(raw: RawResumeMatchResponse = {}): ResumeMatchResponse {
  return {
    overallScore: toNumberValue(raw.overall_score),
    requirementFitScore: toNumberValue(raw.requirement_fit_score),
    skillFitScore: toNumberValue(raw.skill_fit_score),
    experienceRelevanceScore: toNumberValue(raw.experience_relevance_score),
    achievementEvidenceScore: toNumberValue(raw.achievement_evidence_score),
    resumeQualityScore: toNumberValue(raw.resume_quality_score),
    keywordMatchScore: toNumberValue(raw.keyword_match_score),
    riskLevel: toRiskLevel(raw.risk_level),
    hrDecision: toHrDecision(raw.hr_decision),
    hrSummary: toStringValue(raw.hr_summary, "Waiting for analysis"),
    strengths: toStringArray(raw.strengths),
    gaps: toStringArray(raw.gaps),
    risks: toStringArray(raw.risks),
    interviewRecommendation: toStringValue(raw.interview_recommendation, "Waiting for analysis"),
    likelyInterviewQuestions: toStringArray(raw.likely_interview_questions),
    resumeRewriteSuggestions: toStringArray(raw.resume_rewrite_suggestions),
    recommendedActions: toStringArray(raw.recommended_actions),
  };
}

export function adaptResumeRewriteSuggestion(raw: RawResumeRewriteSuggestion = {}): ResumeRewriteSuggestion {
  return {
    id: toStringValue(raw.id, "rewrite-demo"),
    targetSection: toStringValue(raw.target_section, "Pending section"),
    issue: toStringValue(raw.issue, "Waiting for user input"),
    suggestion: toStringValue(raw.suggestion, "Waiting for user input"),
    rewrittenExample: toStringValue(raw.rewritten_example, "Waiting for user input"),
    reason: toStringValue(raw.reason, "Only rewrite from user-provided evidence"),
    evidenceStatus: toRewriteEvidenceStatus(raw.evidence_status),
    allowedToApply: toBooleanValue(raw.allowed_to_apply),
    userInputRequired: toBooleanValue(raw.user_input_required, true),
    professionalLens: "resume_consultant",
    priority: toPriorityLevel(raw.priority),
  };
}

export function adaptResumeRewriteSuggestions(raw: unknown): ResumeRewriteSuggestion[] {
  return Array.isArray(raw) ? raw.map((item) => adaptResumeRewriteSuggestion(item as RawResumeRewriteSuggestion)) : [];
}

export function adaptGapAnalysisItem(raw: RawGapAnalysisItem = {}): GapAnalysisItem {
  return {
    id: toStringValue(raw.id, "gap-demo"),
    category: toStringValue(raw.category, "Evidence"),
    gap: toStringValue(raw.gap, "Waiting for user input"),
    impact: toStringValue(raw.impact, "Waiting for user input"),
    suggestedAction: toStringValue(raw.suggested_action, "Add verified evidence"),
    priority: toPriorityLevel(raw.priority),
    evidenceStatus: toEvidenceSupportStatus(raw.evidence_status),
  };
}

export function adaptGapAnalysisItems(raw: unknown): GapAnalysisItem[] {
  return Array.isArray(raw) ? raw.map((item) => adaptGapAnalysisItem(item as RawGapAnalysisItem)) : [];
}

export function adaptOpportunityDecisionResult(raw: RawOpportunityDecisionResult = {}): OpportunityDecisionResult {
  return {
    id: toStringValue(raw.id, "decision-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    jobTitle: toStringValue(raw.job_title, "Demo job"),
    recommendationLevel: toRecommendationLevel(raw.recommendation_level),
    priority: toPriorityLevel(raw.priority),
    estimatedReadiness: toNumberValue(raw.estimated_readiness),
    keyReasons: toStringArray(raw.key_reasons),
    keyRisks: toStringArray(raw.key_risks),
    nextActions: toStringArray(raw.next_actions),
    professionalLens: toProfessionalLens(raw.professional_lens),
    professionalRole: raw.professional_role === "application_strategy_advisor" ? raw.professional_role : undefined,
  };
}

export function adaptOpportunityDecisionResults(raw: unknown): OpportunityDecisionResult[] {
  return Array.isArray(raw) ? raw.map((item) => adaptOpportunityDecisionResult(item as RawOpportunityDecisionResult)) : [];
}

export function adaptApplicationPipelineItem(raw: RawApplicationPipelineItem = {}): ApplicationPipelineItem {
  return {
    id: toStringValue(raw.id, "pipeline-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    jobTitle: toStringValue(raw.job_title, "Demo job"),
    companyDisplayName: toStringValue(raw.company_display_name, "Demo company"),
    status: toApplicationStatus(raw.status),
    recommendedPriority: toPriorityLevel(raw.recommended_priority),
    nextSuggestedAction: toStringValue(raw.next_suggested_action, "Waiting for user confirmation"),
    userConfirmed: toBooleanValue(raw.user_confirmed),
  };
}

export function adaptApplicationPipeline(raw: unknown): ApplicationPipelineItem[] {
  return Array.isArray(raw) ? raw.map((item) => adaptApplicationPipelineItem(item as RawApplicationPipelineItem)) : [];
}

export function adaptInterviewReportSummary(raw: RawInterviewReportSummary = {}): InterviewReportSummary {
  return {
    id: toStringValue(raw.id, "interview-demo"),
    userId: toStringValue(raw.user_id, DEFAULT_USER_ID),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    title: toStringValue(raw.title, "Demo interview summary"),
    professionalLens: "data_analyst",
    readinessScore: toNumberValue(raw.readiness_score),
    keyQuestions: toStringArray(raw.key_questions),
    risks: toStringArray(raw.risks),
    nextActions: toStringArray(raw.next_actions),
  };
}

export function adaptInterviewReports(raw: unknown): InterviewReportSummary[] {
  return Array.isArray(raw) ? raw.map((item) => adaptInterviewReportSummary(item as RawInterviewReportSummary)) : [];
}

export function adaptDashboardMetric(raw: RawDashboardMetric = {}): DashboardMetric {
  return {
    id: toStringValue(raw.id, "metric-demo"),
    workspaceId: toStringValue(raw.workspace_id, DEFAULT_WORKSPACE_ID),
    label: toStringValue(raw.label, "Metric"),
    value: typeof raw.value === "number" || typeof raw.value === "string" ? raw.value : 0,
    trendLabel: toStringValue(raw.trend_label, "Waiting for data"),
    professionalLens: "data_analyst",
  };
}

export function adaptDashboardMetrics(raw: unknown): DashboardMetric[] {
  return Array.isArray(raw) ? raw.map((item) => adaptDashboardMetric(item as RawDashboardMetric)) : [];
}

export function adaptJdAnalyzeResponse(raw: RawJobAnalysisResult = {}): JDAnalyzeResponse {
  const result = adaptJobAnalysisResult(raw);
  return {
    analysis: {
      title: result.job.title,
      company: result.job.companyDisplayName,
      location: result.job.locationDisplayName,
      seniority: result.job.seniority,
      responsibilities: result.responsibilities,
      requiredSkills: result.hardRequirements,
      preferredSkills: result.job.coreSkills,
      experienceRequirements: [],
      educationRequirements: [],
      keywords: result.keywords,
    },
    analysisNotes: result.concerns,
  };
}

export function adaptBootstrapUser(raw: RawBootstrapResponse = {}, workspaces: WorkspaceConfig[]): UserProfile {
  const user = adaptUserProfileSummary(raw.settings_summary?.user ?? raw.me);
  return {
    name: user.displayName,
    resumeName: adaptResumeSummary(raw.settings_summary?.current_resume).displayName,
    target: adaptJobPreferenceSummary(raw.settings_summary?.preferences).targetRoles[0] ?? "Demo target",
    location: adaptJobPreferenceSummary(raw.settings_summary?.preferences).locationDisplayName,
    stage: workspaces.length ? workspaces[0].status : "Demo mode",
  };
}
