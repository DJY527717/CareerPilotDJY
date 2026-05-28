// API payloads may arrive as snake_case from careerpilot_api.
// Web presentation state uses camelCase where practical; add adapter functions
// at API boundaries instead of changing backend schemas in place.

export type ModuleKey = "settings" | "jd" | "resume" | "decision" | "report";
export type WorkspaceKey = ModuleKey;

export type ProfessionalLens = "senior_hr" | "headhunter" | "resume_consultant" | "data_analyst";

export type ProfessionalRoleId =
  | ProfessionalLens
  | "application_strategy_advisor"
  | "interview_review_advisor"
  | "report_analyst";

export type RecommendationLevel = "strongly_recommend" | "recommend" | "cautious" | "not_recommend";
export type LegacyRecommendationLevel =
  | "priority_apply"
  | "apply_after_rewrite"
  | "cautious_apply"
  | "backup"
  | "not_recommended";

export type RiskLevel = "low" | "medium" | "high";
export type PriorityLevel = "P0" | "P1" | "P2";

export type UserRole = "owner" | "admin" | "member" | "viewer";
export type AuthRole = UserRole;
export type UsageEventType =
  | "bootstrap_viewed"
  | "jd_analyzed"
  | "jd_batch_screened"
  | "resume_parsed"
  | "resume_matched"
  | "decision_evaluated"
  | "report_viewed";

export interface EntityMeta {
  userId: string;
  workspaceId: string;
  createdAt: string;
  updatedAt: string;
}

export interface CurrentUser extends EntityMeta {
  id: string;
  email: string;
  displayName: string;
  role: AuthRole;
  isAdmin: boolean;
  defaultWorkspaceId: string;
}

export interface AuthSession {
  authenticated: boolean;
  sessionId: string;
  user: CurrentUser | null;
  selectedWorkspaceId: string;
  expiresAt: string;
}

export interface Workspace {
  workspaceId: string;
  ownerUserId: string;
  name: string;
  role: AuthRole;
  createdAt: string;
  updatedAt: string;
}

export interface WorkspaceSummary {
  id: string;
  name: string;
  role: UserRole;
  isActive: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface UploadedFileMeta extends EntityMeta {
  uploadedFileId: string;
  storageKey: string;
  fileName: string;
  mimeType: string;
  sizeBytes: number;
}

export interface UsageEvent extends EntityMeta {
  id: string;
  eventType: UsageEventType;
  module: ModuleKey;
  occurredAt: string;
}

export interface DailyMetric {
  id: string;
  workspaceId: string;
  metricDate: string;
  activeUsers: number;
  usageEvents: number;
}

export interface AdminMetrics {
  usageEvents: UsageEvent[];
  dailyMetrics: DailyMetric[];
}

export type ModeKey =
  | "settings-profile"
  | "settings-resume"
  | "settings-preferences"
  | "jd-single"
  | "jd-batch"
  | "jd-monitor"
  | "resume-match"
  | "resume-rewrite"
  | "resume-gap"
  | "decision-offer"
  | "decision-internship"
  | "decision-pipeline"
  | "report-interview"
  | "report-dashboard";

export interface WorkspaceConfig {
  key: WorkspaceKey;
  label: string;
  title: string;
  subtitle: string;
  status: string;
  modes: Array<{
    key: ModeKey;
    label: string;
  }>;
}

export interface UserProfileSummary {
  userId?: string;
  workspaceId?: string;
  displayName: string;
  role?: UserRole;
  isAdmin?: boolean;
  loginState: "anonymous" | "signed_in";
  profileStatus: string;
  dataSource: "mock" | "user_input" | "database" | "session";
  createdAt?: string;
  updatedAt?: string;
}

export interface ResumeSummary {
  id: string;
  userId?: string;
  workspaceId?: string;
  uploadedFile?: UploadedFileMeta;
  displayName: string;
  status: "not_selected" | "ready" | "needs_review";
  evidenceCount: number;
  lastUpdatedLabel: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface JobPreferenceSummary {
  targetRoles: string[];
  targetIndustries: string[];
  locationDisplayName: string;
  workModes: string[];
  excludedSignals: string[];
}

export interface SettingsSummary {
  user: UserProfileSummary;
  currentResume: ResumeSummary;
  preferences: JobPreferenceSummary;
  nextActions: string[];
  roleBoundaryNotes: RoleBoundaryNote[];
  workspaces?: WorkspaceSummary[];
  adminMetrics?: AdminMetrics;
}

export interface JobPosting {
  id: string;
  userId?: string;
  workspaceId?: string;
  title: string;
  companyDisplayName: string;
  locationDisplayName: string;
  workMode: string;
  seniority: string;
  coreSkills: string[];
  sourceType: "manual" | "batch_import" | "browser_capture" | "demo";
  jdText?: string;
  industry?: string;
  uploadedFile?: UploadedFileMeta;
  createdAt?: string;
  updatedAt?: string;
}

export interface ProfessionalJudgement {
  id: string;
  module: ModuleKey;
  professionalLens: ProfessionalLens;
  professionalRole?: ProfessionalRoleId;
  conclusion: string;
  evidenceRefs: string[];
  riskLevel: RiskLevel;
  nextAction: string;
}

export interface RoleBoundaryNote {
  id: string;
  module: ModuleKey;
  professionalLens: ProfessionalLens;
  professionalRole?: ProfessionalRoleId;
  allowedInputs: string[];
  blockedOutputs: string[];
  note: string;
}

export interface JobAnalysisResult {
  id: string;
  userId?: string;
  workspaceId?: string;
  job: JobPosting;
  recommendationLevel: RecommendationLevel;
  riskLevel: RiskLevel;
  professionalLens: ProfessionalLens;
  judgement: ProfessionalJudgement;
  responsibilities: string[];
  hardRequirements: string[];
  keywords: string[];
  concerns: string[];
  nextAction: string;
}

export interface BatchJobRankingResult {
  id: string;
  userId?: string;
  workspaceId?: string;
  job: JobPosting;
  matchScore: number;
  recommendationLevel: RecommendationLevel;
  riskLevel: RiskLevel;
  priority: PriorityLevel;
  professionalLens: ProfessionalLens;
  reasons: string[];
  concerns: string[];
  nextAction: string;
}

export interface JobTrendSummary {
  id: string;
  workspaceId?: string;
  title: string;
  professionalLens: "data_analyst";
  sampleSize: number;
  topSkills: string[];
  marketSignals: string[];
  concerns: string[];
}

export interface ResumeEvidenceItem {
  id: string;
  section: string;
  sourceExcerpt: string;
  supportedSkills: string[];
  evidenceStatus: "supported" | "weak" | "missing";
  confidence: number;
}

export interface ResumeMatchResult {
  id: string;
  userId?: string;
  workspaceId?: string;
  targetJob: JobPosting;
  matchScore: number;
  recommendationLevel: RecommendationLevel;
  riskLevel: RiskLevel;
  professionalLens: "senior_hr";
  strengths: string[];
  gaps: string[];
  evidence: ResumeEvidenceItem[];
  nextAction: string;
}

export type EvidenceStatus = "direct_rewrite_allowed" | "needs_user_evidence";

export interface ResumeRewriteSuggestion {
  id: string;
  targetSection: string;
  issue: string;
  suggestion: string;
  rewrittenExample: string;
  reason: string;
  evidenceStatus: EvidenceStatus;
  allowedToApply: boolean;
  userInputRequired: boolean;
  professionalLens: "resume_consultant";
  priority: PriorityLevel;
}

export interface GapAnalysisItem {
  id: string;
  category: string;
  gap: string;
  impact: string;
  suggestedAction: string;
  priority: PriorityLevel;
  evidenceStatus: "supported" | "weak" | "missing";
}

export interface OpportunityDecisionResult {
  id: string;
  userId?: string;
  workspaceId?: string;
  jobTitle: string;
  recommendationLevel: RecommendationLevel;
  priority: PriorityLevel;
  estimatedReadiness: number;
  keyReasons: string[];
  keyRisks: string[];
  nextActions: string[];
  professionalLens: ProfessionalLens;
  professionalRole?: "application_strategy_advisor";
}

export interface JobComparisonItem {
  id: string;
  jobTitle: string;
  opportunityValue: number;
  preparationCost: number;
  riskLevel: RiskLevel;
  recommendationLevel: RecommendationLevel;
  notes: string[];
}

export interface ApplicationPipelineItem {
  id: string;
  userId?: string;
  workspaceId?: string;
  jobTitle: string;
  companyDisplayName: string;
  status: ApplicationStatus;
  recommendedPriority: PriorityLevel;
  nextSuggestedAction: string;
  userConfirmed: boolean;
}

export interface InterviewReportSummary {
  id: string;
  userId?: string;
  workspaceId?: string;
  title: string;
  professionalLens: "data_analyst";
  readinessScore: number;
  keyQuestions: string[];
  risks: string[];
  nextActions: string[];
}

export interface DashboardMetric {
  id: string;
  workspaceId?: string;
  label: string;
  value: string | number;
  trendLabel: string;
  professionalLens: "data_analyst";
}

export interface UserProfile {
  name: string;
  resumeName: string;
  target: string;
  location: string;
  stage: string;
}

export interface JobCard {
  id: string;
  title: string;
  company: string;
  location: string;
  track: string;
  matchScore: number;
  salary: string;
  highlights: string[];
}

export type ApplicationPriority =
  | "today_priority"
  | "weekly_focus"
  | "after_resume_update"
  | "observe_only"
  | "skip";

export type PreparationStatus =
  | "ready"
  | "needs_rewrite"
  | "missing_evidence"
  | "preference_conflict"
  | "insufficient_jd_info";

export type ApplicationStatus = "not_applied" | "ready_to_apply" | "applied" | "followed_up" | "interviewing" | "closed";

export interface JobOpportunity {
  jobId: string;
  title: string;
  company: string;
  location: string;
  industry: string;
  jdText: string;
  source: string;
  capturedAt: string;
}

export interface ResumeEvidence {
  evidenceId: string;
  section: string;
  originalText: string;
  skills: string[];
  metrics: string[];
  confidence: number;
}

export interface MatchAssessment {
  jobId: string;
  overallScore: number;
  hardMatchScore: number;
  preferenceFitScore: number;
  evidenceScore: number;
  growthValueScore: number;
  riskScore: number;
  recommendationLevel: LegacyRecommendationLevel;
  recommendationReason: string;
  risks: string[];
  missingEvidence: string[];
  weakEvidence: string[];
  nextAction: string;
}

export interface RewriteSuggestion {
  targetJobId: string;
  originalText: string;
  rewrittenText: string;
  reason: string;
  jdKeywords: string[];
  strengthenedRequirement: string;
  evidenceNeeded: string[];
  needsUserEvidence: boolean;
  evidenceStatus: EvidenceStatus;
  /**
   * Compatibility marker for the existing persona integrity script and backend
   * RewriteSuggestion schema. Page components should use evidenceStatus.
   */
  evidence_status: EvidenceStatus;
  allowedToApply: boolean;
  userInputRequired: string[];
  confidence: number;
  recommendedJobsAfterRewrite: string[];
}

export interface ApplicationStrategy {
  jobId: string;
  priority: ApplicationPriority;
  applicationStatus: ApplicationStatus;
  preparationStatus: PreparationStatus;
  timing: string;
  requiredPreparation: string[];
  followUpAction: string;
  reason: string;
  plannedApplyDate?: string | null;
  followUpReminder?: string | null;
  interviewStage?: string | null;
  reviewResult?: string | null;
}

export interface ActionItem {
  id: string;
  title: string;
  detail: string;
  priority: "高" | "中" | "低";
}

export interface ResumeProfile {
  basicInfo: Record<string, string>;
  education: string[];
  workExperience: string[];
  projects: string[];
  skills: string[];
  certificates: string[];
  languages: string[];
  rawText: string;
}

export interface ResumeParseResponse {
  profile: ResumeProfile;
  parsingNotes: string[];
}

export interface JDAnalysis {
  title: string;
  company: string;
  location: string;
  seniority: string;
  responsibilities: string[];
  requiredSkills: string[];
  preferredSkills: string[];
  experienceRequirements: string[];
  educationRequirements: string[];
  keywords: string[];
}

export interface JDAnalyzeResponse {
  analysis: JDAnalysis;
  analysisNotes: string[];
}

export type HRDecision = "recommend_interview" | "maybe" | "not_recommended";

export interface ResumeMatchResponse {
  overallScore: number;
  requirementFitScore: number;
  skillFitScore: number;
  experienceRelevanceScore: number;
  achievementEvidenceScore: number;
  resumeQualityScore: number;
  keywordMatchScore: number;
  riskLevel: RiskLevel;
  hrDecision: HRDecision;
  hrSummary: string;
  strengths: string[];
  gaps: string[];
  risks: string[];
  interviewRecommendation: string;
  likelyInterviewQuestions: string[];
  resumeRewriteSuggestions: string[];
  recommendedActions: string[];
}
