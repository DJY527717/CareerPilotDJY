// Raw API contracts for CareerPilot.
// Backend, offline fixtures, and future database responses may use snake_case.
// React pages must consume camelCase view types after adapter conversion.

import type {
  ApplicationStatus,
  ModuleKey,
  PriorityLevel,
  ProfessionalLens,
  ProfessionalRoleId,
  RecommendationLevel,
  RiskLevel,
  UsageEventType,
  UserRole,
} from "../types";

export interface RawEntityMeta {
  user_id?: string;
  workspace_id?: string;
  created_at?: string;
  updated_at?: string;
}

export interface RawCurrentUser extends RawEntityMeta {
  id?: string;
  email?: string;
  display_name?: string;
  role?: UserRole;
  is_admin?: boolean;
  default_workspace_id?: string;
}

export interface RawAuthSession {
  authenticated?: boolean;
  session_id?: string;
  user?: RawCurrentUser | null;
  selected_workspace_id?: string;
  expires_at?: string;
}

export interface RawAuthResponse {
  session?: RawAuthSession;
  user?: RawCurrentUser | null;
  workspaces?: RawWorkspace[];
  auth_mode?: "mock" | "session" | "database";
  validation_errors?: string[];
}

export interface RawWorkspace {
  workspace_id?: string;
  owner_user_id?: string;
  name?: string;
  role?: UserRole;
  created_at?: string;
  updated_at?: string;
}

export interface RawWorkspaceSelectionResponse {
  selected_workspace_id?: string;
  workspace?: RawWorkspace;
  selection_mode?: "mock" | "session" | "database";
}

export interface RawWorkspaceSummary {
  id?: string;
  name?: string;
  role?: UserRole;
  is_active?: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface RawUploadedFileMeta extends RawEntityMeta {
  uploaded_file_id?: string;
  storage_key?: string;
  file_name?: string;
  mime_type?: string;
  size_bytes?: number;
}

export interface RawUsageEvent extends RawEntityMeta {
  id?: string;
  event_type?: UsageEventType;
  module?: ModuleKey;
  occurred_at?: string;
}

export interface RawDailyMetric {
  id?: string;
  workspace_id?: string;
  metric_date?: string;
  active_users?: number;
  usage_events?: number;
}

export interface RawAdminMetrics {
  usage_events?: RawUsageEvent[];
  daily_metrics?: RawDailyMetric[];
}

export interface RawUserProfileSummary extends RawEntityMeta {
  display_name?: string;
  role?: UserRole;
  is_admin?: boolean;
  login_state?: "anonymous" | "signed_in";
  profile_status?: string;
  data_source?: "mock" | "user_input" | "database" | "session";
}

export interface RawResumeSummary extends RawEntityMeta {
  id?: string;
  display_name?: string;
  status?: "not_selected" | "ready" | "needs_review";
  evidence_count?: number;
  last_updated_label?: string;
  uploaded_file?: RawUploadedFileMeta;
}

export interface RawJobPreferenceSummary {
  target_roles?: string[];
  target_industries?: string[];
  location_display_name?: string;
  work_modes?: string[];
  excluded_signals?: string[];
}

export interface RawRoleBoundaryNote {
  id?: string;
  module?: ModuleKey;
  professional_lens?: ProfessionalLens;
  professional_role?: ProfessionalRoleId;
  allowed_inputs?: string[];
  blocked_outputs?: string[];
  note?: string;
}

export interface RawSettingsSummary {
  user?: RawUserProfileSummary;
  current_resume?: RawResumeSummary;
  preferences?: RawJobPreferenceSummary;
  next_actions?: string[];
  role_boundary_notes?: RawRoleBoundaryNote[];
  workspaces?: RawWorkspaceSummary[];
  admin_metrics?: RawAdminMetrics;
}

export interface RawBootstrapResponse {
  me?: RawCurrentUser;
  workspaces?: RawWorkspaceSummary[];
  settings_summary?: RawSettingsSummary;
}

export interface RawJobPosting extends RawEntityMeta {
  id?: string;
  title?: string;
  company_display_name?: string;
  location_display_name?: string;
  work_mode?: string;
  seniority?: string;
  core_skills?: string[];
  source_type?: "manual" | "batch_import" | "browser_capture" | "demo";
  jd_text?: string;
  industry?: string;
  uploaded_file?: RawUploadedFileMeta;
}

export interface RawProfessionalJudgement {
  id?: string;
  module?: ModuleKey;
  professional_lens?: ProfessionalLens;
  professional_role?: ProfessionalRoleId;
  conclusion?: string;
  evidence_refs?: string[];
  risk_level?: RiskLevel;
  next_action?: string;
}

export interface RawJobAnalysisResult extends RawEntityMeta {
  id?: string;
  job?: RawJobPosting;
  recommendation_level?: RecommendationLevel;
  risk_level?: RiskLevel;
  professional_lens?: ProfessionalLens;
  judgement?: RawProfessionalJudgement;
  responsibilities?: string[];
  hard_requirements?: string[];
  keywords?: string[];
  concerns?: string[];
  next_action?: string;
}

export interface RawBatchJobRankingResult extends RawEntityMeta {
  id?: string;
  job?: RawJobPosting;
  match_score?: number;
  recommendation_level?: RecommendationLevel;
  risk_level?: RiskLevel;
  priority?: PriorityLevel;
  professional_lens?: ProfessionalLens;
  reasons?: string[];
  concerns?: string[];
  next_action?: string;
}

export interface RawJobTrendSummary {
  id?: string;
  workspace_id?: string;
  title?: string;
  professional_lens?: "data_analyst";
  sample_size?: number;
  top_skills?: string[];
  market_signals?: string[];
  concerns?: string[];
}

export interface RawResumeEvidenceItem {
  id?: string;
  section?: string;
  source_excerpt?: string;
  supported_skills?: string[];
  evidence_status?: "supported" | "weak" | "missing";
  confidence?: number;
}

export interface RawResumeParseResponse {
  profile?: {
    basic_info?: Record<string, string>;
    education?: string[];
    work_experience?: string[];
    projects?: string[];
    skills?: string[];
    certificates?: string[];
    languages?: string[];
    raw_text?: string;
  };
  parsing_notes?: string[];
  uploaded_file?: RawUploadedFileMeta;
}

export interface RawResumeMatchResult extends RawEntityMeta {
  id?: string;
  target_job?: RawJobPosting;
  match_score?: number;
  recommendation_level?: RecommendationLevel;
  risk_level?: RiskLevel;
  professional_lens?: "senior_hr";
  strengths?: string[];
  gaps?: string[];
  evidence?: RawResumeEvidenceItem[];
  next_action?: string;
}

export interface RawResumeMatchResponse {
  overall_score?: number;
  requirement_fit_score?: number;
  skill_fit_score?: number;
  experience_relevance_score?: number;
  achievement_evidence_score?: number;
  resume_quality_score?: number;
  keyword_match_score?: number;
  risk_level?: RiskLevel;
  hr_decision?: "recommend_interview" | "maybe" | "not_recommended";
  hr_summary?: string;
  strengths?: string[];
  gaps?: string[];
  risks?: string[];
  interview_recommendation?: string;
  likely_interview_questions?: string[];
  resume_rewrite_suggestions?: string[];
  recommended_actions?: string[];
}

export interface RawResumeRewriteSuggestion {
  id?: string;
  target_section?: string;
  issue?: string;
  suggestion?: string;
  rewritten_example?: string;
  reason?: string;
  evidence_status?: "direct_rewrite_allowed" | "needs_user_evidence";
  allowed_to_apply?: boolean;
  user_input_required?: boolean;
  professional_lens?: "resume_consultant";
  priority?: PriorityLevel;
}

export interface RawGapAnalysisItem {
  id?: string;
  category?: string;
  gap?: string;
  impact?: string;
  suggested_action?: string;
  priority?: PriorityLevel;
  evidence_status?: "supported" | "weak" | "missing";
}

export interface RawOpportunityDecisionResult extends RawEntityMeta {
  id?: string;
  job_title?: string;
  recommendation_level?: RecommendationLevel;
  priority?: PriorityLevel;
  estimated_readiness?: number;
  key_reasons?: string[];
  key_risks?: string[];
  next_actions?: string[];
  professional_lens?: ProfessionalLens;
  professional_role?: "application_strategy_advisor";
}

export interface RawApplicationPipelineItem extends RawEntityMeta {
  id?: string;
  job_title?: string;
  company_display_name?: string;
  status?: ApplicationStatus;
  recommended_priority?: PriorityLevel;
  next_suggested_action?: string;
  user_confirmed?: boolean;
}

export interface RawInterviewReportSummary extends RawEntityMeta {
  id?: string;
  title?: string;
  professional_lens?: "data_analyst";
  readiness_score?: number;
  key_questions?: string[];
  risks?: string[];
  next_actions?: string[];
}

export interface RawDashboardMetric {
  id?: string;
  workspace_id?: string;
  label?: string;
  value?: string | number;
  trend_label?: string;
  professional_lens?: "data_analyst";
}
