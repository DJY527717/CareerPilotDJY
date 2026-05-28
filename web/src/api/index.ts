import {
  adaptApplicationPipeline,
  adaptAuthResponse,
  adaptAuthSession,
  adaptBatchJobRankingResults,
  adaptBootstrapUser,
  adaptCurrentUser,
  adaptDashboardMetrics,
  adaptGapAnalysisItems,
  adaptInterviewReports,
  adaptJdAnalyzeResponse,
  adaptJobAnalysisResult,
  adaptJobTrendSummary,
  adaptOpportunityDecisionResults,
  adaptResumeMatchResponse,
  adaptResumeParseResponse,
  adaptResumeRewriteSuggestions,
  adaptSettingsSummary,
  adaptWorkspaceSelection,
  adaptWorkspaces,
} from "./adapters";
import {
  rawApplicationPipeline,
  rawAuthSession,
  rawBatchJobRankingResults,
  rawBootstrapResponse,
  rawDashboardMetrics,
  rawGapAnalysisItems,
  rawInterviewReports,
  rawJobAnalysisResult,
  rawJobTrendSummary,
  rawOpportunityDecisionResults,
  rawResumeMatchResponse,
  rawResumeParseResponse,
  rawResumeRewriteSuggestions,
  rawSettingsSummary,
  rawWorkspaces,
} from "./adapterFixtures";
import type {
  ApplicationPipelineItem,
  AuthSession,
  BatchJobRankingResult,
  CurrentUser,
  DashboardMetric,
  GapAnalysisItem,
  InterviewReportSummary,
  JDAnalyzeResponse,
  JobAnalysisResult,
  JobCard,
  JobTrendSummary,
  OpportunityDecisionResult,
  ResumeMatchResponse,
  ResumeParseResponse,
  ResumeRewriteSuggestion,
  SettingsSummary,
  UserProfile,
  Workspace,
  WorkspaceConfig,
} from "../types";

export type * from "./contracts";
export * from "./adapters";

export type BootstrapData = {
  user: UserProfile;
  workspaces: WorkspaceConfig[];
  jobs: JobCard[];
};

type ApiMode = "offline" | "live";

const apiMode: ApiMode = "offline";

const fallbackWorkspaces: WorkspaceConfig[] = [
  {
    key: "settings",
    label: "Settings",
    title: "Settings and profile",
    subtitle: "Maintain profile, resume context, and job preferences.",
    status: "Demo",
    modes: [
      { key: "settings-profile", label: "Profile" },
      { key: "settings-resume", label: "Resume" },
      { key: "settings-preferences", label: "Preferences" },
    ],
  },
  {
    key: "jd",
    label: "JD analysis",
    title: "JD analysis",
    subtitle: "Review job requirements, keywords, and screening signals.",
    status: "Demo",
    modes: [
      { key: "jd-single", label: "Single JD" },
      { key: "jd-batch", label: "Batch screen" },
      { key: "jd-monitor", label: "Trends" },
    ],
  },
  {
    key: "resume",
    label: "Resume match",
    title: "Resume parsing and JD match",
    subtitle: "Check resume evidence, role fit, risks, and next actions.",
    status: "Demo",
    modes: [
      { key: "resume-match", label: "Match" },
      { key: "resume-rewrite", label: "Rewrite" },
      { key: "resume-gap", label: "Gaps" },
    ],
  },
  {
    key: "decision",
    label: "Application strategy",
    title: "Application strategy",
    subtitle: "Compare opportunity quality and user-confirmed application actions.",
    status: "Demo",
    modes: [
      { key: "decision-offer", label: "Evaluate" },
      { key: "decision-internship", label: "Compare" },
      { key: "decision-pipeline", label: "Pipeline" },
    ],
  },
  {
    key: "report",
    label: "Reports",
    title: "Interview reports",
    subtitle: "Review interview notes, risks, and summary metrics.",
    status: "Demo",
    modes: [
      { key: "report-interview", label: "Interview" },
      { key: "report-dashboard", label: "Dashboard" },
    ],
  },
];

function toJobCards(results: BatchJobRankingResult[]): JobCard[] {
  return results.slice(0, 3).map((item) => ({
    id: item.job.id,
    title: item.job.title,
    company: item.job.companyDisplayName,
    location: item.job.locationDisplayName,
    track: item.job.industry ?? "Demo track",
    matchScore: item.matchScore,
    salary: "Not configured",
    highlights: item.job.coreSkills,
  }));
}

export const apiCapabilities = {
  mode: apiMode,
  liveClientReserved: true,
};

export const offlineData = {
  authSession: adaptAuthSession(rawAuthSession),
  currentUser: adaptCurrentUser(rawAuthSession.user ?? {}),
  workspaces: adaptWorkspaces(rawWorkspaces),
  settingsSummary: adaptSettingsSummary(rawSettingsSummary),
  jobAnalysis: adaptJobAnalysisResult(rawJobAnalysisResult),
  batchJobRankings: adaptBatchJobRankingResults(rawBatchJobRankingResults),
  jobTrend: adaptJobTrendSummary(rawJobTrendSummary),
  resumeParse: adaptResumeParseResponse(rawResumeParseResponse),
  resumeMatch: adaptResumeMatchResponse(rawResumeMatchResponse),
  resumeRewriteSuggestions: adaptResumeRewriteSuggestions(rawResumeRewriteSuggestions),
  gapAnalysisItems: adaptGapAnalysisItems(rawGapAnalysisItems),
  opportunityDecisions: adaptOpportunityDecisionResults(rawOpportunityDecisionResults),
  applicationPipeline: adaptApplicationPipeline(rawApplicationPipeline),
  interviewReports: adaptInterviewReports(rawInterviewReports),
  dashboardMetrics: adaptDashboardMetrics(rawDashboardMetrics),
};

export const emptyResumeParse = adaptResumeParseResponse({
  profile: {
    basic_info: {
      candidateName: "Anonymous demo profile",
      fileName: "No resume selected",
      targetRole: "Demo target role",
      contact: "Not shown in demo output",
    },
    education: ["Waiting for user input"],
    work_experience: ["Waiting for user input"],
    projects: [],
    skills: [],
    certificates: [],
    languages: [],
    raw_text: "",
  },
  parsing_notes: ["Paste resume text to parse education, experience, projects, skills, and evidence boundaries."],
});

export const emptyMatchResult = adaptResumeMatchResponse({
  hr_summary: "Waiting for analysis. Paste resume and JD text to generate an offline demo result.",
  risk_level: "medium",
  hr_decision: "maybe",
  strengths: ["No resume uploaded"],
  gaps: ["No target JD configured"],
  risks: ["Insufficient material to judge fit."],
  interview_recommendation: "Waiting for analysis.",
  likely_interview_questions: ["Questions will appear after analysis."],
  resume_rewrite_suggestions: ["Rewrite suggestions will appear after analysis."],
  recommended_actions: ["Paste resume and target JD, then start analysis."],
});

export const sampleResumeText = [
  "Anonymous demo profile",
  "Target role: demo analyst role",
  "Project evidence: supported dashboard maintenance, metric review, and analysis notes.",
  "Skills: SQL, dashboarding, business analysis, collaboration.",
  "Outcome evidence must be provided by the user before production use.",
].join("\n");

export const sampleJdText = [
  "Title: Demo analyst role",
  "Company: Demo company",
  "Responsibilities: dashboard maintenance, metric review, and business analysis.",
  "Requirements: SQL, structured communication, and cross-functional collaboration.",
].join("\n");

export const fallbackBootstrap: BootstrapData = {
  user: adaptBootstrapUser(rawBootstrapResponse, fallbackWorkspaces),
  workspaces: fallbackWorkspaces,
  jobs: toJobCards(offlineData.batchJobRankings),
};

export async function fetchBootstrap(): Promise<BootstrapData> {
  return fallbackBootstrap;
}

export async function fetchAuthSession(): Promise<AuthSession> {
  return offlineData.authSession;
}

export async function fetchCurrentUser(): Promise<CurrentUser | null> {
  return offlineData.currentUser;
}

export async function register(input: { email: string; password: string; displayName: string }) {
  return adaptAuthResponse({
    session: {
      ...rawAuthSession,
      user: {
        ...(rawAuthSession.user ?? {}),
        email: input.email || "demo@example.invalid",
        display_name: input.displayName || "Demo user",
        role: "member",
        is_admin: false,
      },
    },
    user: {
      ...(rawAuthSession.user ?? {}),
      email: input.email || "demo@example.invalid",
      display_name: input.displayName || "Demo user",
      role: "member",
      is_admin: false,
    },
    workspaces: rawWorkspaces,
    auth_mode: "mock",
    validation_errors: input.password.length >= 8 ? [] : ["password must be at least 8 characters in the mock contract"],
  });
}

export async function login(input: { email: string; password: string }) {
  return adaptAuthResponse({
    session: {
      ...rawAuthSession,
      user: {
        ...(rawAuthSession.user ?? {}),
        email: input.email || "demo@example.invalid",
      },
      authenticated: Boolean(input.password),
    },
    user: {
      ...(rawAuthSession.user ?? {}),
      email: input.email || "demo@example.invalid",
    },
    workspaces: rawWorkspaces,
    auth_mode: "mock",
    validation_errors: input.password ? [] : ["password is required in the mock contract"],
  });
}

export async function logout() {
  return adaptAuthResponse({
    session: {
      authenticated: false,
      session_id: "",
      user: null,
      selected_workspace_id: "",
      expires_at: "",
    },
    user: null,
    workspaces: [],
    auth_mode: "mock",
  });
}

export async function fetchWorkspaces(): Promise<Workspace[]> {
  return offlineData.workspaces;
}

export async function selectWorkspace(workspaceId: string) {
  const workspace = rawWorkspaces.find((item) => item.workspace_id === workspaceId) ?? rawWorkspaces[0];
  return adaptWorkspaceSelection({
    selected_workspace_id: workspace?.workspace_id ?? "demo-workspace",
    workspace,
    selection_mode: "mock",
  });
}

export async function fetchSettingsSummary(): Promise<SettingsSummary> {
  return offlineData.settingsSummary;
}

export async function fetchRankedJobs(): Promise<BatchJobRankingResult[]> {
  return offlineData.batchJobRankings;
}

export async function fetchJdAnalysis(): Promise<JobAnalysisResult> {
  return offlineData.jobAnalysis;
}

export async function fetchJobTrend(): Promise<JobTrendSummary> {
  return offlineData.jobTrend;
}

export async function fetchResumeRewriteSuggestions(): Promise<ResumeRewriteSuggestion[]> {
  return offlineData.resumeRewriteSuggestions;
}

export async function fetchGapAnalysisItems(): Promise<GapAnalysisItem[]> {
  return offlineData.gapAnalysisItems;
}

export async function fetchOpportunityDecisions(): Promise<OpportunityDecisionResult[]> {
  return offlineData.opportunityDecisions;
}

export async function fetchApplicationPipeline(): Promise<ApplicationPipelineItem[]> {
  return offlineData.applicationPipeline;
}

export async function fetchInterviewReports(): Promise<InterviewReportSummary[]> {
  return offlineData.interviewReports;
}

export async function fetchDashboardMetrics(): Promise<DashboardMetric[]> {
  return offlineData.dashboardMetrics;
}

export async function parseResume(resumeText: string, fileName?: string): Promise<ResumeParseResponse> {
  if (!resumeText.trim()) {
    return adaptResumeParseResponse({
      profile: {
        basic_info: {
          candidateName: "Anonymous demo profile",
          fileName: fileName || "demo-resume.txt",
          targetRole: "Demo target role",
          contact: "Not shown in demo output",
        },
        raw_text: "",
      },
      parsing_notes: ["Waiting for resume input."],
    });
  }

  return adaptResumeParseResponse({
    ...rawResumeParseResponse,
    profile: {
      ...rawResumeParseResponse.profile,
      basic_info: {
        ...(rawResumeParseResponse.profile?.basic_info ?? {}),
        fileName: fileName || "demo-resume.txt",
      },
      raw_text: resumeText,
    },
  });
}

export async function analyzeJd(jdText: string): Promise<JDAnalyzeResponse> {
  if (!jdText.trim()) {
    return adaptJdAnalyzeResponse({
      job: {
        title: "Demo job",
        company_display_name: "Demo company",
        location_display_name: "Demo city",
        seniority: "Pending",
      },
      responsibilities: [],
      hard_requirements: [],
      keywords: [],
      concerns: ["Waiting for JD input."],
    });
  }

  return adaptJdAnalyzeResponse(rawJobAnalysisResult);
}

export async function screenBatchJds(): Promise<BatchJobRankingResult[]> {
  return offlineData.batchJobRankings;
}

export async function matchResume(input: { resumeText: string; jdText: string }): Promise<ResumeMatchResponse> {
  if (!input.resumeText.trim() || !input.jdText.trim()) {
    return adaptResumeMatchResponse({
      hr_summary: "Waiting for resume and JD input.",
      risk_level: "medium",
      hr_decision: "maybe",
    });
  }

  return offlineData.resumeMatch;
}

export async function evaluateDecision(): Promise<OpportunityDecisionResult[]> {
  return offlineData.opportunityDecisions;
}

export async function fetchInterviewSummary(): Promise<InterviewReportSummary[]> {
  return offlineData.interviewReports;
}
