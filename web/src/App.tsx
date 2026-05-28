import { useEffect, useState } from "react";
import {
  fallbackBootstrap,
  fetchAuthSession,
  fetchBootstrap,
  fetchDashboardMetrics,
  fetchInterviewReports,
  fetchSettingsSummary,
  offlineData,
} from "./api";
import { ApplicationStrategyPage } from "./pages/decision/ApplicationStrategyPage";
import { BatchJDResultsPage } from "./pages/jd/BatchJDResultsPage";
import { ResumeMatchPage } from "./pages/resume/ResumeMatchPage";
import type {
  AuthSession,
  DashboardMetric,
  InterviewReportSummary,
  SettingsSummary,
  UserProfile,
  WorkspaceConfig,
  WorkspaceKey,
} from "./types";

function App() {
  const [user, setUser] = useState<UserProfile>(fallbackBootstrap.user);
  const [authSession, setAuthSession] = useState<AuthSession>(offlineData.authSession);
  const [workspaces, setWorkspaces] = useState<WorkspaceConfig[]>(fallbackBootstrap.workspaces);
  const [settingsSummary, setSettingsSummary] = useState<SettingsSummary>(offlineData.settingsSummary);
  const [interviewReports, setInterviewReports] = useState<InterviewReportSummary[]>(offlineData.interviewReports);
  const [dashboardMetrics, setDashboardMetrics] = useState<DashboardMetric[]>(offlineData.dashboardMetrics);
  const [activeWorkspace, setActiveWorkspace] = useState<WorkspaceKey>("resume");
  const [apiStatus, setApiStatus] = useState("Demo mode");

  useEffect(() => {
    let alive = true;
    fetchBootstrap()
      .then((bootstrap) => {
        if (!alive) return;
        setUser(bootstrap.user);
        setWorkspaces(bootstrap.workspaces.length ? bootstrap.workspaces : fallbackBootstrap.workspaces);
        setApiStatus("API facade ready");
      })
      .catch(() => {
        if (!alive) return;
        setUser(fallbackBootstrap.user);
        setWorkspaces(fallbackBootstrap.workspaces);
        setApiStatus("Demo mode");
      });
    void fetchAuthSession().then((session) => {
      if (alive) setAuthSession(session);
    });
    void fetchSettingsSummary().then((settings) => {
      if (alive) setSettingsSummary(settings);
    });
    void fetchInterviewReports().then((reports) => {
      if (alive) setInterviewReports(reports);
    });
    void fetchDashboardMetrics().then((metrics) => {
      if (alive) setDashboardMetrics(metrics);
    });
    return () => {
      alive = false;
    };
  }, []);

  const workspace = workspaces.find((item) => item.key === activeWorkspace) ?? workspaces[0];
  const sessionLabel = authSession.authenticated ? authSession.user?.displayName ?? "Demo user" : "Guest";
  const roleLabel = authSession.authenticated ? authSession.user?.role ?? "member" : "anonymous";

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">CP</div>
          <div>
            <strong>CareerPilot</strong>
            <span>Resume Match</span>
          </div>
        </div>
        <nav className="nav" aria-label="Workspace modules">
          {workspaces.map((item) => (
            <button
              className={item.key === activeWorkspace ? "nav-item active" : "nav-item"}
              key={item.key}
              onClick={() => setActiveWorkspace(item.key)}
              type="button"
            >
              <span>{item.label}</span>
              <small>{item.status}</small>
            </button>
          ))}
        </nav>
        <section className="profile-card">
          <p>Current session</p>
          <strong>{sessionLabel}</strong>
          <span>{roleLabel}</span>
          <span>{authSession.selectedWorkspaceId || "demo-workspace"}</span>
          {authSession.user?.isAdmin ? <span>Admin boundary reserved</span> : null}
        </section>
        <section className="profile-card">
          <p>Current profile</p>
          <strong>{user.name}</strong>
          <span>{user.resumeName}</span>
          <span>{user.target}</span>
        </section>
      </aside>

      <main className="main-shell">
        <header className="topbar">
          <div>
            <h1>{workspace?.title ?? "Resume and JD matching"}</h1>
            <p>{workspace?.subtitle ?? "Review resume evidence, job requirements, risk signals, and next actions."}</p>
          </div>
          <span className="status-chip">{apiStatus}</span>
        </header>

        {activeWorkspace === "decision" ? (
          <ApplicationStrategyPage />
        ) : activeWorkspace === "jd" ? (
          <BatchJDResultsPage onOpenStrategy={() => setActiveWorkspace("decision")} />
        ) : activeWorkspace === "resume" ? (
          <ResumeMatchPage />
        ) : activeWorkspace === "settings" ? (
          <SettingsOverview settings={settingsSummary} />
        ) : activeWorkspace === "report" ? (
          <ReportOverview metrics={dashboardMetrics} reports={interviewReports} />
        ) : (
          <section className="placeholder-panel">
            <h2>{workspace?.label}</h2>
            <p>Select a module from the sidebar to continue the offline demo workflow.</p>
          </section>
        )}
      </main>
    </div>
  );
}

function SettingsOverview({ settings }: { settings: SettingsSummary }) {
  return (
    <section className="placeholder-panel">
      <h2>Settings and profile</h2>
      <div className="result-grid">
        <article className="evidence-card preference_match">
          <header>
            <span>Profile summary</span>
            <strong>{settings.user.displayName}</strong>
          </header>
          <p>{settings.user.profileStatus}</p>
        </article>
        <article className="evidence-card resume_evidence">
          <header>
            <span>Current resume</span>
            <strong>{settings.currentResume.displayName}</strong>
          </header>
          <p>{settings.currentResume.lastUpdatedLabel}</p>
        </article>
        <article className="evidence-card jd_requirement">
          <header>
            <span>Job preference</span>
            <strong>{settings.preferences.targetRoles[0]}</strong>
          </header>
          <p>{settings.preferences.locationDisplayName}</p>
        </article>
      </div>
      <ul className="compact-list">
        {settings.nextActions.map((action) => (
          <li key={action}>
            <span>{action}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function ReportOverview({ metrics, reports }: { metrics: DashboardMetric[]; reports: InterviewReportSummary[] }) {
  const report = reports[0];
  return (
    <section className="placeholder-panel">
      <h2>Interview reports</h2>
      <p>Report views stay limited to review notes, risks, and aggregate metrics in this demo boundary.</p>
      {report && (
        <div className="result-grid">
          <article className="evidence-card jd_requirement">
            <header>
              <span>Interview summary</span>
              <strong>{report.title}</strong>
            </header>
            <p>Readiness {report.readinessScore}</p>
          </article>
          <article className="evidence-card risk_signal">
            <header>
              <span>Review risks</span>
              <strong>{report.risks.length}</strong>
            </header>
            <p>{report.risks[0]}</p>
          </article>
        </div>
      )}
      <section className="batch-metrics" aria-label="Report dashboard metrics">
        {metrics.map((metric) => (
          <article className="strategy-metric" key={metric.id}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
            <p>{metric.trendLabel}</p>
          </article>
        ))}
      </section>
    </section>
  );
}

export default App;
