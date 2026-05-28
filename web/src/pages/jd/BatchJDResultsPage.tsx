import { useState } from "react";
import { offlineData } from "../../api";
import { EmptyState } from "../../components/EmptyState";
import { EvidenceCard } from "../../components/EvidenceCard";
import { ResultSummary } from "../../components/ResultSummary";
import type { BatchJobRankingResult, ProfessionalLens, RecommendationLevel } from "../../types";

type ResultActionId = "view_evidence" | "prepare_resume" | "save_backup" | "mark_skip" | "enter_application_strategy";

const recommendationLabels: Record<RecommendationLevel, string> = {
  strongly_recommend: "Strong recommend",
  recommend: "Recommend",
  cautious: "Review carefully",
  not_recommend: "Not recommended",
};

const recommendationTone: Record<RecommendationLevel, string> = {
  strongly_recommend: "priority",
  recommend: "rewrite",
  cautious: "caution",
  not_recommend: "reject",
};

const lensLabels: Record<ProfessionalLens, string> = {
  senior_hr: "Senior HR lens",
  headhunter: "Headhunter lens",
  resume_consultant: "Resume consultant lens",
  data_analyst: "Data analyst lens",
};

const resultActionLabels: Record<ResultActionId, string> = {
  view_evidence: "View evidence",
  prepare_resume: "Prepare resume evidence",
  save_backup: "Save backup",
  mark_skip: "Mark skip",
  enter_application_strategy: "Open application strategy",
};

function countByLevel(items: BatchJobRankingResult[], levels: RecommendationLevel[]): number {
  return items.filter((item) => levels.includes(item.recommendationLevel)).length;
}

function primaryActionForRecommendation(level: RecommendationLevel): ResultActionId {
  if (level === "strongly_recommend" || level === "recommend") return "prepare_resume";
  if (level === "cautious") return "view_evidence";
  return "mark_skip";
}

function buildConclusion(items: BatchJobRankingResult[]): string {
  if (!items.length) return "No batch screening results are available.";
  const topCount = countByLevel(items, ["strongly_recommend", "recommend"]);
  return topCount ? `${topCount} job(s) are worth prioritizing in the demo pool.` : "No priority jobs in the current demo pool.";
}

function topConcerns(items: BatchJobRankingResult[], limit = 5): Array<{ label: string; count: number }> {
  const counts = new Map<string, number>();
  items.flatMap((item) => item.concerns).forEach((concern) => {
    const clean = concern.trim();
    if (!clean) return;
    counts.set(clean, (counts.get(clean) ?? 0) + 1);
  });
  return [...counts.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((left, right) => right.count - left.count)
    .slice(0, limit);
}

function JobRow({
  confirmedAction,
  evidenceOpen,
  onAction,
  result,
}: {
  confirmedAction?: ResultActionId;
  evidenceOpen: boolean;
  onAction: (jobId: string, actionId: ResultActionId) => void;
  result: BatchJobRankingResult;
}) {
  const primaryAction = primaryActionForRecommendation(result.recommendationLevel);
  return (
    <article className="batch-row">
      <div className="batch-role">
        <strong>{result.job.title}</strong>
        <span>{result.job.companyDisplayName}</span>
      </div>
      <span className={`level-pill ${recommendationTone[result.recommendationLevel]}`}>
        {recommendationLabels[result.recommendationLevel]}
      </span>
      <strong className="batch-score">{result.matchScore}</strong>
      <p>{result.reasons[0]}</p>
      <div className="job-action-stack">
        <button className="job-primary-action" onClick={() => onAction(result.id, primaryAction)} type="button">
          {resultActionLabels[primaryAction]}
        </button>
        <button className="job-secondary-action" onClick={() => onAction(result.id, "enter_application_strategy")} type="button">
          {resultActionLabels.enter_application_strategy}
        </button>
        {confirmedAction && <span>Confirmed: {resultActionLabels[confirmedAction]}</span>}
      </div>
      <details className="evidence-disclosure" open={evidenceOpen}>
        <summary>Review role boundary evidence</summary>
        <div className="batch-evidence">
          <EvidenceCard
            confidence={result.matchScore / 100}
            evidenceType="jd_requirement"
            interpretation={`${lensLabels[result.professionalLens]}: ${result.reasons[0] ?? "No reason provided."}`}
            sourceExcerpt={result.job.jdText ?? ""}
            sourceLabel="JD source"
            title="Job evidence"
          />
          <EvidenceCard
            confidence={result.riskLevel === "low" ? 0.78 : result.riskLevel === "medium" ? 0.56 : 0.34}
            evidenceType={result.riskLevel === "high" ? "risk_signal" : "preference_match"}
            interpretation={result.concerns[0] ?? "No high risk signal in the demo result."}
            sourceExcerpt={`${result.job.locationDisplayName} / ${result.job.workMode} / ${result.priority}`}
            sourceLabel="Job fields"
            title="Risk and priority"
          />
        </div>
      </details>
    </article>
  );
}

function CompactList({ items, emptyText }: { items: Array<{ label: string; count?: number }>; emptyText: string }) {
  if (!items.length) return <p className="empty-copy">{emptyText}</p>;
  return (
    <ul className="compact-list">
      {items.map((item) => (
        <li key={item.label}>
          <span>{item.label}</span>
          {item.count ? <strong>{item.count}</strong> : null}
        </li>
      ))}
    </ul>
  );
}

export function BatchJDResultsPage({ onOpenStrategy }: { onOpenStrategy?: () => void }) {
  const [confirmedActions, setConfirmedActions] = useState<Record<string, ResultActionId>>({});
  const [expandedEvidenceJobId, setExpandedEvidenceJobId] = useState<string | null>(null);
  const [exportNote, setExportNote] = useState("");
  const results = [...offlineData.batchJobRankings].sort((left, right) => right.matchScore - left.matchScore);
  const jobAnalysis = offlineData.jobAnalysis;
  const jobTrend = offlineData.jobTrend;
  const topResult = results[0];
  const riskItems = results.filter((item) => item.recommendationLevel === "not_recommend");
  const concerns = topConcerns(results);

  const handleJobAction = (jobId: string, actionId: ResultActionId) => {
    if (actionId === "view_evidence") setExpandedEvidenceJobId(jobId);
    if (actionId === "enter_application_strategy") {
      onOpenStrategy?.();
      return;
    }
    setConfirmedActions((current) => ({ ...current, [jobId]: actionId }));
  };

  const scrollToTop = () => {
    document.querySelector(".batch-dashboard")?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const exportResults = () => {
    const payload = JSON.stringify({ results, trend: jobTrend }, null, 2);
    const blob = new Blob([payload], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = "careerpilot-batch-results.json";
    link.click();
    URL.revokeObjectURL(url);
    setExportNote("Results exported. Application status was not changed.");
  };

  return (
    <section className="batch-dashboard">
      {topResult && (
        <ResultSummary
          conclusion={buildConclusion(results)}
          nextAction={{
            label: resultActionLabels[primaryActionForRecommendation(topResult.recommendationLevel)],
            onClick: () => handleJobAction(topResult.id, primaryActionForRecommendation(topResult.recommendationLevel)),
          }}
          priorityLabel={recommendationLabels[topResult.recommendationLevel]}
          reasons={jobAnalysis.judgement.evidenceRefs.length ? jobAnalysis.responsibilities : topResult.reasons}
          risks={results.flatMap((item) => item.concerns).slice(0, 3)}
        />
      )}

      {!results.length && (
        <EmptyState
          body="Add JD data before screening. Demo results will appear here after the adapter maps raw responses."
          primaryAction={{ label: "Add JD", onClick: scrollToTop }}
          secondaryAction={{ label: "Review demo", onClick: scrollToTop }}
          title="No JD results"
        />
      )}

      <section className="batch-metrics" aria-label="Batch screening overview">
        <article className="batch-card priority">
          <span>Strong recommend</span>
          <strong>{countByLevel(results, ["strongly_recommend"])}</strong>
          <p>High priority opportunities.</p>
        </article>
        <article className="batch-card rewrite">
          <span>Recommend</span>
          <strong>{countByLevel(results, ["recommend"])}</strong>
          <p>Match exists but evidence needs review.</p>
        </article>
        <article className="batch-card caution">
          <span>Careful review</span>
          <strong>{countByLevel(results, ["cautious"])}</strong>
          <p>Some uncertainty remains.</p>
        </article>
        <article className="batch-card reject">
          <span>Not recommended</span>
          <strong>{countByLevel(results, ["not_recommend"])}</strong>
          <p>Low opportunity value for now.</p>
        </article>
      </section>

      <section className="batch-panel">
        <div className="batch-section-title">
          <div>
            <h2>Top jobs</h2>
            <span>Sorted by recommendation, score, risk, and priority.</span>
          </div>
          <div className="batch-toolbar">
            <button onClick={exportResults} type="button">Export results</button>
            <button onClick={scrollToTop} type="button">Back to top</button>
          </div>
        </div>
        {exportNote && <p className="action-note">{exportNote}</p>}
        <div className="batch-table">
          {results.map((result) => (
            <JobRow
              confirmedAction={confirmedActions[result.id]}
              evidenceOpen={expandedEvidenceJobId === result.id}
              key={result.id}
              onAction={handleJobAction}
              result={result}
            />
          ))}
        </div>
      </section>

      <div className="batch-bottom-grid">
        <section className="batch-panel">
          <div className="batch-section-title">
            <h2>Risk jobs</h2>
            <span>Only jobs currently marked not recommended.</span>
          </div>
          <div className="risk-board">
            {!riskItems.length && <EmptyState body="No high-risk jobs in the current demo set." title="No risk jobs" />}
            {riskItems.map((result) => (
              <article className="risk-item" key={result.id}>
                <strong>{result.job.title}</strong>
                <span>{result.job.companyDisplayName}</span>
                <p>{result.concerns[0] ?? result.reasons[0]}</p>
              </article>
            ))}
          </div>
        </section>

        <section className="batch-panel">
          <div className="batch-section-title">
            <h2>Job pool trend</h2>
            <span>{lensLabels[jobTrend.professionalLens]}: statistics only, no job conclusion rewrite.</span>
          </div>
          <div className="gap-grid">
            <div>
              <h3>Top skills</h3>
              <CompactList items={jobTrend.topSkills.map((label) => ({ label }))} emptyText="No top skills" />
            </div>
            <div>
              <h3>Market signals</h3>
              <CompactList items={jobTrend.marketSignals.map((label) => ({ label }))} emptyText="No market signals" />
            </div>
            <div>
              <h3>Risk summary</h3>
              <CompactList items={concerns} emptyText="No obvious risks" />
            </div>
          </div>
        </section>
      </div>
    </section>
  );
}
