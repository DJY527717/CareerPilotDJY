import { useState } from "react";
import { offlineData } from "../../api";
import { EvidenceCard } from "../../components/EvidenceCard";
import { ResultSummary } from "../../components/ResultSummary";
import type { ApplicationPipelineItem, ApplicationStatus, OpportunityDecisionResult, PriorityLevel, RecommendationLevel } from "../../types";

const statusLabels: Record<ApplicationStatus, string> = {
  not_applied: "Not applied",
  ready_to_apply: "Ready to apply",
  applied: "Applied",
  followed_up: "Followed up",
  interviewing: "Interviewing",
  closed: "Closed",
};

const recommendationLabels: Record<RecommendationLevel, string> = {
  strongly_recommend: "Strong recommend",
  recommend: "Recommend",
  cautious: "Review carefully",
  not_recommend: "Not recommended",
};

function countByPriority(items: OpportunityDecisionResult[], priority: PriorityLevel): number {
  return items.filter((item) => item.priority === priority).length;
}

export function ApplicationStrategyPage() {
  const decisions = offlineData.opportunityDecisions;
  const pipeline = offlineData.applicationPipeline;
  const [statusByJob, setStatusByJob] = useState<Record<string, ApplicationStatus>>(
    Object.fromEntries(pipeline.map((item) => [item.id, item.status])),
  );

  const confirmStatus = (itemId: string, status: ApplicationStatus) => {
    setStatusByJob((current) => ({ ...current, [itemId]: status }));
  };

  const topDecision = decisions[0];
  const riskReasons = decisions.flatMap((item) => item.keyRisks).slice(0, 3);
  const scrollToStrategies = () => {
    document.querySelector(".strategy-list")?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  return (
    <section className="strategy-board">
      <header className="strategy-hero">
        <h2>Application strategy</h2>
        <p>Application status changes require user confirmation. The system only suggests priority and preparation actions.</p>
      </header>

      {topDecision && (
        <ResultSummary
          conclusion={`Current priority: ${topDecision.jobTitle}, suggested priority ${topDecision.priority}.`}
          nextAction={{ label: topDecision.nextActions[0] ?? "Review actions", onClick: scrollToStrategies }}
          priorityLabel={recommendationLabels[topDecision.recommendationLevel]}
          reasons={topDecision.keyReasons}
          risks={riskReasons}
        />
      )}

      <section className="strategy-metrics" aria-label="Application strategy overview">
        <StrategyMetric label="P0 today" value={countByPriority(decisions, "P0")} />
        <StrategyMetric label="P1 this week" value={countByPriority(decisions, "P1")} />
        <StrategyMetric label="P2 observe" value={countByPriority(decisions, "P2")} />
        <StrategyMetric label="Pipeline records" value={pipeline.length} />
      </section>

      <section className="strategy-list">
        {decisions.map((decision) => (
          <DecisionRow decision={decision} key={decision.id} />
        ))}
        {pipeline.map((item) => (
          <PipelineRow
            currentStatus={statusByJob[item.id] ?? item.status}
            item={item}
            key={item.id}
            onConfirmStatus={confirmStatus}
          />
        ))}
      </section>
    </section>
  );
}

function StrategyMetric({ label, value }: { label: string; value: number }) {
  return (
    <article className="strategy-metric">
      <span>{label}</span>
      <strong>{value}</strong>
    </article>
  );
}

function DecisionRow({ decision }: { decision: OpportunityDecisionResult }) {
  return (
    <article className="strategy-row">
      <div className="strategy-job">
        <strong>{decision.jobTitle}</strong>
        <span>{decision.professionalRole === "application_strategy_advisor" ? "Application strategy advisor" : "Strategy suggestion"}</span>
      </div>
      <div className="strategy-tags">
        <span className="strategy-pill focus">{decision.priority}</span>
        <span className="strategy-pill ready">{recommendationLabels[decision.recommendationLevel]}</span>
        <span className="strategy-pill status">Readiness {decision.estimatedReadiness}</span>
      </div>
      <div className="strategy-next">
        <strong>Suggested action</strong>
        <p>{decision.nextActions[0] ?? "Waiting for user confirmation."}</p>
      </div>
      <div className="strategy-reason">
        <p>{decision.keyReasons[0]}</p>
        <details className="evidence-disclosure">
          <summary>Review strategy evidence</summary>
          <EvidenceCard
            confidence={decision.estimatedReadiness / 100}
            evidenceType={decision.keyRisks.length ? "risk_signal" : "preference_match"}
            interpretation={decision.keyReasons.join("; ")}
            sourceExcerpt={decision.keyRisks[0] ?? decision.nextActions[0] ?? ""}
            sourceLabel="Strategy input"
            title="Strategy evidence"
          />
        </details>
        <ul>
          {decision.nextActions.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      </div>
      <div className="strategy-extension">
        <span>The system does not apply on behalf of the user.</span>
        <span>The system does not automatically change application status.</span>
      </div>
    </article>
  );
}

function PipelineRow({
  currentStatus,
  item,
  onConfirmStatus,
}: {
  currentStatus: ApplicationStatus;
  item: ApplicationPipelineItem;
  onConfirmStatus: (itemId: string, status: ApplicationStatus) => void;
}) {
  const canMarkReady = currentStatus !== "applied";
  return (
    <article className="strategy-row">
      <div className="strategy-job">
        <strong>{item.jobTitle}</strong>
        <span>{item.companyDisplayName}</span>
      </div>
      <div className="strategy-tags">
        <span className="strategy-pill status">{statusLabels[currentStatus]}</span>
        <span className="strategy-pill focus">{item.recommendedPriority}</span>
        <span className="strategy-pill ready">{item.userConfirmed ? "User confirmed" : "Awaiting confirmation"}</span>
      </div>
      <div className="strategy-next">
        <strong>Pipeline suggestion</strong>
        <p>{item.nextSuggestedAction}</p>
      </div>
      <div className="strategy-actions">
        <button disabled={!canMarkReady} onClick={() => onConfirmStatus(item.id, "ready_to_apply")} type="button">
          Mark ready
        </button>
        <button disabled={currentStatus === "applied"} onClick={() => onConfirmStatus(item.id, "applied")} type="button">
          User marked applied
        </button>
      </div>
      <div className="strategy-extension">
        <span>Only user-confirmed status changes are recorded.</span>
        <span>Next actions are suggestions.</span>
      </div>
    </article>
  );
}
