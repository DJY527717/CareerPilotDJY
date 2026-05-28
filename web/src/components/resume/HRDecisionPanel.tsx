import type { ResumeMatchResponse } from "../../types";
import { EvidenceCard } from "../EvidenceCard";

const decisionLabels: Record<ResumeMatchResponse["hrDecision"], string> = {
  recommend_interview: "Recommend interview",
  maybe: "Needs review",
  not_recommended: "Not recommended",
};

type HRDecisionPanelProps = {
  result: ResumeMatchResponse;
};

export function HRDecisionPanel({ result }: HRDecisionPanelProps) {
  return (
    <section className="hr-decision-panel">
      <div className="decision-header">
        <div>
          <span>Senior HR review</span>
          <h2>{decisionLabels[result.hrDecision]}</h2>
        </div>
        <strong>{result.riskLevel}</strong>
      </div>
      <p>{result.hrSummary}</p>
      <div className="evidence-grid">
        <EvidenceCard
          confidence={result.overallScore / 100}
          evidenceType="resume_evidence"
          interpretation={result.strengths[0] ?? "Waiting for resume evidence."}
          sourceExcerpt={result.strengths[0] ?? ""}
          sourceLabel="Resume match result"
          title="Positive signal"
        />
        <EvidenceCard
          confidence={result.riskLevel === "high" ? 0.35 : 0.6}
          evidenceType="risk_signal"
          interpretation={result.risks[0] ?? "No strong risk signal in the demo result."}
          sourceExcerpt={result.risks[0] ?? ""}
          sourceLabel="Risk review"
          title="Risk signal"
        />
      </div>
    </section>
  );
}
