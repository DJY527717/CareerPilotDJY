export type EvidenceType =
  | "jd_requirement"
  | "resume_evidence"
  | "preference_match"
  | "missing_evidence"
  | "risk_signal"
  | "rewrite_reason";

type EvidenceCardProps = {
  evidenceType: EvidenceType;
  title: string;
  sourceLabel: string;
  sourceExcerpt?: string;
  interpretation: string;
  confidence: number;
};

const evidenceTypeLabels: Record<EvidenceType, string> = {
  jd_requirement: "JD要求",
  resume_evidence: "简历证据",
  preference_match: "偏好匹配",
  missing_evidence: "缺失证据",
  risk_signal: "风险信号",
  rewrite_reason: "改写依据",
};

function clampConfidence(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(1, value));
}

function hasTraceableSource(value: string): boolean {
  const text = value.trim();
  return Boolean(text) && !["尚未", "未识别", "等待", "暂无"].some((marker) => text.includes(marker));
}

export function EvidenceCard({
  evidenceType,
  title,
  sourceLabel,
  sourceExcerpt = "",
  interpretation,
  confidence,
}: EvidenceCardProps) {
  const hasSource = hasTraceableSource(sourceExcerpt);
  const safeConfidence = hasSource ? clampConfidence(confidence) : 0;

  return (
    <article className={`evidence-card ${evidenceType} ${hasSource ? "" : "insufficient"}`}>
      <div className="evidence-card-top">
        <span>{evidenceTypeLabels[evidenceType]}</span>
        <strong>{hasSource ? `${Math.round(safeConfidence * 100)}%` : "证据不足"}</strong>
      </div>
      <h3>{hasSource ? title : "证据不足，需要用户补充"}</h3>
      <p className="evidence-source">
        <span>{sourceLabel}</span>
        {hasSource ? sourceExcerpt : "未提供可追溯片段"}
      </p>
      <p className="evidence-interpretation">
        {hasSource ? interpretation : "暂不输出强结论。请补充JD、简历片段或偏好项后再判断。"}
      </p>
    </article>
  );
}
