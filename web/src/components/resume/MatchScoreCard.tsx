import type { CSSProperties } from "react";
import type { ResumeMatchResponse, RiskLevel } from "../../types";

const riskLabels: Record<RiskLevel, string> = {
  low: "低风险",
  medium: "中风险",
  high: "高风险",
};

const scoreRows: Array<{ key: keyof ResumeMatchResponse; label: string }> = [
  { key: "requirementFitScore", label: "硬性条件" },
  { key: "skillFitScore", label: "技能证据" },
  { key: "experienceRelevanceScore", label: "经历相关度" },
  { key: "achievementEvidenceScore", label: "成果说服力" },
  { key: "resumeQualityScore", label: "表达质量" },
  { key: "keywordMatchScore", label: "关键词覆盖" },
];

export function MatchScoreCard({ result }: { result: ResumeMatchResponse }) {
  return (
    <section className="score-card">
      <div className="score-ring" style={{ "--score": `${result.overallScore * 3.6}deg` } as CSSProperties}>
        <div>
          <strong>{result.overallScore}</strong>
          <span>总匹配度</span>
        </div>
      </div>
      <div className="score-bars">
        <div className={`risk-pill ${result.riskLevel}`}>
          <span>岗位风险</span>
          <strong>{riskLabels[result.riskLevel]}</strong>
        </div>
        {scoreRows.map((row) => {
          const value = Number(result[row.key]);
          return (
            <div className="score-row" key={row.key}>
              <span>{row.label}</span>
              <div aria-label={`${row.label}${value}分`} className="bar">
                <i style={{ width: `${value}%` }} />
              </div>
              <strong>{value}</strong>
            </div>
          );
        })}
      </div>
    </section>
  );
}
