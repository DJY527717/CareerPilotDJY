type ResultSummaryProps = {
  conclusion: string;
  priorityLabel: string;
  reasons: string[];
  risks: string[];
  nextAction: {
    label: string;
    onClick: () => void;
  };
};

function takeTopThree(items: string[]): string[] {
  return items.filter(Boolean).slice(0, 3);
}

export function ResultSummary({ conclusion, priorityLabel, reasons, risks, nextAction }: ResultSummaryProps) {
  const visibleReasons = takeTopThree(reasons);
  const visibleRisks = takeTopThree(risks);

  return (
    <section className="result-summary">
      <div className="result-summary-main">
        <span>{priorityLabel}</span>
        <h2>{conclusion}</h2>
      </div>
      <div className="result-summary-grid">
        <SummaryList title="关键理由" items={visibleReasons.length ? visibleReasons : ["证据不足，需要用户补充"]} />
        <SummaryList title="主要风险" items={visibleRisks.length ? visibleRisks : ["暂未发现明确高风险，仍需核对证据来源。"]} />
      </div>
      <button className="summary-action" onClick={nextAction.onClick} type="button">
        {nextAction.label}
      </button>
    </section>
  );
}

function SummaryList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="summary-list">
      <strong>{title}</strong>
      <ul>
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
}
