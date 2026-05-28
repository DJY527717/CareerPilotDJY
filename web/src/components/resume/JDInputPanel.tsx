type JDInputPanelProps = {
  value: string;
  onAnalyze: () => void;
  onChange: (value: string) => void;
  disabled?: boolean;
};

export function JDInputPanel({ value, onAnalyze, onChange, disabled = false }: JDInputPanelProps) {
  return (
    <section className="input-panel">
      <div className="panel-heading">
        <span>Job Description</span>
        <h2>目标JD</h2>
      </div>
      <textarea
        aria-label="目标JD文本"
        id="jd-text-input"
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        placeholder="粘贴目标岗位描述。示例：数据分析岗位，负责指标监控、数据看板、业务分析报告，要求SQL、Excel、项目协作能力。"
        rows={10}
        value={value}
      />
      <button className="primary-action-button" disabled={disabled} onClick={onAnalyze} type="button">
        {disabled ? "正在分析..." : "开始分析"}
      </button>
    </section>
  );
}
