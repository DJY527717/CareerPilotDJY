type ResumeInputPanelProps = {
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
};

export function ResumeInputPanel({ value, onChange, disabled = false }: ResumeInputPanelProps) {
  return (
    <section className="input-panel">
      <div className="panel-heading">
        <span>Resume</span>
        <h2>简历文本</h2>
      </div>
      <textarea
        aria-label="简历文本"
        id="resume-text-input"
        disabled={disabled}
        onChange={(event) => onChange(event.target.value)}
        placeholder="粘贴简历文本。示例：教育背景、项目协作经历、数据看板建设、业务分析报告、用户研究支持等。"
        rows={12}
        value={value}
      />
    </section>
  );
}
