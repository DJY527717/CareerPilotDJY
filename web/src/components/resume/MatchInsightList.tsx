type MatchInsightListProps = {
  title: string;
  items: string[];
  tone?: "neutral" | "risk" | "strength";
};

export function MatchInsightList({ title, items, tone = "neutral" }: MatchInsightListProps) {
  return (
    <section className="insight-list" data-tone={tone}>
      <h2>{title}</h2>
      <ul>
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </section>
  );
}
