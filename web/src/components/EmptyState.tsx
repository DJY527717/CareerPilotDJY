type EmptyStateProps = {
  title: string;
  body: string;
  primaryAction?: {
    label: string;
    onClick: () => void;
  };
  secondaryAction?: {
    label: string;
    onClick: () => void;
  };
};

export function EmptyState({ title, body, primaryAction, secondaryAction }: EmptyStateProps) {
  return (
    <div className="empty-state" role="status">
      <h3>{title}</h3>
      <p>{body}</p>
      {(primaryAction || secondaryAction) && (
        <div className="empty-state-actions">
          {primaryAction && (
            <button onClick={primaryAction.onClick} type="button">
              {primaryAction.label}
            </button>
          )}
          {secondaryAction && (
            <button onClick={secondaryAction.onClick} type="button">
              {secondaryAction.label}
            </button>
          )}
        </div>
      )}
    </div>
  );
}
