import type { CareerPilotErrorCopy, CareerPilotErrorType } from "../errorTypes";

type ErrorCardProps = CareerPilotErrorCopy & {
  type: CareerPilotErrorType;
  onPrimaryAction?: () => void;
  secondaryAction?: {
    label: string;
    onClick: () => void;
  };
};

export function ErrorCard({
  message,
  nextAction,
  onPrimaryAction,
  recoverable,
  secondaryAction,
  title,
}: ErrorCardProps) {
  return (
    <div className="error-card" role="alert">
      <div>
        <h3>{title}</h3>
        <p>{message}</p>
      </div>
      <div className="error-card-actions">
        {recoverable && onPrimaryAction && (
          <button onClick={onPrimaryAction} type="button">
            {nextAction}
          </button>
        )}
        {secondaryAction && (
          <button onClick={secondaryAction.onClick} type="button">
            {secondaryAction.label}
          </button>
        )}
      </div>
    </div>
  );
}
