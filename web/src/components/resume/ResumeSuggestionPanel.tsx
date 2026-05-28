import { useMemo, useState } from "react";
import { offlineData } from "../../api";
import { feedbackCopy } from "../../uiCopy";
import type { ResumeRewriteSuggestion } from "../../types";
import { EmptyState } from "../EmptyState";
import { EvidenceCard } from "../EvidenceCard";
import { ResultSummary } from "../ResultSummary";

type ResumeSuggestionPanelProps = {
  suggestions: ResumeRewriteSuggestion[];
  parsingNotes: string[];
};

export function ResumeSuggestionPanel({ suggestions, parsingNotes }: ResumeSuggestionPanelProps) {
  const [draftText, setDraftText] = useState("");
  const [savedCount, setSavedCount] = useState(0);
  const draftLines = useMemo(
    () => suggestions.filter((suggestion) => suggestion.allowedToApply).map((suggestion) => suggestion.rewrittenExample),
    [suggestions],
  );
  const needsMoreEvidence = suggestions.some((suggestion) => !suggestion.allowedToApply || suggestion.userInputRequired);
  const safeRewriteCount = suggestions.filter((suggestion) => suggestion.allowedToApply).length;

  const applyDraft = () => {
    setDraftText(draftLines.length ? draftLines.join("\n") : "More verified evidence is required before a resume draft can be applied.");
  };

  const saveSuggestions = () => {
    setSavedCount(suggestions.length);
  };

  return (
    <section className="suggestion-panel rewrite-panel">
      <div className="suggestion-groups">
        <ResultSummary
          conclusion={
            safeRewriteCount
              ? `${safeRewriteCount} suggestion(s) can be moved into a draft after review.`
              : "Current rewrite suggestions need more verified evidence before use."
          }
          nextAction={{
            label: safeRewriteCount ? "Apply to draft" : "Save suggestions",
            onClick: safeRewriteCount ? applyDraft : saveSuggestions,
          }}
          priorityLabel="Resume rewrite"
          reasons={suggestions.map((suggestion) => suggestion.reason)}
          risks={suggestions
            .filter((suggestion) => !suggestion.allowedToApply || suggestion.userInputRequired)
            .map((suggestion) => suggestion.issue)}
        />

        <div className="rewrite-head">
          <div>
            <h2>Target JD rewrite suggestions</h2>
            <p>Suggestions stay within user-provided evidence and do not invent personal information.</p>
          </div>
          <div className="rewrite-actions">
            <button onClick={safeRewriteCount ? applyDraft : saveSuggestions} type="button">
              {safeRewriteCount ? "Apply to draft" : "Save suggestions"}
            </button>
            <button onClick={saveSuggestions} type="button">
              Save only
            </button>
          </div>
        </div>

        {!suggestions.length && <EmptyState body={feedbackCopy.insufficientEvidence.body} title={feedbackCopy.insufficientEvidence.title} />}
        {needsMoreEvidence && <EmptyState body={feedbackCopy.missingProjectEvidence.body} title={feedbackCopy.missingProjectEvidence.title} />}

        {suggestions.map((suggestion) => (
          <RewriteCard key={suggestion.id} suggestion={suggestion} />
        ))}

        {draftText && (
          <div className="draft-preview">
            <h3>Draft preview</h3>
            <p>{draftText}</p>
          </div>
        )}
        {savedCount > 0 && <p className="save-note">Saved {savedCount} suggestion(s). Original resume content was not overwritten.</p>}
      </div>

      <div className="parse-notes">
        <h3>Recommended jobs after rewrite</h3>
        <RecommendedJobs suggestions={suggestions} />
        <h3>Parsing notes</h3>
        <ul>
          {parsingNotes.map((note) => (
            <li key={note}>{note}</li>
          ))}
        </ul>
      </div>
    </section>
  );
}

function RewriteCard({ suggestion }: { suggestion: ResumeRewriteSuggestion }) {
  return (
    <article className="rewrite-card">
      <div className="rewrite-card-top">
        <strong>{suggestion.targetSection}</strong>
        <span>{suggestion.allowedToApply ? "Can apply" : "Needs evidence"}</span>
        <span>{suggestion.priority}</span>
      </div>
      <div className="rewrite-pair">
        <div>
          <span>Current issue</span>
          <p>{suggestion.issue}</p>
        </div>
        <div>
          <span>Suggested wording</span>
          <p>{suggestion.rewrittenExample}</p>
        </div>
      </div>
      <div className="rewrite-meta">
        <p>{suggestion.suggestion}</p>
        <p>{suggestion.reason}</p>
        <details className="evidence-disclosure">
          <summary>Review rewrite evidence</summary>
          <EvidenceCard
            confidence={suggestion.allowedToApply ? 0.78 : 0.36}
            evidenceType={suggestion.allowedToApply ? "rewrite_reason" : "missing_evidence"}
            interpretation={suggestion.reason}
            sourceExcerpt={suggestion.allowedToApply ? suggestion.rewrittenExample : ""}
            sourceLabel="Resume excerpt"
            title={suggestion.allowedToApply ? "Rewrite evidence" : "Missing evidence"}
          />
        </details>
        <TagList title="Evidence status" items={[suggestion.evidenceStatus]} />
        <TagList title="Role lens" items={["resume_consultant"]} />
      </div>
      {(!suggestion.allowedToApply || suggestion.userInputRequired) && (
        <div className="evidence-warning">More real project or work evidence is required before this can be used.</div>
      )}
    </article>
  );
}

function TagList({ title, items }: { title: string; items: string[] }) {
  return (
    <div className="tag-list">
      <span>{title}</span>
      <div>
        {items.map((item) => (
          <i key={item}>{item}</i>
        ))}
      </div>
    </div>
  );
}

function RecommendedJobs({ suggestions }: { suggestions: ResumeRewriteSuggestion[] }) {
  const hasRewriteSignal = suggestions.some((suggestion) => suggestion.allowedToApply || suggestion.userInputRequired);
  const recommendedJobs = hasRewriteSignal ? offlineData.opportunityDecisions.slice(0, 3) : [];
  if (!recommendedJobs.length) {
    return <EmptyState body={feedbackCopy.noPriorityJobs.body} title={feedbackCopy.noPriorityJobs.title} />;
  }
  return (
    <ul className="recommended-job-list">
      {recommendedJobs.map((job) => (
        <li key={job.id}>{job.jobTitle}</li>
      ))}
    </ul>
  );
}
