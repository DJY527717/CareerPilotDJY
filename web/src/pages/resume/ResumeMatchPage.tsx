import { useMemo, useState } from "react";
import {
  analyzeJd,
  emptyMatchResult,
  emptyResumeParse,
  matchResume,
  offlineData,
  parseResume,
  sampleJdText,
  sampleResumeText,
} from "../../api";
import { ErrorCard } from "../../components/ErrorCard";
import { feedbackCopy } from "../../uiCopy";
import type { JDAnalyzeResponse, ResumeMatchResponse, ResumeParseResponse } from "../../types";
import { createErrorState, type CareerPilotErrorCopy, type CareerPilotErrorType } from "../../errorTypes";
import { EmptyState } from "../../components/EmptyState";
import { EvidenceCard } from "../../components/EvidenceCard";
import { ResultSummary } from "../../components/ResultSummary";
import { HRDecisionPanel } from "../../components/resume/HRDecisionPanel";
import { JDInputPanel } from "../../components/resume/JDInputPanel";
import { MatchInsightList } from "../../components/resume/MatchInsightList";
import { MatchScoreCard } from "../../components/resume/MatchScoreCard";
import { ResumeInputPanel } from "../../components/resume/ResumeInputPanel";
import { ResumeSuggestionPanel } from "../../components/resume/ResumeSuggestionPanel";

const MIN_RESUME_LENGTH = 80;
const MIN_JD_LENGTH = 80;

type ErrorState = CareerPilotErrorCopy & { type: CareerPilotErrorType };

export function ResumeMatchPage() {
  const [resumeText, setResumeText] = useState("");
  const [jdText, setJdText] = useState("");
  const [parseResult, setParseResult] = useState<ResumeParseResponse>(emptyResumeParse);
  const [jdResult, setJdResult] = useState<JDAnalyzeResponse | null>(null);
  const [matchResult, setMatchResult] = useState<ResumeMatchResponse>(emptyMatchResult);
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [error, setError] = useState<ErrorState | null>(null);
  const hasResume = Boolean(resumeText.trim());
  const hasJd = Boolean(jdText.trim());

  const jdKeywords = useMemo(() => jdResult?.analysis.keywords ?? ["Waiting for JD analysis"], [jdResult]);
  const rewriteSuggestions = useMemo(() => offlineData.resumeRewriteSuggestions, []);

  const fillDemoContent = () => {
    setResumeText(sampleResumeText);
    setJdText(sampleJdText);
    setError(null);
  };

  const clearContent = () => {
    setResumeText("");
    setJdText("");
    setParseResult(emptyResumeParse);
    setJdResult(null);
    setMatchResult(emptyMatchResult);
    setError(null);
  };

  const focusResumeInput = () => document.getElementById("resume-text-input")?.focus();
  const focusJdInput = () => document.getElementById("jd-text-input")?.focus();
  const scrollToResults = () => document.querySelector(".resume-result-column")?.scrollIntoView({ behavior: "smooth", block: "start" });
  const retryAnalyze = () => void handleAnalyze();

  const validateInputs = (): ErrorState | null => {
    const cleanResume = resumeText.trim();
    const cleanJd = jdText.trim();
    if (!cleanResume) return createErrorState("missing_resume");
    if (!cleanJd) return createErrorState("missing_jd");
    if (cleanResume.length < MIN_RESUME_LENGTH) {
      return createErrorState("invalid_input", {
        title: "Resume content is too short",
        message: "Add education, project or work experience, ownership, and verifiable outcomes before analysis.",
        nextAction: "Add resume details",
      });
    }
    if (cleanJd.length < MIN_JD_LENGTH) {
      return createErrorState("invalid_input", {
        title: "JD content is too short",
        message: "Add responsibilities, requirements, keywords, location, and work mode before analysis.",
        nextAction: "Add JD details",
      });
    }
    return null;
  };

  const primaryErrorAction = (currentError: ErrorState) => {
    if (currentError.type === "api_unavailable" || currentError.type === "analysis_failed") return retryAnalyze;
    if (currentError.type === "missing_jd" || currentError.nextAction.includes("JD")) return focusJdInput;
    return focusResumeInput;
  };

  const handleAnalyze = async () => {
    const inputError = validateInputs();
    if (inputError) {
      setError(inputError);
      return;
    }

    setIsAnalyzing(true);
    setError(null);
    try {
      const [resume, jd, match] = await Promise.all([
        parseResume(resumeText, "demo-resume.txt"),
        analyzeJd(jdText),
        matchResume({ resumeText, jdText }),
      ]);
      const jdHasUsableStructure =
        jd.analysis.responsibilities.length > 0 || jd.analysis.requiredSkills.length > 0 || jd.analysis.keywords.length > 0;
      if (!jdHasUsableStructure) {
        setJdResult(jd);
        setError(createErrorState("analysis_failed"));
        return;
      }
      const hasResumeEvidence =
        resume.profile.projects.length > 0 || resume.profile.workExperience.length > 0 || resume.profile.skills.length > 0;
      if (!hasResumeEvidence) {
        setParseResult(resume);
        setJdResult(jd);
        setMatchResult(emptyMatchResult);
        setError(createErrorState("insufficient_evidence"));
        return;
      }
      setParseResult(resume);
      setJdResult(jd);
      setMatchResult(match);
    } catch {
      setError(createErrorState("api_unavailable"));
    } finally {
      setIsAnalyzing(false);
    }
  };

  return (
    <div className="resume-workbench">
      <section className="resume-input-column" aria-label="Resume and JD input">
        <div className="input-tools">
          <button disabled={isAnalyzing} onClick={fillDemoContent} type="button">Fill demo content</button>
          <button disabled={isAnalyzing} onClick={clearContent} type="button">Clear</button>
        </div>
        <ResumeInputPanel disabled={isAnalyzing} onChange={setResumeText} value={resumeText} />
        <JDInputPanel disabled={isAnalyzing} onAnalyze={handleAnalyze} onChange={setJdText} value={jdText} />
        {!hasResume && (
          <EmptyState
            body={feedbackCopy.resumeMissing.body}
            primaryAction={{ label: feedbackCopy.resumeMissing.action, onClick: focusResumeInput }}
            secondaryAction={{ label: "Review demo", onClick: fillDemoContent }}
            title={feedbackCopy.resumeMissing.title}
          />
        )}
        {hasResume && !hasJd && (
          <EmptyState
            body={feedbackCopy.jdMissing.body}
            primaryAction={{ label: "Paste target JD", onClick: focusJdInput }}
            secondaryAction={{ label: "Review demo", onClick: fillDemoContent }}
            title={feedbackCopy.jdMissing.title}
          />
        )}
        {isAnalyzing && (
          <div className="status-stack" aria-busy="true" role="status">
            {[feedbackCopy.analyzingJd, feedbackCopy.matchingResume, feedbackCopy.generatingRewrite].map((item) => (
              <div className="status-step" key={item.title}>
                <strong>{item.title}</strong>
                <span>{item.body}</span>
              </div>
            ))}
          </div>
        )}
        {error && (
          <ErrorCard
            {...error}
            onPrimaryAction={primaryErrorAction(error)}
            secondaryAction={{ label: "Review demo", onClick: fillDemoContent }}
          />
        )}
      </section>

      <section className="resume-result-column" aria-label="Analysis results">
        {!error && jdResult && (
          <ResultSummary
            conclusion={`Target JD parsed. Core requirement: ${jdResult.analysis.requiredSkills[0] ?? jdResult.analysis.keywords[0] ?? "job requirement"}.`}
            nextAction={{ label: "Continue resume match", onClick: scrollToResults }}
            priorityLabel="Single JD analysis"
            reasons={(jdResult.analysis.responsibilities.length ? jdResult.analysis.responsibilities : jdResult.analysis.keywords).slice(0, 3)}
            risks={jdResult.analysisNotes.slice(0, 3)}
          />
        )}
        {!error && (
          <>
            <ResultSummary
              conclusion={`Demo baseline: ${offlineData.jobAnalysis.job.title} current score ${offlineData.resumeMatch.overallScore}. Review evidence boundaries first.`}
              nextAction={{ label: offlineData.resumeMatch.recommendedActions[0] ?? "Review evidence", onClick: scrollToResults }}
              priorityLabel="Senior HR lens"
              reasons={offlineData.resumeMatch.strengths}
              risks={offlineData.resumeMatch.gaps}
            />
            <HRDecisionPanel result={matchResult} />
            <MatchScoreCard result={matchResult} />
            <div className="result-grid">
              <MatchInsightList items={matchResult.strengths} title="Strengths" tone="strength" />
              <MatchInsightList items={matchResult.gaps} title="Gaps and uncertainty" tone="risk" />
              <MatchInsightList items={jdKeywords} title="JD keywords" />
              <MatchInsightList items={parseResult.profile.skills.length ? parseResult.profile.skills : ["No skills identified"]} title="Resume skills" />
              <MatchInsightList items={offlineData.gapAnalysisItems.map((item) => `${item.priority} ${item.gap}`)} title="Capability gaps" tone="risk" />
            </div>
          </>
        )}
        {!error && jdResult && (
          <details className="evidence-disclosure" open>
            <summary>Review JD and resume evidence</summary>
            <div className="evidence-grid">
              <EvidenceCard
                confidence={0.86}
                evidenceType="jd_requirement"
                interpretation="Used to identify hard requirements and keywords."
                sourceExcerpt={jdResult.analysis.requiredSkills[0] ?? jdResult.analysis.responsibilities[0] ?? ""}
                sourceLabel="Single JD analysis"
                title="JD evidence"
              />
              <EvidenceCard
                confidence={matchResult.keywordMatchScore / 100}
                evidenceType={parseResult.profile.projects.length ? "resume_evidence" : "missing_evidence"}
                interpretation="Used to decide whether the resume has matching evidence."
                sourceExcerpt={parseResult.profile.projects[0] ?? ""}
                sourceLabel="Resume excerpt"
                title="Resume evidence"
              />
            </div>
          </details>
        )}
        {!error && <ResumeSuggestionPanel parsingNotes={parseResult.parsingNotes} suggestions={rewriteSuggestions} />}
      </section>
    </div>
  );
}
