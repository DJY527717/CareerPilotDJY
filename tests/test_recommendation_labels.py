from __future__ import annotations

from pathlib import Path

from careerpilot_api.schemas import (
    APPLICATION_PRIORITY_LABELS,
    PREPARATION_STATUS_LABELS,
    RECOMMENDATION_DECISION_NOTE,
    RECOMMENDATION_LEVEL_LABELS,
)


def test_backend_recommendation_labels_are_canonical() -> None:
    assert RECOMMENDATION_LEVEL_LABELS == {
        "priority_apply": "优先投递",
        "apply_after_rewrite": "改简历后投",
        "cautious_apply": "谨慎投递",
        "backup": "备选观察",
        "not_recommended": "不建议投",
    }
    assert PREPARATION_STATUS_LABELS == {
        "ready": "简历已适配",
        "needs_rewrite": "简历需修改",
        "missing_evidence": "缺少项目证据",
        "preference_conflict": "偏好冲突",
        "insufficient_jd_info": "JD信息不足",
    }
    assert APPLICATION_PRIORITY_LABELS == {
        "today_priority": "今日优先",
        "weekly_focus": "本周重点",
        "after_resume_update": "改简历后投",
        "observe_only": "仅观察",
        "skip": "不建议投",
    }


def test_recommendation_decision_note_allows_risk_and_preference_correction() -> None:
    assert "风险" in RECOMMENDATION_DECISION_NOTE
    assert "偏好" in RECOMMENDATION_DECISION_NOTE
    assert "不只由分数决定" in RECOMMENDATION_DECISION_NOTE


def test_frontend_uses_shared_recommendation_label_module() -> None:
    constants_source = Path("web/src/constants/recommendationLabels.ts").read_text(encoding="utf-8")
    jd_page_source = Path("web/src/pages/jd/BatchJDResultsPage.tsx").read_text(encoding="utf-8")
    strategy_page_source = Path("web/src/pages/decision/ApplicationStrategyPage.tsx").read_text(encoding="utf-8")

    assert "recommendationLevelLabels" in jd_page_source
    assert "applicationPriorityLabels" in strategy_page_source
    assert "preparationStatusLabels" in strategy_page_source
    assert "recommendationDecisionNote" in constants_source
