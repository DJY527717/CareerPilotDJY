from __future__ import annotations

from pathlib import Path

import pandas as pd

from careerpilot.queue_helpers import build_batch_queue_records, build_current_jd_queue_record
from careerpilot.report_exports import build_report_frames


APP_SOURCE = Path(__file__).resolve().parents[1] / "app.py"


def _resume_match_with_conflicting_legacy() -> dict:
    return {
        "overall_score": 80,
        "score": 1,
        "matched_evidence": [
            {
                "jd_requirement": "SQL",
                "resume_evidence": "Built SQL dashboard from real project data.",
                "explanation": "Direct project evidence.",
            }
        ],
        "missing_requirements": [
            {"requirement": "Python", "importance": "HIGH", "reason": "No Python evidence."}
        ],
        "weak_requirements": [
            {"requirement": "A/B testing", "problem": "Evidence is too thin."}
        ],
        "matched_skills": ["LegacyMatched"],
        "missing_skills": ["LegacyMissing"],
        "hard_skill_gaps": ["LegacyHardGap"],
        "evidence_skill_gaps": ["LegacyEvidenceGap"],
        "expression_skill_gaps": ["LegacyExpressionGap"],
        "gap_examples": ["Legacy gap example"],
        "strengths": ["Legacy strength"],
    }


def test_report_export_uses_new_match_fields_over_legacy_fields() -> None:
    frames = build_report_frames(
        session_state={
            "jd_analysis": {
                "basic": {"岗位名": "Data Analyst", "公司名": "Acme"},
                "category": "数据/商业分析岗",
                "value": {"is_high_value": True, "is_generic_esg": False, "high_score": 80, "low_score": 0, "label": "值得看"},
                "skills": pd.DataFrame([{"技能": "SQL", "出现频次": 1}]),
            },
            "resume_match": _resume_match_with_conflicting_legacy(),
        },
        get_active_profile=lambda: {"name": "目标", "content": "数据分析"},
        get_active_resume=lambda: {"name": "简历A", "content": "SQL dashboard", "updated_at": ""},
        build_job_action_plan=lambda _jd, _match: {"decision": "可投", "actions": ["补证据"], "interview_focus": ["SQL"]},
        normalize_resume_lines=lambda text: [text] if text else [],
        target_preferences_text=lambda: "上海 / 金融",
        parse_resume_content=lambda _text: {"lines": ["SQL dashboard"], "sections": {}, "skills": ["SQL"]},
        resume_parse_quality=lambda _parsed: {"label": "OK", "detail": "OK"},
        public_export_df=lambda df: df,
    )

    summary = frames["投递包摘要"].iloc[0]
    match = frames["简历匹配"].iloc[0]

    assert summary["简历匹配度"] == 80
    assert match["匹配度"] == 80
    assert "SQL" in match["已匹配证据"]
    assert "Python" in match["待补充要求"]
    assert "A/B testing" in match["证据偏弱要求"]
    assert "Legacy" not in repr(match.to_dict())


def test_queue_records_use_overall_score_and_final_rank_score_only() -> None:
    current = build_current_jd_queue_record(
        {"basic": {"岗位名": "Data Analyst"}, "category": "数据/商业分析岗", "value": {}},
        _resume_match_with_conflicting_legacy(),
        build_job_action_plan=lambda _jd, _match: {"actions": ["补 SQL 证据"]},
        today_label_fn=lambda: "2026-05-18",
    )
    batch = build_batch_queue_records(
        pd.DataFrame(
            [
                {
                    "公司": "Acme",
                    "岗位": "Data Analyst",
                    "final_rank_score": 77,
                    "意向匹配度": 1,
                    "推荐评分": 2,
                    "综合分": 3,
                    "match_score": 4,
                }
            ]
        ),
        today_label_fn=lambda: "2026-05-18",
    )[0]

    assert current["match_score"] == 80
    assert batch["match_score"] == 77


def test_current_jd_readiness_and_manual_score_ignore_legacy_score() -> None:
    app_source = APP_SOURCE.read_text(encoding="utf-8")

    assert 'resume_match.get("overall_score", 0)' in app_source
    assert 'resume_match.get("final_rank_score", resume_match.get("overall_score", 0))' in app_source
    legacy_score_ref = app_source.index('resume_match.get("score", 0)')
    legacy_helper_ref = app_source.index("def legacy_resume_match_overall_score")
    assert legacy_score_ref > legacy_helper_ref


def test_current_resume_match_helpers_do_not_read_legacy_gap_fields() -> None:
    app_source = APP_SOURCE.read_text(encoding="utf-8")
    helper_start = app_source.index("def resume_match_overall_score")
    helper_end = app_source.index("def legacy_resume_match_overall_score")
    current_score_helper = app_source[helper_start:helper_end]
    assert '"score"' not in current_score_helper

    current_helper_names = [
        "resume_matched_requirement_names",
        "resume_missing_requirement_names",
        "resume_hard_gap_names",
        "resume_evidence_gap_names",
        "resume_expression_gap_names",
        "resume_gap_example_texts",
        "resume_strength_texts",
        "resume_evidence_texts",
    ]
    legacy_fields = [
        "matched_skills",
        "missing_skills",
        "hard_skill_gaps",
        "evidence_skill_gaps",
        "expression_skill_gaps",
    ]
    for name in current_helper_names:
        start = app_source.index(f"def {name}")
        next_def = app_source.index("\ndef ", start + 1)
        body = app_source[start:next_def]
        for field in legacy_fields:
            assert field not in body


def test_report_and_queue_fallback_score_only_for_old_records() -> None:
    current_empty = _resume_match_with_conflicting_legacy() | {
        "overall_score": 0,
        "final_rank_score": 66,
        "matched_evidence": [],
        "missing_requirements": [],
        "weak_requirements": [],
    }
    frames = build_report_frames(
        session_state={
            "jd_analysis": {
                "basic": {"岗位名称": "Data Analyst", "公司名称": "Acme"},
                "category": "数据/商业分析岗",
                "value": {"is_high_value": False, "is_generic_esg": False},
                "skills": pd.DataFrame(),
            },
            "resume_match": current_empty,
        },
        get_active_profile=lambda: {"name": "目标", "content": ""},
        get_active_resume=lambda: {"name": "简历A", "content": "", "updated_at": ""},
        build_job_action_plan=lambda _jd, _match: {"decision": "观察", "actions": [], "interview_focus": []},
        normalize_resume_lines=lambda text: [text] if text else [],
        target_preferences_text=lambda: "",
        parse_resume_content=lambda _text: {"lines": [], "sections": {}, "skills": []},
        resume_parse_quality=lambda _parsed: {"label": "OK", "detail": "OK"},
        public_export_df=lambda df: df,
    )
    match = frames["简历匹配"].iloc[0]
    assert match["匹配度"] == 0
    assert match["推荐评分"] == 66
    assert "Legacy" not in repr(match.to_dict())

    old_records = build_batch_queue_records(
        pd.DataFrame([{"job_title": "Old", "score": 9}]),
        today_label_fn=lambda: "2026-05-18",
    )
    current_records = build_batch_queue_records(
        pd.DataFrame([{"job_title": "Current", "overall_score": 44, "score": 9}]),
        today_label_fn=lambda: "2026-05-18",
    )
    assert old_records[0]["match_score"] == 9
    assert current_records[0]["match_score"] == 44


def test_not_safe_target_ui_does_not_render_copyable_resume_text_area() -> None:
    app_source = APP_SOURCE.read_text(encoding="utf-8")
    ready_start = app_source.index("with ready_tab:")
    ready_end = app_source.index("with parts_tab:", ready_start)
    ready_block = app_source[ready_start:ready_end]

    not_safe_start = ready_block.index('if revision_level == "NOT_SAFE_TO_TARGET"')
    else_start = ready_block.index("else:", not_safe_start)
    not_safe_block = ready_block[not_safe_start:else_start]
    assert "ready_resume_text" not in not_safe_block
    assert "可复制到简历里" not in not_safe_block
    assert "修改方向参考/风险提示" in not_safe_block
