from datetime import datetime
from typing import Any, Callable

import pandas as pd


def row_first(row: pd.Series, *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = row.get(key, None)
        if value is not None and str(value).strip() not in {"", "nan", "None"}:
            return value
    return default


def today_label() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def resume_queue_score(resume_match: dict[str, Any] | None) -> int:
    if not resume_match:
        return 0
    # Legacy score fallback is only for old saved records; current JD queues use final_rank_score/overall_score.
    if "final_rank_score" in resume_match:
        return int(resume_match.get("final_rank_score") or 0)
    if "overall_score" in resume_match:
        return int(resume_match.get("overall_score") or 0)
    return int(resume_match.get("score", 0) or 0)


def resume_overall_score(resume_match: dict[str, Any] | None) -> int:
    if not resume_match:
        return 0
    # Legacy score fallback is only for old saved records that predate overall_score.
    return int((resume_match.get("overall_score") if "overall_score" in resume_match else resume_match.get("score", 0)) or 0)


def build_current_jd_queue_record(
    jd_analysis: dict[str, Any],
    resume_match: dict[str, Any] | None,
    *,
    build_job_action_plan: Callable[[dict[str, Any], dict[str, Any] | None], dict[str, Any]],
    today_label_fn: Callable[[], str],
) -> dict[str, Any]:
    basic = jd_analysis.get("basic", {})
    plan = build_job_action_plan(jd_analysis, resume_match)
    return {
        "company": basic.get("公司名", ""),
        "job_title": basic.get("岗位名", "") or jd_analysis.get("category", ""),
        "salary": basic.get("薪资", ""),
        "location": basic.get("地点", ""),
        "category": jd_analysis.get("category", ""),
        "match_score": resume_queue_score(resume_match),
        "is_high_value": jd_analysis.get("value", {}).get("is_high_value", False),
        "is_generic_esg": jd_analysis.get("value", {}).get("is_generic_esg", False),
        "applied": False,
        "interview_status": "未开始",
        "offer_status": "无",
        "notes": "今日队列：来自单条JD分析",
        "queue_date": today_label_fn(),
        "next_action": str(plan.get("actions", ["定制简历并确认投递渠道"])[0]),
    }


def build_batch_queue_records(
    rows: pd.DataFrame,
    *,
    today_label_fn: Callable[[], str],
) -> list[dict[str, Any]]:
    if rows is None or rows.empty:
        return []
    records: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        records.append(
            {
                "company": row_first(row, "公司", "company"),
                "job_title": row_first(row, "岗位", "岗位名", "job_title"),
                "salary": row_first(row, "薪资", "salary"),
                "location": row_first(row, "地点", "location"),
                "category": row_first(row, "岗位分类", "岗位方向", "分类", "category"),
                # Legacy score fallback is only for old queue rows that lack current ranking fields.
                "match_score": int(row_first(row, "final_rank_score", "overall_score", "score", default=0) or 0),
                "is_high_value": str(row_first(row, "高价值", "优先关注", "高价值岗位")) == "是",
                "is_generic_esg": str(row_first(row, "低价值风险", "待核实风险", "泛ESG风险")) == "是",
                "applied": False,
                "interview_status": "未开始",
                "offer_status": "无",
                "notes": f"今日队列：{row.get('来源', '批量JD筛选')}",
                "queue_date": today_label_fn(),
                "next_action": row_first(row, "下一步动作", "下一步", default="定制简历并确认投递渠道"),
            }
        )
    return records
