from datetime import datetime
from typing import Any, Callable

import pandas as pd


def today_label() -> str:
    return datetime.now().strftime("%Y-%m-%d")


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
        "match_score": int(resume_match.get("score", 0) if resume_match else 0),
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
                "company": row.get("公司", ""),
                "job_title": row.get("岗位", ""),
                "salary": row.get("薪资", ""),
                "location": row.get("地点", ""),
                "category": row.get("岗位分类", ""),
                "match_score": int(row.get("意向匹配度", 0) or 0),
                "is_high_value": str(row.get("高价值", "")) == "是",
                "is_generic_esg": str(row.get("低价值风险", "")) == "是",
                "applied": False,
                "interview_status": "未开始",
                "offer_status": "无",
                "notes": f"今日队列：{row.get('来源', '批量JD筛选')}",
                "queue_date": today_label_fn(),
                "next_action": row.get("下一步动作", "定制简历并确认投递渠道"),
            }
        )
    return records
