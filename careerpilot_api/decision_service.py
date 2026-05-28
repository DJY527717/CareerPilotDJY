"""Decision service skeleton for opportunity evaluation."""

from __future__ import annotations

from typing import Any

from careerpilot_api.schemas import ActionItemSchema


def evaluate_decision_for_api(payload: dict[str, Any]) -> dict[str, object]:
    text = _text(payload)
    fit_score = _fit_score(text)
    risk_score = _risk_score(text)
    priority_score = max(0, min(100, round(fit_score * 0.7 + (100 - risk_score) * 0.3)))
    reasons = _reasons(text, fit_score, risk_score)
    return {
        "summary": f"当前机会优先级占位评分为 {priority_score}，用于后续替换真实决策模型。",
        "priority_score": priority_score,
        "fit_score": fit_score,
        "risk_score": risk_score,
        "reasons": reasons,
        "suggested_actions": [
            ActionItemSchema(id="decision-compare", title="补充对比信息", detail="加入岗位职责、薪资、地点和流程状态后再做排序。", priority="中"),
            ActionItemSchema(id="decision-evidence", title="核对关键风险", detail="确认岗位来源、职责边界和投入成本。", priority="高" if risk_score >= 60 else "中"),
        ],
    }


def _text(payload: dict[str, Any]) -> str:
    return " ".join(str(payload.get(key) or "") for key in ["text", "job_text", "jd_text", "notes"]).strip()


def _fit_score(text: str) -> int:
    if not text:
        return 35
    positive = ["匹配", "清晰", "成长", "项目", "数据", "产品", "remote", "growth", "project"]
    return min(100, 45 + sum(1 for item in positive if item.lower() in text.lower()) * 8 + min(len(text) // 60, 15))


def _risk_score(text: str) -> int:
    if not text:
        return 65
    risk_terms = ["模糊", "无转正", "长期出差", "销售指标", "unclear", "unpaid", "travel"]
    return min(100, 25 + sum(1 for item in risk_terms if item.lower() in text.lower()) * 18)


def _reasons(text: str, fit_score: int, risk_score: int) -> list[str]:
    reasons = []
    if not text:
        reasons.append("尚未提供机会信息，当前仅返回通用决策骨架。")
    if fit_score >= 70:
        reasons.append("文本中出现较多正向匹配信号。")
    else:
        reasons.append("匹配信号仍需补充。")
    if risk_score >= 60:
        reasons.append("存在需要优先核实的风险信号。")
    else:
        reasons.append("暂未识别到高风险信号。")
    return reasons
