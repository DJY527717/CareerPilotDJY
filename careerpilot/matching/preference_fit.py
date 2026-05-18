from __future__ import annotations

import re
from typing import Any


JsonDict = dict


UNKNOWN_SALARY: JsonDict = {
    "raw_text": "",
    "min_monthly": None,
    "max_monthly": None,
    "min_annual": None,
    "max_annual": None,
    "currency": "CNY",
    "period": "unknown",
    "salary_type": "unknown",
    "confidence": 0.0,
}


NEGOTIABLE_TERMS = ("薪资面议", "工资面议", "待遇面议", "面议", "薪酬面议")


def _clamp_score(value: float) -> int:
    return int(round(max(0, min(100, value))))


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _to_number(value: str, unit: str) -> float:
    number = float(str(value).replace(",", ""))
    unit = unit.lower()
    if unit == "k":
        return number * 1000
    if unit in {"w", "万"}:
        return number * 10000
    return number


def _annualized_monthly(min_monthly: float | None, max_monthly: float | None, months: int = 12) -> tuple[int | None, int | None]:
    if min_monthly is None and max_monthly is None:
        return None, None
    min_annual = int(round((min_monthly or max_monthly or 0) * months))
    max_annual = int(round((max_monthly or min_monthly or 0) * months))
    return min_annual, max_annual


def _monthly_from_annual(min_annual: float | None, max_annual: float | None) -> tuple[int | None, int | None]:
    if min_annual is None and max_annual is None:
        return None, None
    min_monthly = int(round((min_annual or max_annual or 0) / 12))
    max_monthly = int(round((max_annual or min_annual or 0) / 12))
    return min_monthly, max_monthly


def extract_salary_from_jd(jd_text: str) -> JsonDict:
    text = _clean_text(jd_text)
    result = dict(UNKNOWN_SALARY)
    if not text:
        return result

    if any(term in text for term in NEGOTIABLE_TERMS):
        result.update(
            {
                "raw_text": next((term for term in NEGOTIABLE_TERMS if term in text), "面议"),
                "period": "negotiable",
                "salary_type": "negotiable",
                "confidence": 0.9,
            }
        )
        return result

    salary_context = r"(?:薪资|薪酬|月薪|工资|待遇|年薪)?\s*"
    separator = r"\s*(?:-|~|–|—|至|到)\s*"

    patterns = [
        (
            "monthly",
            re.compile(
                salary_context
                + r"(?P<min>\d+(?:\.\d+)?)\s*(?P<min_unit>[kK千])?"
                + separator
                + r"(?P<max>\d+(?:\.\d+)?)\s*(?P<max_unit>[kK千])(?:\s*[·xX*]\s*(?P<months>1[0-9])\s*薪)?",
            ),
        ),
        (
            "annual",
            re.compile(
                salary_context
                + r"(?P<min>\d+(?:\.\d+)?)\s*(?P<min_unit>万|w|W)?"
                + separator
                + r"(?P<max>\d+(?:\.\d+)?)\s*(?P<max_unit>万|w|W)\s*/?\s*年",
            ),
        ),
        (
            "annual",
            re.compile(r"年薪\s*(?P<min>\d+(?:\.\d+)?)\s*(?P<min_unit>万|w|W)"),
        ),
        (
            "daily",
            re.compile(
                salary_context
                + r"(?P<min>\d+(?:\.\d+)?)"
                + separator
                + r"(?P<max>\d+(?:\.\d+)?)\s*(?:元)?\s*/?\s*(?:天|日)"
            ),
        ),
        (
            "hourly",
            re.compile(
                salary_context
                + r"(?P<min>\d+(?:\.\d+)?)"
                + separator
                + r"(?P<max>\d+(?:\.\d+)?)\s*(?:元)?\s*/?\s*(?:小时|时|h|H)"
            ),
        ),
    ]

    for period, pattern in patterns:
        match = pattern.search(text)
        if not match:
            continue
        groups = match.groupdict()
        min_unit = groups.get("min_unit") or groups.get("max_unit") or ""
        max_unit = groups.get("max_unit") or min_unit
        min_value = _to_number(groups["min"], "k" if min_unit == "千" else min_unit)
        max_value = _to_number(groups.get("max") or groups["min"], "k" if max_unit == "千" else max_unit)
        raw_text = match.group(0).strip()
        months = int(groups.get("months") or 12)

        if period == "monthly":
            min_monthly = int(round(min_value))
            max_monthly = int(round(max_value))
            min_annual, max_annual = _annualized_monthly(min_monthly, max_monthly, months)
        elif period == "annual":
            min_annual = int(round(min_value))
            max_annual = int(round(max_value))
            min_monthly, max_monthly = _monthly_from_annual(min_annual, max_annual)
        else:
            min_monthly = max_monthly = min_annual = max_annual = None

        result.update(
            {
                "raw_text": raw_text,
                "min_monthly": min_monthly,
                "max_monthly": max_monthly,
                "min_annual": min_annual,
                "max_annual": max_annual,
                "period": period,
                "salary_type": period,
                "confidence": 0.88 if period in {"monthly", "annual"} else 0.78,
            }
        )
        return result

    return result


def _target_salary_from_preferences(preferences: JsonDict | None) -> JsonDict | None:
    if not preferences:
        return None
    target = preferences.get("target_salary")
    if isinstance(target, dict) and (target.get("min") or target.get("max")):
        return target

    # Backward compatibility: old preferences stored only one monthly/daily floor.
    min_monthly = preferences.get("min_monthly_salary")
    if preferences.get("target_salary_enabled") and min_monthly:
        return {
            "min": int(min_monthly),
            "max": int(preferences.get("max_monthly_salary") or 0) or None,
            "period": "monthly",
            "currency": "CNY",
            "strict": bool(preferences.get("salary_strict")),
        }
    return None


def _salary_range_for_period(salary: JsonDict, period: str) -> tuple[float | None, float | None]:
    if period == "annual":
        return salary.get("min_annual"), salary.get("max_annual")
    if period == "daily" and salary.get("period") == "daily":
        return salary.get("min"), salary.get("max")
    return salary.get("min_monthly"), salary.get("max_monthly")


def _target_range(target_salary: JsonDict) -> tuple[float | None, float | None, str]:
    period = str(target_salary.get("period") or "monthly")
    minimum = target_salary.get("min")
    maximum = target_salary.get("max")
    try:
        minimum = float(minimum) if minimum is not None and str(minimum).strip() else None
    except Exception:
        minimum = None
    try:
        maximum = float(maximum) if maximum is not None and str(maximum).strip() else None
    except Exception:
        maximum = None
    return minimum, maximum, period


def calculate_salary_fit(jd_salary: JsonDict, target_salary: JsonDict | None) -> JsonDict:
    if not target_salary:
        return {
            "salary_fit_score": 70,
            "salary_fit_level": "UNKNOWN",
            "salary_mismatch_warnings": [],
            "reason": "用户未设置目标薪资，薪资不参与强筛选。",
        }

    period = str((jd_salary or {}).get("period") or "unknown")
    if period in {"unknown", "negotiable"}:
        return {
            "salary_fit_score": 70,
            "salary_fit_level": "UNKNOWN",
            "salary_mismatch_warnings": [],
            "reason": "JD 未明确薪资或薪资面议，不做强惩罚。",
        }

    target_min, target_max, target_period = _target_range(target_salary)
    if target_min is None and target_max is None:
        return {
            "salary_fit_score": 70,
            "salary_fit_level": "UNKNOWN",
            "salary_mismatch_warnings": [],
            "reason": "目标薪资为空，薪资不参与强筛选。",
        }

    jd_min, jd_max = _salary_range_for_period(jd_salary, target_period)
    if jd_min is None and jd_max is None:
        return {
            "salary_fit_score": 70,
            "salary_fit_level": "UNKNOWN",
            "salary_mismatch_warnings": [],
            "reason": "JD 薪资单位和目标薪资暂不能直接比较。",
        }
    jd_min = float(jd_min or jd_max or 0)
    jd_max = float(jd_max or jd_min or 0)
    target_min = float(target_min or target_max or 0)
    target_max = float(target_max or target_min or 0)

    warnings: list[str] = []
    if jd_max < target_min:
        warnings.append("JD 薪资上限低于你的目标薪资下限。")
        gap_ratio = (target_min - jd_max) / max(target_min, 1)
        score = 40 - min(20, gap_ratio * 60)
        return {
            "salary_fit_score": _clamp_score(score),
            "salary_fit_level": "LOW",
            "salary_mismatch_warnings": warnings,
            "reason": "JD 薪资区间明显低于目标薪资。",
        }

    if jd_min >= target_min:
        score = 92
        if jd_min >= target_min * 1.2:
            score = 98
        if target_max and jd_min > target_max * 1.15:
            score = 95
        return {
            "salary_fit_score": _clamp_score(score),
            "salary_fit_level": "HIGH",
            "salary_mismatch_warnings": [],
            "reason": "JD 薪资下限达到或高于目标薪资下限。",
        }

    has_overlap = jd_min <= target_max and jd_max >= target_min
    if has_overlap:
        overlap = min(jd_max, target_max) - max(jd_min, target_min)
        target_width = max(target_max - target_min, 1)
        score = 80 + min(15, max(0, overlap) / target_width * 15)
        return {
            "salary_fit_score": _clamp_score(score),
            "salary_fit_level": "HIGH" if score >= 88 else "MEDIUM",
            "salary_mismatch_warnings": [],
            "reason": "JD 薪资区间和目标薪资区间有交集。",
        }

    return {
        "salary_fit_score": 62,
        "salary_fit_level": "MEDIUM",
        "salary_mismatch_warnings": [],
        "reason": "JD 薪资略低于目标区间，但仍接近目标下限。",
    }


def _split_items(value: Any) -> list[str]:
    if isinstance(value, list):
        raw_items = value
    else:
        raw_items = re.split(r"[、,，/\n\s]+", str(value or ""))
    return list(dict.fromkeys(str(item).strip() for item in raw_items if str(item).strip()))


def _text_has_any(text: str, items: list[str]) -> bool:
    clean = _clean_text(text).lower()
    return any(str(item).lower() in clean for item in items if item)


def _dimension_score(text: str, items: list[str], *, unknown_score: int = 70) -> tuple[int, bool]:
    if not items:
        return unknown_score, False
    if not text:
        return unknown_score, False
    return (92, True) if _text_has_any(text, items) else (45, True)


def calculate_preference_fit(jd_structured: JsonDict, preferences: JsonDict | None) -> JsonDict:
    preferences = preferences or {}
    location_text = _clean_text(str(jd_structured.get("location", "") or ""))
    industry_text = _clean_text(" ".join(jd_structured.get("industry_background", []) or []))
    jd_text = _clean_text(
        " ".join(
            str(item or "")
            for item in [
                jd_structured.get("raw_text", ""),
                jd_structured.get("job_title", ""),
                location_text,
                industry_text,
            ]
        )
    )
    target_industries = _split_items(preferences.get("preferred_industries", []))
    target_cities = _split_items(preferences.get("target_cities", []))
    target_salary = _target_salary_from_preferences(preferences)
    jd_salary = jd_structured.get("salary") or extract_salary_from_jd(jd_text)

    industry_score, industry_active = _dimension_score(jd_text, target_industries)
    city_score, city_active = _dimension_score(location_text, target_cities)
    salary_result = calculate_salary_fit(jd_salary, target_salary)
    salary_score = int(salary_result["salary_fit_score"])
    salary_active = bool(target_salary and salary_result["salary_fit_level"] != "UNKNOWN")

    dimensions = [
        ("industry", industry_score, 0.35, industry_active),
        ("city", city_score, 0.35, city_active),
        ("salary", salary_score, 0.30, salary_active),
    ]
    active = [item for item in dimensions if item[3]]
    if active:
        weight_total = sum(item[2] for item in active)
        preference_score = _clamp_score(sum(item[1] * item[2] for item in active) / max(weight_total, 0.01))
    else:
        preference_score = 70

    warnings: list[str] = []
    if target_cities and city_active and city_score < 60:
        warnings.append("城市不在当前目标城市偏好中。")
    warnings.extend(salary_result["salary_mismatch_warnings"])

    return {
        "preference_fit_score": preference_score,
        "industry_fit_score": industry_score,
        "city_fit_score": city_score,
        "salary_fit_score": salary_score,
        "salary_fit_level": salary_result["salary_fit_level"],
        "salary_mismatch_warnings": salary_result["salary_mismatch_warnings"],
        "preference_warnings": warnings,
        "salary_fit_reason": salary_result["reason"],
    }


def has_meaningful_preferences(preferences: JsonDict | None) -> bool:
    if not preferences:
        return False
    return bool(
        _split_items(preferences.get("preferred_industries", []))
        or _split_items(preferences.get("target_cities", []))
        or _split_items(preferences.get("target_roles", []))
        or _split_items(preferences.get("career_target", []))
        or _split_items(preferences.get("job_keywords", []))
        or _target_salary_from_preferences(preferences)
    )


def apply_preference_ceilings(
    final_rank_score: int,
    *,
    overall_score: int,
    career_target_fit_score: int,
    preference_fit: JsonDict,
    preferences: JsonDict | None,
) -> int:
    preferences = preferences or {}
    score = final_rank_score
    salary_score = int(preference_fit.get("salary_fit_score") or 70)
    city_score = int(preference_fit.get("city_fit_score") or 70)
    target_salary = _target_salary_from_preferences(preferences)
    salary_strict = bool((target_salary or {}).get("strict"))
    city_strict = bool(
        preferences.get("target_city_strict")
        or preferences.get("city_strict")
        or preferences.get("strict_city_match")
    )

    if career_target_fit_score < 40:
        score = min(score, 60)
    if salary_score <= 40 and target_salary:
        score = min(score, 50 if salary_strict else 70)
    if city_score < 60 and city_strict and _split_items(preferences.get("target_cities", [])):
        score = min(score, 55)
    if overall_score < 55:
        score = min(score, 70)
    return _clamp_score(score)
