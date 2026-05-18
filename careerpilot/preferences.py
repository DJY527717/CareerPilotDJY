import json
from typing import Any, Callable


def load_target_preferences_from_json(
    raw: str,
    *,
    default_preferences: dict[str, Any],
    legacy_auto_target_cities: list[str],
    legacy_auto_target_industries: list[str],
    legacy_auto_avoid_keywords: str,
    legacy_auto_notes: str,
    split_preference_items: Callable[[Any], list[str]],
    normalize_industry_direction_selection: Callable[[list[str] | Any, list[str] | Any], tuple[list[str], list[str]]],
) -> dict[str, Any]:
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            data = {}
    except Exception:
        data = {}

    merged = dict(default_preferences)
    merged.update(data)
    merged["target_roles"] = split_preference_items(
        split_preference_items(merged.get("target_roles", []))
        + split_preference_items(merged.get("target_directions", []))
        + split_preference_items(merged.get("preferred_roles", []))
        + split_preference_items(merged.get("job_directions", []))
    )
    merged["target_cities"] = split_preference_items(
        split_preference_items(merged.get("target_cities", [])) + split_preference_items(merged.get("extra_cities", ""))
    )
    merged["preferred_industries"] = split_preference_items(
        split_preference_items(merged.get("preferred_industries", []))
        + split_preference_items(merged.get("target_industries", []))
        + split_preference_items(merged.get("industries", []))
        + split_preference_items(merged.get("extra_industries", ""))
    )
    merged["preferred_industries"], merged["target_roles"] = normalize_industry_direction_selection(
        merged.get("preferred_industries", []),
        merged.get("target_roles", []),
    )
    merged["job_keywords"] = split_preference_items(
        split_preference_items(merged.get("job_keywords", []))
        + split_preference_items(merged.get("positive_keywords", []))
        + split_preference_items(merged.get("preferred_keywords", []))
    )
    if not str(merged.get("avoid_keywords", "")).strip():
        merged["avoid_keywords"] = "、".join(
            split_preference_items(merged.get("avoid_terms", []))
            + split_preference_items(merged.get("negative_keywords", []))
        )
    if merged["target_cities"] == legacy_auto_target_cities:
        merged["target_cities"] = []
    if merged["preferred_industries"] == legacy_auto_target_industries:
        merged["preferred_industries"] = []
    if str(merged.get("avoid_keywords", "")).strip() == legacy_auto_avoid_keywords:
        merged["avoid_keywords"] = ""
    if str(merged.get("notes", "")).strip() == legacy_auto_notes:
        merged["notes"] = ""
    merged["extra_cities"] = ""
    merged["extra_industries"] = ""
    for key in ["min_monthly_salary", "max_monthly_salary", "min_daily_salary"]:
        try:
            merged[key] = int(merged.get(key) or default_preferences[key])
        except Exception:
            merged[key] = default_preferences[key]
    if isinstance(merged.get("target_salary"), dict):
        merged["target_salary_enabled"] = True
        target_salary = dict(merged["target_salary"])
        try:
            merged["min_monthly_salary"] = int(target_salary.get("min") or merged["min_monthly_salary"])
        except Exception:
            pass
        try:
            merged["max_monthly_salary"] = int(target_salary.get("max") or merged["max_monthly_salary"])
        except Exception:
            pass
        merged["salary_strict"] = bool(target_salary.get("strict"))
    merged["accept_remote"] = bool(merged.get("accept_remote"))
    merged["accept_nationwide"] = bool(merged.get("accept_nationwide"))
    merged["target_salary_enabled"] = bool(merged.get("target_salary_enabled"))
    merged["salary_strict"] = bool(merged.get("salary_strict"))
    merged["target_city_strict"] = bool(merged.get("target_city_strict"))
    if merged["target_salary_enabled"]:
        merged["target_salary"] = {
            "min": int(merged.get("min_monthly_salary") or 0) or None,
            "max": int(merged.get("max_monthly_salary") or 0) or None,
            "period": "monthly",
            "currency": "CNY",
            "strict": bool(merged.get("salary_strict")),
        }
    else:
        merged.pop("target_salary", None)
    return merged


def dump_target_preferences(
    preferences: dict[str, Any],
    *,
    default_preferences: dict[str, Any],
    split_preference_items: Callable[[Any], list[str]],
    normalize_industry_direction_selection: Callable[[list[str] | Any, list[str] | Any], tuple[list[str], list[str]]],
) -> str:
    clean = dict(default_preferences)
    clean.update(preferences)
    clean["target_roles"] = split_preference_items(clean.get("target_roles", []))
    clean["target_cities"] = split_preference_items(clean.get("target_cities", []))
    clean["preferred_industries"] = split_preference_items(clean.get("preferred_industries", []))
    clean["preferred_industries"], clean["target_roles"] = normalize_industry_direction_selection(
        clean.get("preferred_industries", []),
        clean.get("target_roles", []),
    )
    clean["job_keywords"] = split_preference_items(clean.get("job_keywords", []))
    clean["extra_cities"] = ""
    clean["extra_industries"] = ""
    clean["avoid_keywords"] = "、".join(split_preference_items(clean.get("avoid_keywords", "")))
    clean["notes"] = str(clean.get("notes", "")).strip()
    clean["min_monthly_salary"] = int(clean.get("min_monthly_salary") or default_preferences["min_monthly_salary"])
    clean["max_monthly_salary"] = int(clean.get("max_monthly_salary") or default_preferences["max_monthly_salary"])
    clean["min_daily_salary"] = int(clean.get("min_daily_salary") or default_preferences["min_daily_salary"])
    clean["accept_remote"] = bool(clean.get("accept_remote"))
    clean["accept_nationwide"] = bool(clean.get("accept_nationwide"))
    clean["target_salary_enabled"] = bool(clean.get("target_salary_enabled"))
    clean["salary_strict"] = bool(clean.get("salary_strict"))
    clean["target_city_strict"] = bool(clean.get("target_city_strict"))
    if clean["target_salary_enabled"]:
        clean["target_salary"] = {
            "min": clean["min_monthly_salary"],
            "max": clean["max_monthly_salary"] or None,
            "period": "monthly",
            "currency": "CNY",
            "strict": clean["salary_strict"],
        }
    else:
        clean.pop("target_salary", None)
    return json.dumps(clean, ensure_ascii=False)


def target_preferences_text(preferences: dict[str, Any], *, split_preference_items: Callable[[Any], list[str]]) -> str:
    city_items = split_preference_items(preferences.get("target_cities", []))
    industry_items = split_preference_items(preferences.get("preferred_industries", []))
    keyword_items = split_preference_items(preferences.get("job_keywords", []))
    lines = [
        "意向城市：" + (" / ".join(city_items) if city_items else "不限"),
        "目标行业：" + (" / ".join(industry_items) if industry_items else "不限"),
    ]
    role_items = split_preference_items(preferences.get("target_roles", []))
    if role_items:
        lines.append("二级方向：" + " / ".join(role_items))
    if keyword_items:
        lines.append("岗位关键词：" + " / ".join(keyword_items))
    if preferences.get("avoid_keywords"):
        lines.append("排除关键词：" + str(preferences.get("avoid_keywords")).strip())
    if preferences.get("target_salary_enabled"):
        salary = f"{int(preferences.get('min_monthly_salary') or 0)}"
        if int(preferences.get("max_monthly_salary") or 0):
            salary += f"-{int(preferences.get('max_monthly_salary') or 0)}"
        lines.append("目标月薪：" + salary + " 元")
    return "\n".join(lines)


def compact_list_text(
    items: list[str],
    *,
    split_preference_items: Callable[[Any], list[str]],
    empty: str = "未设置",
    limit: int = 3,
) -> str:
    clean_items = split_preference_items(items)
    if not clean_items:
        return empty
    label = " / ".join(clean_items[:limit])
    if len(clean_items) > limit:
        label += f" 等 {len(clean_items)} 项"
    return label


def compact_profile_summary(
    content: str,
    *,
    normalize_resume_lines: Callable[[str], list[str]],
    empty: str = "未设置",
    max_length: int = 72,
) -> str:
    lines = normalize_resume_lines(content)
    if not lines:
        return empty
    summary = lines[0]
    return summary[:max_length] + ("..." if len(summary) > max_length else "")
