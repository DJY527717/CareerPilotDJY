from typing import Any, Callable

import pandas as pd


def clear_runtime_data_cache(
    *,
    session_state: Any,
    cache_clear: Callable[[], None],
) -> None:
    try:
        cache_clear()
    except Exception:
        pass
    for key in ["report_excel_bytes", "report_pdf_bytes"]:
        session_state.pop(key, None)


def get_active_profile(
    *,
    session_state: Any,
    load_user_profiles: Callable[[], pd.DataFrame],
    repair_mojibake_text: Callable[[str], str],
) -> dict[str, Any]:
    profiles = load_user_profiles()
    if profiles.empty:
        return {"id": None, "name": "未设置", "content": "", "is_default": 1}

    active_id = session_state.get("active_profile_id")
    if active_id is None or active_id not in profiles["id"].tolist():
        default_rows = profiles[profiles["is_default"] == 1]
        active_row = default_rows.iloc[0] if not default_rows.empty else profiles.iloc[0]
        session_state.active_profile_id = int(active_row["id"])
    else:
        active_row = profiles[profiles["id"] == active_id].iloc[0]

    return {
        "id": int(active_row["id"]),
        "name": repair_mojibake_text(str(active_row["name"])),
        "content": repair_mojibake_text(str(active_row["content"])),
        "is_default": int(active_row["is_default"]),
    }


def profile_text_for_analysis(
    *,
    get_active_profile: Callable[[], dict[str, Any]],
    target_preferences_text: Callable[[], str],
    normalize_text: Callable[[str], str],
) -> str:
    active = get_active_profile()
    return normalize_text(active.get("content", "") + "\n" + target_preferences_text())


def get_active_resume(
    *,
    session_state: Any,
    load_user_resumes: Callable[[], pd.DataFrame],
) -> dict[str, Any]:
    resumes = load_user_resumes()
    if resumes.empty:
        return {"id": None, "name": "未设置简历", "content": "", "is_default": 1, "updated_at": ""}
    active_id = session_state.get("active_resume_id")
    if active_id is None or active_id not in resumes["id"].tolist():
        default_rows = resumes[resumes["is_default"] == 1]
        active_row = default_rows.iloc[0] if not default_rows.empty else resumes.iloc[0]
        session_state.active_resume_id = int(active_row["id"])
    else:
        active_row = resumes[resumes["id"] == active_id].iloc[0]
    return {
        "id": int(active_row["id"]),
        "name": str(active_row["name"]),
        "content": str(active_row["content"]),
        "is_default": int(active_row["is_default"]),
        "updated_at": str(active_row.get("updated_at", "")),
    }


def resume_text_for_analysis(
    *,
    get_active_resume: Callable[[], dict[str, Any]],
) -> str:
    active_resume = get_active_resume()
    return str(active_resume.get("content") or "")


def current_resume_fingerprint(
    *,
    resume_text_for_analysis: Callable[[], str],
    content_fingerprint: Callable[[str], str],
) -> str:
    return content_fingerprint(resume_text_for_analysis())
