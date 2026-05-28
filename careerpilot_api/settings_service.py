"""Settings validation service for the Web API skeleton."""

from __future__ import annotations

from typing import Any

from careerpilot_api.schemas import ActionItemSchema


def validate_settings_for_api(payload: dict[str, Any]) -> dict[str, object]:
    text = _payload_text(payload, ["profile_text", "resume_text", "target_text", "text"])
    missing_fields: list[str] = []
    if not _has_value(payload, "profile_text") and len(text) < 20:
        missing_fields.append("profile")
    if not _has_value(payload, "resume_text"):
        missing_fields.append("resume")
    if not _has_value(payload, "target_text"):
        missing_fields.append("target")

    return {
        "profile_status": "待确认" if "profile" in missing_fields else "已提供基础信息",
        "resume_status": "尚未选择简历" if "resume" in missing_fields else "已提供简历文本",
        "target_status": "尚未设置求职目标" if "target" in missing_fields else "已提供目标信息",
        "missing_fields": missing_fields,
        "suggested_actions": _actions_for_missing_fields(missing_fields),
    }


def _actions_for_missing_fields(missing_fields: list[str]) -> list[ActionItemSchema]:
    actions: list[ActionItemSchema] = []
    if "profile" in missing_fields:
        actions.append(ActionItemSchema(id="settings-profile", title="补充基础档案", detail="填写通用档案信息，便于后续模块使用。", priority="中"))
    if "resume" in missing_fields:
        actions.append(ActionItemSchema(id="settings-resume", title="提供简历文本", detail="添加简历文本后，可进入简历解析和匹配流程。", priority="高"))
    if "target" in missing_fields:
        actions.append(ActionItemSchema(id="settings-target", title="确认求职目标", detail="补充目标岗位、方向或基本偏好。", priority="高"))
    return actions or [
        ActionItemSchema(id="settings-review", title="检查资料一致性", detail="确认档案、简历和目标信息是否互相一致。", priority="中")
    ]


def _payload_text(payload: dict[str, Any], keys: list[str]) -> str:
    return " ".join(str(payload.get(key) or "").strip() for key in keys).strip()


def _has_value(payload: dict[str, Any], key: str) -> bool:
    return bool(str(payload.get(key) or "").strip())
