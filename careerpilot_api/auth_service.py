"""Mock authentication and workspace contracts for CareerPilot.

This module intentionally does not persist passwords, connect databases, or
call external identity providers. It only defines the API shape that the Web
client and future production services should depend on.
"""

from __future__ import annotations

from typing import Any


JsonDict = dict[str, Any]

DEMO_USER_ID = "demo-user"
DEMO_WORKSPACE_ID = "demo-workspace"
DEMO_SESSION_ID = "demo-session"
DEMO_EMAIL = "demo@example.invalid"
DEMO_DISPLAY_NAME = "Demo user"
DEMO_TIMESTAMP = ""


def _text(value: Any, fallback: str = "") -> str:
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def _role(value: Any) -> str:
    return value if value in {"owner", "admin", "member", "viewer"} else "member"


def _public_user(payload: JsonDict | None = None) -> JsonDict:
    payload = payload or {}
    role = _role(payload.get("role"))
    display_name = _text(payload.get("display_name"), DEMO_DISPLAY_NAME)
    email = _text(payload.get("email"), DEMO_EMAIL)
    return {
        "user_id": DEMO_USER_ID,
        "email": email,
        "display_name": display_name,
        "role": role,
        "is_admin": role in {"owner", "admin"},
        "default_workspace_id": DEMO_WORKSPACE_ID,
    }


def _workspace(role: str = "owner") -> JsonDict:
    return {
        "workspace_id": DEMO_WORKSPACE_ID,
        "owner_user_id": DEMO_USER_ID,
        "name": "Demo workspace",
        "role": role,
        "created_at": DEMO_TIMESTAMP,
        "updated_at": DEMO_TIMESTAMP,
    }


def _session(user: JsonDict | None = None, authenticated: bool = True) -> JsonDict:
    return {
        "authenticated": authenticated,
        "session_id": DEMO_SESSION_ID if authenticated else "",
        "user": user if authenticated else None,
        "selected_workspace_id": DEMO_WORKSPACE_ID if authenticated else "",
        "expires_at": DEMO_TIMESTAMP,
    }


def _auth_response(user: JsonDict | None = None) -> JsonDict:
    user = user or _public_user()
    return {
        "session": _session(user),
        "user": user,
        "workspaces": [_workspace()],
        "auth_mode": "mock",
    }


def register_for_api(payload: JsonDict) -> JsonDict:
    """Return a mock registered session without storing any password value."""

    email = _text(payload.get("email"), DEMO_EMAIL)
    display_name = _text(payload.get("display_name"), DEMO_DISPLAY_NAME)
    password = payload.get("password")
    if "@" not in email:
        email = DEMO_EMAIL
    if not isinstance(password, str) or len(password) < 8:
        return {
            "session": _session(authenticated=False),
            "user": None,
            "workspaces": [],
            "auth_mode": "mock",
            "validation_errors": ["password must be at least 8 characters in the mock contract"],
        }
    return _auth_response(_public_user({"email": email, "display_name": display_name, "role": "member"}))


def login_for_api(payload: JsonDict) -> JsonDict:
    """Return a mock login session without comparing or storing a password."""

    email = _text(payload.get("email"), DEMO_EMAIL)
    password = payload.get("password")
    if not isinstance(password, str) or not password:
        return {
            "session": _session(authenticated=False),
            "user": None,
            "workspaces": [],
            "auth_mode": "mock",
            "validation_errors": ["password is required in the mock contract"],
        }
    return _auth_response(_public_user({"email": email, "display_name": DEMO_DISPLAY_NAME, "role": "member"}))


def logout_for_api(payload: JsonDict | None = None) -> JsonDict:
    return {
        "session": _session(authenticated=False),
        "user": None,
        "workspaces": [],
        "auth_mode": "mock",
    }


def get_auth_session_for_api() -> JsonDict:
    return _session(_public_user())


def get_me_for_api() -> JsonDict:
    return _public_user()


def get_workspaces_for_api() -> list[JsonDict]:
    return [_workspace()]


def select_workspace_for_api(payload: JsonDict) -> JsonDict:
    requested_workspace_id = _text(payload.get("workspace_id"), DEMO_WORKSPACE_ID)
    workspace = _workspace()
    if requested_workspace_id != DEMO_WORKSPACE_ID:
        requested_workspace_id = DEMO_WORKSPACE_ID
    return {
        "selected_workspace_id": requested_workspace_id,
        "workspace": workspace,
        "selection_mode": "mock",
    }
