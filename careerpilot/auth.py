from datetime import datetime
from typing import Any, Callable


def current_user_id(session_state: Any, auth_session_key: str) -> int | None:
    user = session_state.get(auth_session_key)
    if isinstance(user, dict) and user.get("id"):
        return int(user["id"])
    return None


def require_user_id(session_state: Any, auth_session_key: str) -> int:
    user_id = current_user_id(session_state, auth_session_key)
    if not user_id:
        raise RuntimeError("用户未登录")
    return int(user_id)


def normalize_email(email: str, *, normalize_text: Callable[[str], str]) -> str:
    return normalize_text(email).strip().lower()


def create_app_user(
    email: str,
    password: str,
    display_name: str,
    *,
    session_state: Any,
    auth_session_key: str,
    init_db: Callable[[], None],
    normalize_text: Callable[[str], str],
    normalize_email_fn: Callable[[str], str],
    db_connect: Callable[[], Any],
    db_insert_and_get_id: Callable[[Any, str, dict[str, Any]], int],
    password_hash: Callable[[str], str],
    db_integrity_errors: Callable[[], tuple[type[BaseException], ...]],
    clear_runtime_data_cache: Callable[[], None],
) -> tuple[bool, str]:
    init_db()
    email = normalize_email_fn(email)
    display_name = normalize_text(display_name) or email.split("@")[0]
    if not email or "@" not in email:
        return False, "请输入有效邮箱。"
    if len(password) < 8:
        return False, "密码至少 8 位。"
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        with db_connect() as conn:
            user_id = db_insert_and_get_id(
                conn,
                "app_users",
                {
                    "email": email,
                    "password_hash": password_hash(password),
                    "display_name": display_name,
                    "created_at": now,
                    "last_login_at": now,
                },
            )
        session_state[auth_session_key] = {"id": user_id, "email": email, "display_name": display_name}
        clear_runtime_data_cache()
        return True, "注册成功。"
    except db_integrity_errors():
        return False, "该邮箱已注册，请直接登录。"


def authenticate_app_user(
    email: str,
    password: str,
    *,
    session_state: Any,
    auth_session_key: str,
    init_db: Callable[[], None],
    normalize_email_fn: Callable[[str], str],
    db_connect: Callable[[], Any],
    verify_password: Callable[[str, str], bool],
    clear_runtime_data_cache: Callable[[], None],
) -> tuple[bool, str]:
    init_db()
    email = normalize_email_fn(email)
    with db_connect() as conn:
        row = conn.execute(
            "SELECT id, email, password_hash, display_name FROM app_users WHERE email = ?",
            (email,),
        ).fetchone()
        if not row or not verify_password(password, str(row[2])):
            return False, "邮箱或密码不正确。"
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE app_users SET last_login_at = ? WHERE id = ?", (now, int(row[0])))
    session_state[auth_session_key] = {"id": int(row[0]), "email": str(row[1]), "display_name": str(row[3] or row[1])}
    clear_runtime_data_cache()
    return True, "登录成功。"


def logout_app_user(
    *,
    session_state: Any,
    auth_session_key: str,
    clear_runtime_data_cache: Callable[[], None],
) -> None:
    session_state.pop(auth_session_key, None)
    for key in ["active_profile_id", "active_resume_id"]:
        session_state.pop(key, None)
    clear_runtime_data_cache()
