import json
from datetime import datetime
from pathlib import Path
from typing import Any, Callable


def ensure_recruitment_region_columns(
    conn: Any,
    *,
    db_columns: Callable[[Any, str], set[str]],
) -> None:
    existing = db_columns(conn, "recruitment_posts")
    for column in ["standard_city", "province", "region", "region_priority"]:
        if column not in existing:
            conn.execute(f"ALTER TABLE recruitment_posts ADD COLUMN {column} TEXT")


def ensure_user_columns(
    conn: Any,
    *,
    db_columns: Callable[[Any, str], set[str]],
) -> None:
    for table in ["applications", "user_profiles", "user_resumes", "recruitment_posts", "app_settings"]:
        existing = db_columns(conn, table)
        if "user_id" not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN user_id INTEGER")


def ensure_application_queue_columns(
    conn: Any,
    *,
    db_columns: Callable[[Any, str], set[str]],
) -> None:
    existing = db_columns(conn, "applications")
    for column in ["queue_date", "next_action"]:
        if column not in existing:
            conn.execute(f"ALTER TABLE applications ADD COLUMN {column} TEXT")


def ensure_database_indexes(conn: Any) -> None:
    index_specs = [
        ("idx_app_settings_user_key", "app_settings", "user_id, key"),
        ("idx_recruitment_posts_user_fingerprint", "recruitment_posts", "user_id, fingerprint"),
        ("idx_recruitment_posts_user_last_seen", "recruitment_posts", "user_id, last_seen"),
        ("idx_user_profiles_user_default", "user_profiles", "user_id, is_default, id"),
        ("idx_user_resumes_user_default", "user_resumes", "user_id, is_default, id"),
        ("idx_applications_user_id", "applications", "user_id, id"),
    ]
    for index_name, table_name, columns in index_specs:
        conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns})")


def repair_default_profiles(
    conn: Any,
    *,
    looks_mojibake: Callable[[str], bool],
    default_target_intention_name: str,
) -> None:
    rows = conn.execute("SELECT id, name, content, is_default FROM user_profiles ORDER BY id ASC").fetchall()
    if not rows:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for profile_id, name, content, _is_default in rows:
        if looks_mojibake(str(name)) or looks_mojibake(str(content)):
            conn.execute(
                """
                UPDATE user_profiles
                SET name = ?, content = ?, updated_at = ?
                WHERE id = ?
                """,
                (default_target_intention_name, "", now, profile_id),
            )


def cleanup_legacy_database_state(
    conn: Any,
    *,
    use_postgres: bool,
    is_legacy_template_resume: Callable[[str, str], bool],
    repair_mojibake_text: Callable[[str], str],
    default_target_preferences: dict[str, Any],
    split_preference_items: Callable[[Any], list[str]],
    legacy_auto_target_cities: list[str],
    legacy_auto_target_industries: list[str],
    legacy_auto_avoid_keywords: str,
    legacy_auto_notes: str,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = conn.execute("SELECT id, name, content FROM user_resumes").fetchall()
    for resume_id, name, content in rows:
        if is_legacy_template_resume(repair_mojibake_text(str(name)), repair_mojibake_text(str(content))):
            conn.execute("DELETE FROM user_resumes WHERE id = ?", (resume_id,))

    if use_postgres:
        return

    setting_rows = conn.execute("SELECT rowid, value FROM app_settings WHERE user_id IS NULL AND key = 'target_preferences'").fetchall()
    for rowid, value in setting_rows:
        raw = repair_mojibake_text(str(value))
        try:
            data = json.loads(raw)
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        clean = dict(default_target_preferences)
        clean.update(data)
        clean["target_cities"] = split_preference_items(
            split_preference_items(clean.get("target_cities", [])) + split_preference_items(clean.get("extra_cities", ""))
        )
        clean["preferred_industries"] = split_preference_items(
            split_preference_items(clean.get("preferred_industries", [])) + split_preference_items(clean.get("extra_industries", ""))
        )
        if clean["target_cities"] == legacy_auto_target_cities:
            clean["target_cities"] = []
        if clean["preferred_industries"] == legacy_auto_target_industries:
            clean["preferred_industries"] = []
        if str(clean.get("avoid_keywords", "")).strip() == legacy_auto_avoid_keywords:
            clean["avoid_keywords"] = ""
        if str(clean.get("notes", "")).strip() == legacy_auto_notes:
            clean["notes"] = ""
        clean["extra_cities"] = ""
        clean["extra_industries"] = ""
        conn.execute(
            """
            UPDATE app_settings
            SET value = ?, updated_at = ?
            WHERE rowid = ?
            """,
            (json.dumps(clean, ensure_ascii=False), now, rowid),
        )


def init_db(
    *,
    session_state: Any,
    db_schema_version: str,
    use_postgres: bool,
    db_path: Path,
    db_autoincrement_pk: Callable[[], str],
    db_connect: Callable[[], Any],
    ensure_user_columns_fn: Callable[[Any], None],
    ensure_recruitment_region_columns_fn: Callable[[Any], None],
    ensure_application_queue_columns_fn: Callable[[Any], None],
    ensure_profile_table_ready_fn: Callable[[Any], None],
    cleanup_legacy_database_state_fn: Callable[[Any], None],
    ensure_database_indexes_fn: Callable[[Any], None],
) -> None:
    try:
        if session_state.get("_careerpilot_db_initialized") == db_schema_version and (use_postgres or db_path.exists()):
            return
    except Exception:
        pass
    pk_sql = db_autoincrement_pk()
    with db_connect() as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS app_users (
                id {pk_sql},
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                display_name TEXT,
                created_at TEXT,
                last_login_at TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS applications (
                id {pk_sql},
                user_id INTEGER,
                company TEXT,
                job_title TEXT,
                salary TEXT,
                location TEXT,
                category TEXT,
                match_score INTEGER,
                is_high_value INTEGER,
                is_generic_esg INTEGER,
                applied INTEGER DEFAULT 0,
                interview_status TEXT DEFAULT '未开始',
                offer_status TEXT DEFAULT '无',
                notes TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS user_profiles (
                id {pk_sql},
                user_id INTEGER,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                is_default INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS user_resumes (
                id {pk_sql},
                user_id INTEGER,
                name TEXT NOT NULL,
                content TEXT NOT NULL,
                is_default INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS recruitment_posts (
                id {pk_sql},
                user_id INTEGER,
                fingerprint TEXT NOT NULL,
                company TEXT,
                job_title TEXT,
                job_type TEXT,
                fresh_graduate TEXT,
                salary TEXT,
                location TEXT,
                standard_city TEXT,
                province TEXT,
                region TEXT,
                region_priority TEXT,
                education TEXT,
                experience TEXT,
                company_tier TEXT,
                category TEXT,
                high_value TEXT,
                generic_esg TEXT,
                skills TEXT,
                source TEXT,
                snippet TEXT,
                first_seen TEXT,
                last_seen TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                user_id INTEGER,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TEXT
            )
            """
        )
        ensure_user_columns_fn(conn)
        ensure_recruitment_region_columns_fn(conn)
        ensure_application_queue_columns_fn(conn)
        ensure_profile_table_ready_fn(conn)
        cleanup_legacy_database_state_fn(conn)
        ensure_database_indexes_fn(conn)
    try:
        session_state["_careerpilot_db_initialized"] = db_schema_version
    except Exception:
        pass
