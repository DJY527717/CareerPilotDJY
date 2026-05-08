from datetime import datetime
from typing import Any, Callable

import pandas as pd


def load_user_profiles_df(
    *,
    init_db: Callable[[], None],
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
    db_read_sql_query: Callable[[str, Any, tuple[Any, ...]], pd.DataFrame],
    repair_dataframe_text: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    init_db()
    user_id = require_user_id()
    with db_connect() as conn:
        df = db_read_sql_query(
            "SELECT * FROM user_profiles WHERE user_id = ? ORDER BY is_default DESC, id ASC",
            conn,
            (user_id,),
        )
    return repair_dataframe_text(df)


def save_user_profile_row(
    profile_id: int,
    name: str,
    content: str,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> None:
    user_id = require_user_id()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE user_profiles
            SET name = ?, content = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
            """,
            (name.strip(), content.strip(), now, profile_id, user_id),
        )


def create_user_profile_row(
    name: str,
    content: str,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
    db_insert_and_get_id: Callable[[Any, str, dict[str, Any]], int],
) -> int:
    user_id = require_user_id()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        is_default = 0 if int(bool(conn.execute("SELECT COUNT(*) FROM user_profiles WHERE user_id = ?", (user_id,)).fetchone()[0])) else 1
        return db_insert_and_get_id(
            conn,
            "user_profiles",
            {
                "user_id": user_id,
                "name": name.strip(),
                "content": content.strip(),
                "is_default": is_default,
                "created_at": now,
                "updated_at": now,
            },
        )


def delete_user_profile_row(
    profile_id: int,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> bool:
    user_id = require_user_id()
    with db_connect() as conn:
        conn.execute("DELETE FROM user_profiles WHERE id = ? AND user_id = ?", (profile_id, user_id))
    return True


def load_user_resumes_df(
    *,
    init_db: Callable[[], None],
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
    db_read_sql_query: Callable[[str, Any, tuple[Any, ...]], pd.DataFrame],
    repair_dataframe_text: Callable[[pd.DataFrame], pd.DataFrame],
    is_legacy_template_resume: Callable[[str, str], bool],
) -> pd.DataFrame:
    init_db()
    user_id = require_user_id()
    with db_connect() as conn:
        df = db_read_sql_query(
            "SELECT * FROM user_resumes WHERE user_id = ? ORDER BY is_default DESC, id ASC",
            conn,
            (user_id,),
        )
    df = repair_dataframe_text(df)
    if df.empty:
        return df
    template_mask = df.apply(
        lambda row: is_legacy_template_resume(str(row.get("name", "")), str(row.get("content", ""))),
        axis=1,
    )
    return df.loc[~template_mask].reset_index(drop=True)


def create_user_resume_row(
    name: str,
    content: str,
    *,
    is_default: int,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
    db_insert_and_get_id: Callable[[Any, str, dict[str, Any]], int],
) -> int:
    user_id = require_user_id()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        if not conn.execute("SELECT COUNT(*) FROM user_resumes WHERE user_id = ?", (user_id,)).fetchone()[0]:
            is_default = 1
        return db_insert_and_get_id(
            conn,
            "user_resumes",
            {
                "user_id": user_id,
                "name": name.strip(),
                "content": content.strip(),
                "is_default": int(is_default),
                "created_at": now,
                "updated_at": now,
            },
        )


def save_user_resume_row(
    resume_id: int,
    name: str,
    content: str,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> None:
    user_id = require_user_id()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_connect() as conn:
        conn.execute(
            """
            UPDATE user_resumes
            SET name = ?, content = ?, updated_at = ?
            WHERE id = ? AND user_id = ?
            """,
            (name.strip(), content.strip(), now, resume_id, user_id),
        )


def delete_user_resume_row(
    resume_id: int,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> bool:
    user_id = require_user_id()
    with db_connect() as conn:
        resume_count = conn.execute("SELECT COUNT(*) FROM user_resumes WHERE user_id = ?", (user_id,)).fetchone()[0]
        if resume_count <= 1:
            return False
        conn.execute("DELETE FROM user_resumes WHERE id = ? AND user_id = ?", (resume_id, user_id))
    return True


def add_application_row(
    record: dict[str, Any],
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> None:
    user_id = require_user_id()
    with db_connect() as conn:
        conn.execute(
            """
            INSERT INTO applications (
                user_id, company, job_title, salary, location, category, match_score,
                is_high_value, is_generic_esg, applied, interview_status,
                offer_status, notes, queue_date, next_action, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                record.get("company", ""),
                record.get("job_title", ""),
                record.get("salary", ""),
                record.get("location", ""),
                record.get("category", ""),
                int(record.get("match_score", 0) or 0),
                int(bool(record.get("is_high_value", False))),
                int(bool(record.get("is_generic_esg", False))),
                int(bool(record.get("applied", False))),
                record.get("interview_status", "未开始"),
                record.get("offer_status", "无"),
                record.get("notes", ""),
                record.get("queue_date", ""),
                record.get("next_action", ""),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )


def load_applications_df(
    *,
    init_db: Callable[[], None],
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
    db_read_sql_query: Callable[[str, Any, tuple[Any, ...]], pd.DataFrame],
    repair_dataframe_text: Callable[[pd.DataFrame], pd.DataFrame],
) -> pd.DataFrame:
    init_db()
    user_id = require_user_id()
    with db_connect() as conn:
        df = db_read_sql_query("SELECT * FROM applications WHERE user_id = ? ORDER BY id DESC", conn, (user_id,))
    df = repair_dataframe_text(df)
    if not df.empty:
        for col in ["is_high_value", "is_generic_esg", "applied"]:
            df[col] = df[col].astype(bool)
    return df


def save_application_edits_df(
    df: pd.DataFrame,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> None:
    if df.empty:
        return
    user_id = require_user_id()
    with db_connect() as conn:
        for _, row in df.iterrows():
            conn.execute(
                """
                UPDATE applications
                SET company=?, job_title=?, salary=?, location=?, category=?,
                    match_score=?, is_high_value=?, is_generic_esg=?, applied=?,
                    interview_status=?, offer_status=?, notes=?, queue_date=?, next_action=?
                WHERE id=? AND user_id=?
                """,
                (
                    str(row.get("company", "")),
                    str(row.get("job_title", "")),
                    str(row.get("salary", "")),
                    str(row.get("location", "")),
                    str(row.get("category", "")),
                    int(row.get("match_score", 0) or 0),
                    int(bool(row.get("is_high_value", False))),
                    int(bool(row.get("is_generic_esg", False))),
                    int(bool(row.get("applied", False))),
                    str(row.get("interview_status", "未开始")),
                    str(row.get("offer_status", "无")),
                    str(row.get("notes", "")),
                    str(row.get("queue_date", "")),
                    str(row.get("next_action", "")),
                    int(row["id"]),
                    user_id,
                ),
            )


def delete_application_row(
    row_id: int,
    *,
    require_user_id: Callable[[], int],
    db_connect: Callable[[], Any],
) -> None:
    user_id = require_user_id()
    with db_connect() as conn:
        conn.execute("DELETE FROM applications WHERE id = ? AND user_id = ?", (row_id, user_id))
