from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_UPLOAD_URL = "https://capture.example.com/api/capture-upload"
os.environ["UPLOAD_API_PUBLIC_URL"] = PUBLIC_UPLOAD_URL
sys.path.insert(0, str(ROOT))

import app


NODE_CANDIDATES = [
    "node",
    str(Path.home() / ".cache" / "codex-runtimes" / "codex-primary-runtime" / "dependencies" / "node" / "bin" / "node.exe"),
]


def assert_true(value, message: str) -> None:
    if not value:
        raise AssertionError(message)


def assert_equal(actual, expected, message: str) -> None:
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def reset_app_db(db_path: Path) -> None:
    app.DB_PATH = db_path
    app.DATABASE_URL = ""
    app.USE_POSTGRES = False
    app._DB_INIT_DONE = False


def write_payload_and_parse(payload: dict, name: str) -> list[dict[str, str]]:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / f"{name}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return app.records_from_exported_jd_file(path)


def base_capture_job(**overrides) -> dict:
    job = {
        "schemaVersion": "careerpilot.capture.v1",
        "captureMode": "list",
        "sourceSite": "job51",
        "title": "数据分析师",
        "company": "上海测试科技有限公司",
        "salary": "10-15万/年",
        "location": "上海",
        "experience": "3年经验",
        "education": "本科",
        "url": "https://jobs.example.com/job/1",
        "detailUrl": "https://jobs.example.com/job/1",
        "sourceUrl": "https://jobs.example.com/list",
        "text": "岗位：数据分析师\n公司：上海测试科技有限公司\n薪资：10-15万/年\n职责：负责数据分析、指标体系和经营报表建设。",
        "detailText": "",
        "rawText": "数据分析师 上海测试科技有限公司 10-15万/年 上海 本科 3年经验 数据分析 指标体系 经营报表",
        "detailFetched": False,
        "capturedAt": "2026-05-20T10:00:00.000Z",
    }
    job.update(overrides)
    return job


def test_public_upload_url() -> None:
    assert_equal(app.capture_upload_public_url(), PUBLIC_UPLOAD_URL, "UPLOAD_API_PUBLIC_URL should win")
    assert_true("localhost" not in app.capture_upload_public_url().lower(), "public upload url should not be localhost")
    assert_true("127.0.0.1" not in app.capture_upload_public_url(), "public upload url should not be loopback")


def test_capture_token_stable_and_lookup_sqlite() -> None:
    old_db_path = app.DB_PATH
    old_database_url = app.DATABASE_URL
    old_use_postgres = app.USE_POSTGRES
    old_init_done = app._DB_INIT_DONE
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        reset_app_db(Path(tmpdir) / "careerpilot_test.db")
        try:
            token_a = app.get_or_create_capture_upload_token(1001)
            token_b = app.get_or_create_capture_upload_token(1001)
            assert_true(token_a, "token should be created")
            assert_equal(token_a, token_b, "token should be stable for same user")
            assert_equal(app.lookup_user_id_by_capture_token(token_a), 1001, "lookup should find sqlite user by token")
        finally:
            app.DB_PATH = old_db_path
            app.DATABASE_URL = old_database_url
            app.USE_POSTGRES = old_use_postgres
            app._DB_INIT_DONE = old_init_done


def test_lookup_placeholder_generation_for_postgres() -> None:
    old_use_postgres = app.USE_POSTGRES
    try:
        app.USE_POSTGRES = False
        sqlite_sql = app.db_sql("SELECT user_id FROM app_settings WHERE key = ? AND value = ?")
        app.USE_POSTGRES = True
        postgres_sql = app.db_sql("SELECT user_id FROM app_settings WHERE key = ? AND value = ?")
    finally:
        app.USE_POSTGRES = old_use_postgres
    assert_equal(sqlite_sql, "SELECT user_id FROM app_settings WHERE key = ? AND value = ?", "sqlite placeholders should stay as question marks")
    assert_equal(postgres_sql, "SELECT user_id FROM app_settings WHERE key = %s AND value = %s", "postgres placeholders should use percent-s")


def test_save_capture_payload_does_not_overwrite_same_title() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        old_root = app.cloud_upload_root_for_user
        app.cloud_upload_root_for_user = lambda user_id: Path(tmpdir) / f"user_{int(user_id)}"
        try:
            payload = {"title": "同名采集", "url": "https://jobs.example.com/list", "jobs": [base_capture_job()]}
            first = app.save_capture_payload_for_user(1001, payload, "bookmarklet")
            second = app.save_capture_payload_for_user(1001, payload, "bookmarklet")
        finally:
            app.cloud_upload_root_for_user = old_root
        assert_true(first.exists(), "first payload should be saved")
        assert_true(second.exists(), "second payload should be saved")
        assert_true(first != second, "same-title payload saves should not overwrite")


def test_list_payload_parses_job() -> None:
    payload = {
        "schemaVersion": "careerpilot.capture.v1",
        "type": "list_paginated_with_details",
        "captureMode": "list",
        "sourceUrl": "https://jobs.example.com/list",
        "jobs": [base_capture_job()],
    }
    records = write_payload_and_parse(payload, "list_payload")
    assert_equal(len(records), 1, "list payload should parse one job")
    assert_equal(records[0]["title"], "数据分析师", "list payload title should parse")


def test_detail_payload_parses_job() -> None:
    payload = {
        **base_capture_job(
            captureMode="detail",
            title="高级数据分析师",
            detailFetched=True,
            detailText="岗位职责：负责用户增长分析、实验评估和经营指标体系建设。任职要求：熟悉 SQL、Python 和统计分析。",
            text="岗位：高级数据分析师\n公司：上海测试科技有限公司\n职责：负责用户增长分析、实验评估和经营指标体系建设。",
        ),
        "type": "detail",
    }
    records = write_payload_and_parse(payload, "detail_payload")
    assert_equal(len(records), 1, "detail payload should parse one job")
    assert_equal(records[0]["title"], "高级数据分析师", "detail payload title should parse")


def test_legacy_bookmarklet_payload_parses_job() -> None:
    payload = {
        "type": "bookmarklet_fallback_list",
        "title": "旧书签采集",
        "url": "https://jobs.example.com/list",
        "jobs": [
            {
                "title": "运营数据分析师",
                "company": "广州旧版科技有限公司",
                "location": "广州",
                "text": "岗位：运营数据分析师\n公司：广州旧版科技有限公司\n地点：广州\n职责：负责运营数据看板、活动复盘和用户行为分析。",
                "url": "",
            }
        ],
    }
    records = write_payload_and_parse(payload, "legacy_payload")
    assert_equal(len(records), 1, "legacy payload without schemaVersion should parse")
    assert_equal(records[0]["url"], "", "legacy url-less payload should remain url-less")


def test_failed_detail_limit_payload_keeps_card() -> None:
    payload = {
        "schemaVersion": "careerpilot.capture.v1",
        "type": "list_paginated_with_details",
        "captureMode": "list",
        "detailRequired": True,
        "detailCount": 0,
        "sourceUrl": "https://jobs.example.com/list",
        "jobs": [
            base_capture_job(
                title="风控数据分析师",
                salary="",
                detailFetched=False,
                detailText="",
                text="岗位：风控数据分析师\n公司：上海测试科技有限公司\n地点：上海\n职责：负责风险策略分析、监控报表和模型效果评估。",
            )
        ],
    }
    records = write_payload_and_parse(payload, "failed_detail_payload")
    assert_equal(len(records), 1, "failed detail should keep original job card")
    assert_equal(records[0]["title"], "风控数据分析师", "failed detail title should survive")
    assert_equal(records[0]["salary"], "", "missing salary should stay empty")


def run_node_check(path: str) -> None:
    last_error = ""
    for node in NODE_CANDIDATES:
        try:
            result = subprocess.run(
                [node, "--check", path],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
                timeout=30,
            )
        except OSError as exc:
            last_error = str(exc)
            continue
        if result.returncode == 0:
            return
        last_error = f"stdout={result.stdout}\nstderr={result.stderr}"
    raise AssertionError(f"node --check failed for {path}: {last_error}")


def run_python_compile(path: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", "py_compile", path],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        timeout=60,
    )
    if result.returncode != 0:
        raise AssertionError(f"py_compile failed for {path}\nstdout={result.stdout}\nstderr={result.stderr}")


def test_static_checks() -> None:
    run_node_check("careerpilot_capture_core.js")
    run_node_check("browser_extension/capture_core.js")
    run_python_compile("app.py")
    run_python_compile("careerpilot/capture.py")


def main() -> None:
    tests = [
        test_public_upload_url,
        test_capture_token_stable_and_lookup_sqlite,
        test_lookup_placeholder_generation_for_postgres,
        test_save_capture_payload_does_not_overwrite_same_title,
        test_list_payload_parses_job,
        test_detail_payload_parses_job,
        test_legacy_bookmarklet_payload_parses_job,
        test_failed_detail_limit_payload_keeps_card,
        test_static_checks,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    main()
