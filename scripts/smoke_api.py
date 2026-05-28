"""Smoke test for the active CareerPilot API vertical slice.

Run after starting the API server:
    .\.venv\Scripts\python.exe scripts\smoke_api.py
"""

from __future__ import annotations

import json
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


BASE_URL = "http://127.0.0.1:8765"
FORBIDDEN_TERMS = [
    "JY",
    "Dong",
    "djiuyi",
    "djiuyi0527",
    "djiuyi0527@gmail.com",
    r"C:\Users",
]


GET_CHECKS = {
    "/api/health": ["status", "version"],
    "/api/bootstrap": ["user", "workspaces", "jobs"],
    "/api/me": ["name", "resume_name", "target"],
    "/api/workspaces": [],
    "/api/settings/summary": ["user", "resume_status", "target_status"],
    "/api/jobs/ranked": [],
}

POST_CHECKS = {
    "/api/jd/analyze": [
        {
            "name": "Chinese complete JD",
            "text": (
                "公司名称：示例科技有限公司\n"
                "岗位名称：数据分析实习生\n"
                "工作地点：上海\n"
                "薪资：150-200元/天\n"
                "岗位职责：负责业务指标分析、SQL 取数、Python 数据清洗和看板建设。\n"
                "任职要求：本科及以上，每周至少 4 天，熟悉 Excel、SQL，有良好沟通协作能力。"
            ),
            "expected_terms": ["数据分析", "SQL"],
        },
        {
            "name": "Chinese short JD",
            "text": "运营实习生，协助活动执行。",
            "expected_terms": ["岗位描述较短"],
        },
        {
            "name": "English JD",
            "text": (
                "Company: Example Analytics Ltd\n"
                "Job Title: Business Analyst Intern\n"
                "Location: Remote\n"
                "Responsibilities: analyze business metrics, build dashboards, and communicate insights.\n"
                "Requirements: Bachelor degree, SQL, Excel, and strong communication skills."
            ),
            "expected_terms": ["Business Analyst Intern", "SQL"],
        },
        {
            "name": "Missing company",
            "text": (
                "岗位名称：产品助理\n"
                "工作地点：杭州\n"
                "岗位职责：支持用户研究、竞品分析和需求文档整理。\n"
                "任职要求：本科及以上，沟通协作能力良好。"
            ),
            "expected_terms": ["待确认公司"],
        },
        {
            "name": "Missing location",
            "text": (
                "公司名称：示例咨询有限公司\n"
                "岗位名称：项目助理\n"
                "岗位职责：支持资料收集、客户访谈和报告撰写。\n"
                "任职要求：本科及以上，具备结构化表达能力。"
            ),
            "expected_terms": ["待确认"],
        },
        {
            "name": "Empty text",
            "text": "",
            "expected_terms": ["未收到岗位描述文本"],
        },
        {
            "name": "JD with URL",
            "text": (
                "岗位名称：市场营销实习生\n"
                "公司名称：示例品牌有限公司\n"
                "详情链接：https://example.com/jobs/123?token=demo\n"
                "工作地点：广州\n"
                "岗位职责：支持渠道投放、Campaign 复盘和用户增长分析。"
            ),
            "expected_terms": ["市场营销"],
            "forbidden_terms": ["https://example.com", "token=demo"],
        },
    ],
}

SKELETON_POST_CHECKS = {
    "/api/settings/validate": {
        "fields": ["profile_status", "resume_status", "target_status", "missing_fields", "suggested_actions"],
        "list_fields": ["missing_fields", "suggested_actions"],
        "payloads": [
            {"profile_text": "通用档案信息", "resume_text": "通用简历文本", "target_text": "通用目标岗位"},
            {},
        ],
    },
    "/api/resume/parse": {
        "fields": ["summary", "sections", "keywords", "warnings", "scores"],
        "list_fields": ["keywords", "warnings", "scores"],
        "payloads": [
            {"text": "项目经历：负责 SQL 数据分析和看板建设。\n技能：Python、Excel、沟通协作。"},
            {"text": ""},
        ],
    },
    "/api/resume/match": {
        "fields": ["summary", "match_score", "matched_keywords", "missing_keywords", "risks", "suggested_actions"],
        "list_fields": ["matched_keywords", "missing_keywords", "risks", "suggested_actions"],
        "payloads": [
            {"resume_text": "具备 SQL 数据分析和项目协作经验。", "jd_text": "要求 SQL、数据分析、沟通协作。"},
            {},
        ],
    },
    "/api/decision/evaluate": {
        "fields": ["summary", "priority_score", "fit_score", "risk_score", "reasons", "suggested_actions"],
        "list_fields": ["reasons", "suggested_actions"],
        "payloads": [
            {"text": "岗位职责清晰，包含数据项目和成长空间。"},
            {},
        ],
    },
    "/api/report/interview-summary": {
        "fields": ["summary", "highlights", "risks", "follow_up_actions", "scores"],
        "list_fields": ["highlights", "risks", "follow_up_actions", "scores"],
        "payloads": [
            {"text": "面试问题：请介绍一个数据项目。回答：说明指标、行动和结果。"},
            {},
        ],
    },
}


def request_json(path: str, payload: dict[str, Any] | None = None) -> tuple[str, Any]:
    url = BASE_URL + path
    try:
        if payload is None:
            with urlopen(url, timeout=10) as response:
                raw = response.read().decode("utf-8")
        else:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=10) as response:
                raw = response.read().decode("utf-8")
    except URLError as exc:
        raise SystemExit(f"{path} request failed: {exc}") from exc

    for term in FORBIDDEN_TERMS:
        if term in raw:
            raise SystemExit(f"{path} contains forbidden term: {term}")

    try:
        envelope = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"{path} did not return valid JSON: {exc}") from exc

    if envelope.get("ok") is not True:
        raise SystemExit(f"{path} did not return ok: true")
    if "data" not in envelope:
        raise SystemExit(f"{path} missing data field")
    return raw, envelope["data"]


def assert_object_fields(path: str, data: Any, fields: list[str]) -> None:
    if not isinstance(data, dict):
        raise SystemExit(f"{path} expected object data")
    missing = [field for field in fields if field not in data]
    if missing:
        raise SystemExit(f"{path} missing fields: {', '.join(missing)}")


def assert_non_empty_list_item(path: str, data: Any, fields: list[str]) -> None:
    if not isinstance(data, list) or not data:
        raise SystemExit(f"{path} expected non-empty list data")
    first = data[0]
    if not isinstance(first, dict):
        raise SystemExit(f"{path} expected object list items")
    missing = [field for field in fields if field not in first]
    if missing:
        raise SystemExit(f"{path} first item missing fields: {', '.join(missing)}")


def assert_jd_analysis(case_name: str, data: Any, expected_terms: list[str] | None = None, forbidden_terms: list[str] | None = None) -> None:
    assert_object_fields(
        f"/api/jd/analyze ({case_name})",
        data,
        ["summary", "input_length", "requirements", "keywords", "risks", "suggested_actions", "recommended_job", "scores"],
    )
    if not isinstance(data.get("summary"), str):
        raise SystemExit(f"/api/jd/analyze ({case_name}) expected summary string")
    if not isinstance(data.get("input_length"), int):
        raise SystemExit(f"/api/jd/analyze ({case_name}) expected numeric input_length")
    for field in ["requirements", "keywords", "risks", "suggested_actions", "scores"]:
        if not isinstance(data.get(field), list) or not data[field]:
            raise SystemExit(f"/api/jd/analyze ({case_name}) expected non-empty list field: {field}")
    job = data.get("recommended_job")
    if not isinstance(job, dict):
        raise SystemExit(f"/api/jd/analyze ({case_name}) expected recommended_job object")
    missing_job_fields = [field for field in ["id", "title", "company", "location", "track", "match_score", "salary", "highlights"] if field not in job]
    if missing_job_fields:
        raise SystemExit(f"/api/jd/analyze ({case_name}) recommended_job missing fields: {', '.join(missing_job_fields)}")
    if not isinstance(job.get("highlights"), list):
        raise SystemExit(f"/api/jd/analyze ({case_name}) expected recommended_job.highlights list")
    raw = json.dumps(data, ensure_ascii=False)
    for term in expected_terms or []:
        if term not in raw:
            raise SystemExit(f"/api/jd/analyze ({case_name}) did not surface expected term: {term}")
    for term in forbidden_terms or []:
        if term in raw:
            raise SystemExit(f"/api/jd/analyze ({case_name}) leaked forbidden term: {term}")


def assert_skeleton_response(path: str, data: Any, fields: list[str], list_fields: list[str]) -> None:
    assert_object_fields(path, data, fields)
    for field in list_fields:
        if not isinstance(data.get(field), list):
            raise SystemExit(f"{path} expected list field: {field}")
    raw = json.dumps(data, ensure_ascii=False)
    for term in FORBIDDEN_TERMS:
        if term in raw:
            raise SystemExit(f"{path} contains forbidden term: {term}")


def main() -> None:
    for path, fields in GET_CHECKS.items():
        _, data = request_json(path)
        if path == "/api/workspaces":
            assert_non_empty_list_item(path, data, ["key", "label", "title", "modes"])
        elif path == "/api/jobs/ranked":
            assert_non_empty_list_item(path, data, ["id", "title", "company", "match_score"])
        else:
            assert_object_fields(path, data, fields)

    for path, cases in POST_CHECKS.items():
        for case in cases:
            _, data = request_json(path, {"text": case["text"]})
            assert_jd_analysis(
                str(case["name"]),
                data,
                expected_terms=case.get("expected_terms"),
                forbidden_terms=case.get("forbidden_terms"),
            )

    for path, spec in SKELETON_POST_CHECKS.items():
        for payload in spec["payloads"]:
            _, data = request_json(path, payload)
            assert_skeleton_response(path, data, spec["fields"], spec["list_fields"])

    print("API smoke test passed.")


if __name__ == "__main__":
    main()
