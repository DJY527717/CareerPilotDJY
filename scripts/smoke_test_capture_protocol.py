from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app


def assert_true(value, message: str) -> None:
    if not value:
        raise AssertionError(message)


def records_from_payload(payload: dict, name: str):
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / f"{name}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        return app.records_from_exported_jd_file(path)


def base_job(**overrides):
    job = {
        "schemaVersion": "careerpilot.capture.v1",
        "captureMode": "list",
        "sourceSite": "job51",
        "title": "数据分析师",
        "company": "上海测试科技有限公司",
        "salary": "",
        "location": "上海",
        "experience": "3年经验",
        "education": "本科",
        "url": "https://jobs.example/job/1",
        "detailUrl": "https://jobs.example/job/1",
        "sourceUrl": "https://jobs.example/list",
        "text": "岗位：数据分析师\n公司：上海测试科技有限公司\n地点：上海\n职责：负责数据分析、指标建设和业务报表，支持经营决策。",
        "detailText": "",
        "rawText": "数据分析师 上海测试科技有限公司 上海 本科 3年经验 负责数据分析、指标建设和业务报表。",
        "detailFetched": False,
        "capturedAt": "2026-05-20T09:00:00.000Z",
    }
    job.update(overrides)
    return job


def test_single_detail_payload() -> None:
    payload = {
        **base_job(captureMode="detail", detailFetched=True),
        "type": "detail",
        "title": "高级数据分析师",
        "company": "上海测试科技有限公司",
        "url": "https://jobs.example/job/detail",
        "detailUrl": "https://jobs.example/job/detail",
        "detailText": "岗位职责：负责用户增长分析、实验评估和经营指标体系建设。任职要求：熟悉 SQL、Python 和统计分析。",
        "text": "岗位：高级数据分析师\n公司：上海测试科技有限公司\n详情：负责用户增长分析、实验评估和经营指标体系建设。",
    }
    records = records_from_payload(payload, "single_detail")
    assert_true(len(records) == 1, "single detail payload should parse one record")
    assert_true(records[0]["title"] == "高级数据分析师", "single detail title should parse")


def test_list_payload() -> None:
    payload = {
        "schemaVersion": "careerpilot.capture.v1",
        "type": "list_visible_fast",
        "captureMode": "list",
        "sourceUrl": "https://jobs.example/list",
        "jobs": [
            base_job(title="数据分析师", company="上海测试科技有限公司", url="https://jobs.example/job/1"),
            base_job(
                title="商业分析师",
                company="北京样例信息技术有限公司",
                location="北京",
                url="https://jobs.example/job/2",
                detailUrl="https://jobs.example/job/2",
                text="岗位：商业分析师\n公司：北京样例信息技术有限公司\n地点：北京\n职责：负责商业化数据分析、收入预测和销售运营报表。",
                rawText="商业分析师 北京样例信息技术有限公司 北京 本科 3年经验 商业化数据分析和收入预测。",
            ),
        ],
    }
    records = records_from_payload(payload, "list_payload")
    assert_true(len(records) == 2, "list payload should parse multiple records")
    assert_true([record["title"] for record in records] == ["数据分析师", "商业分析师"], "list order should be stable")


def test_list_payload_with_detail_text() -> None:
    payload = {
        "schemaVersion": "careerpilot.capture.v1",
        "type": "list_paginated_with_details",
        "captureMode": "list",
        "sourceUrl": "https://jobs.example/list",
        "jobs": [
            base_job(
                title="增长数据分析师",
                detailFetched=True,
                detailText="岗位职责：负责增长漏斗、渠道归因和 A/B 实验分析。任职要求：熟练 SQL 和 Python，能独立推进分析项目。",
            )
        ],
    }
    records = records_from_payload(payload, "list_with_detail")
    assert_true(len(records) == 1, "list detail payload should parse one record")
    assert_true("增长漏斗" in records[0]["text"], "detailText should be preserved in record text")


def test_failed_detail_keeps_card_fields() -> None:
    payload = {
        "schemaVersion": "careerpilot.capture.v1",
        "type": "list_paginated_with_details",
        "captureMode": "list",
        "sourceUrl": "https://jobs.example/list",
        "jobs": [
            base_job(
                title="风控数据分析师",
                salary="",
                url="https://jobs.example/job/failed",
                detailUrl="https://jobs.example/job/failed",
                detailFetched=False,
                detailText="",
                text="岗位：风控数据分析师\n公司：上海测试科技有限公司\n地点：上海\n职责：负责风险策略分析、监控报表和模型效果评估。",
            )
        ],
    }
    records = records_from_payload(payload, "failed_detail")
    assert_true(len(records) == 1, "failed detail should not drop complete card")
    assert_true(records[0]["salary"] == "", "missing salary should remain empty")
    assert_true(records[0]["title"] == "风控数据分析师", "card title should survive failed detail")


def test_legacy_payload_without_schema_version() -> None:
    payload = {
        "type": "list_visible_fast",
        "title": "旧版采集",
        "url": "https://jobs.example/list",
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
    records = records_from_payload(payload, "legacy_payload")
    assert_true(len(records) == 1, "legacy payload should remain compatible")
    assert_true(records[0]["url"] == "", "legacy url-less record should remain url-less")


def main() -> None:
    tests = [
        test_single_detail_payload,
        test_list_payload,
        test_list_payload_with_detail_text,
        test_failed_detail_keeps_card_fields,
        test_legacy_payload_without_schema_version,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    main()
