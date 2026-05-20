from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from textwrap import dedent
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import app


NODE_CANDIDATES = [
    "node",
    r"C:\Users\董玖一\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe",
]


def assert_equal(actual, expected, message: str) -> None:
    if actual != expected:
        raise AssertionError(f"{message}: expected {expected!r}, got {actual!r}")


def assert_true(value, message: str) -> None:
    if not value:
        raise AssertionError(message)


def test_exported_jd_keeps_structured_job_without_url() -> None:
    payload = {
        "title": "测试导出",
        "url": "https://jobs.example/list",
        "jobs": [
            {
                "title": "数据分析师",
                "company": "上海测试科技有限公司",
                "salary": "10-15万/年",
                "location": "上海",
                "education": "本科",
                "experience": "3年经验",
                "text": "岗位职责：负责数据分析和报表建设，支持业务决策和指标体系。任职要求：熟悉 SQL 和 Python，沟通能力好。",
            }
        ],
    }
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "captured_jobs.json"
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        records = app.records_from_exported_jd_file(path)

    assert_equal(len(records), 1, "structured no-url captured job should be kept")
    assert_equal(records[0].get("title"), "数据分析师", "title should survive")
    assert_equal(records[0].get("url"), "", "record should remain url-less")
    assert_equal(records[0].get("company"), "上海测试科技有限公司", "company should survive")


def test_dedupe_keeps_multiline_text_shape() -> None:
    records = app.dedupe_jd_records(
        [
            {
                "source": "unit",
                "title": "数据分析师",
                "company": "上海测试科技有限公司",
                "salary": "10-15万/年",
                "location": "上海",
                "text": "岗位：数据分析师\n公司：上海测试科技有限公司\n薪资：10-15万/年\n职责：第一行\n第二行需求\n第三行福利",
            }
        ]
    )
    assert_equal(len(records), 1, "dedupe should keep the valid record")
    text = records[0]["text"]
    assert_true("\n第二行需求\n" in text, "dedupe should not flatten multiline text")
    assert_true(text.count("\n") >= 4, "dedupe should preserve multiple text lines")


def test_crawl_jd_urls_dedupes_fetches_and_preserves_order() -> None:
    calls: list[str] = []

    def fake_fetch(url: str, timeout: int = 12, use_dynamic: bool = False, detail_limit: int = 5):
        calls.append(url)
        return {"url": url, "ok": True, "text": f"详情 {url}", "error": ""}

    urls = [
        "https://jobs.example/a",
        "https://jobs.example/b",
        "https://jobs.example/a",
        " https://jobs.example/c ",
        "https://jobs.example/b",
    ]
    with patch.object(app, "fetch_jd_url", side_effect=fake_fetch):
        results = app.crawl_jd_urls(urls, max_workers=3)

    assert_equal(
        calls,
        ["https://jobs.example/a", "https://jobs.example/b", "https://jobs.example/c"],
        "duplicate urls should be fetched once",
    )
    assert_equal(
        [item["url"] for item in results],
        ["https://jobs.example/a", "https://jobs.example/b", "https://jobs.example/c"],
        "results should follow original first-seen order",
    )


def test_51job_listing_text_parses_multiple_jobs() -> None:
    text = "\n".join(
        [
            "数据分析师",
            "10-15万/年",
            "上海",
            "本科",
            "3年经验",
            "上海测试科技有限公司",
            "民营 100-499人 互联网",
            "",
            "市场分析师",
            "8千-1.2万",
            "北京",
            "本科",
            "1年经验",
            "北京样例信息技术有限公司",
            "民营 50-150人 互联网",
        ]
    )
    records = app.records_from_51job_listing_text(text, source="smoke")

    assert_equal(len(records), 2, "51job listing text should produce multiple records")
    assert_equal([record["title"] for record in records], ["数据分析师", "市场分析师"], "titles should parse in order")
    assert_equal([record["salary"] for record in records], ["10-15万/年", "8千-1.2万"], "salaries should parse")


def test_capture_core_js() -> None:
    js = dedent(
        r"""
        const assert = require("node:assert/strict");
        const fs = require("node:fs");
        const vm = require("node:vm");

        const sourcePath = "browser_extension/capture_core.js";
        let source = fs.readFileSync(sourcePath, "utf8");
        source = source.replace(
          "const { allJobs, lastTitle, lastUrl } = await collectCurrentAndNextPages(normalized);",
          "const { allJobs, lastTitle, lastUrl } = (options && options.__testListResult) || await collectCurrentAndNextPages(normalized);"
        );
        source = source.replace(
          "const fetched = await fetchDocument(job.url, signal, 8000);",
          "const fetched = await ((globalThis.__testFetchDocument || fetchDocument)(job.url, signal, 8000));"
        );
        source = source.replace(
          "globalThis.CareerPilotExtractor = {",
          "globalThis.CareerPilotExtractor = { findSalary, salaryPatterns,"
        );

        class Element {}
        class HTMLIFrameElement extends Element {}
        const context = {
          console,
          setTimeout,
          clearTimeout,
          AbortController,
          URL,
          Element,
          HTMLElement: Element,
          HTMLIFrameElement,
          MutationObserver: class { observe() {} disconnect() {} },
          location: { href: "https://jobs.example/list" },
          document: {
            title: "Job list",
            readyState: "complete",
            documentElement: {},
            body: { innerText: "visible list text", textContent: "visible list text" },
            querySelectorAll() { return []; },
            addEventListener() {},
            removeEventListener() {},
          },
          window: {
            getComputedStyle() { return { display: "block", visibility: "visible", opacity: "1", overflow: "visible", overflowY: "visible" }; },
          },
          globalThis: null,
        };
        context.globalThis = context;
        context.window.window = context.window;
        vm.createContext(context);
        vm.runInContext(source, context, { filename: sourcePath });

        const { collectListPayload, findSalary, salaryPatterns } = context.CareerPilotExtractor;
        assert.ok(Array.isArray(salaryPatterns()) && salaryPatterns().length > 0, "salaryPatterns should be available");
        for (const salary of ["8千-1.2万", "10-15万/年", "150-200/天", "面议", "13薪"]) {
          assert.equal(findSalary(salary), salary, `salary should match ${salary}`);
        }

        const originalJobs = [
          { title: "数据分析师", company: "上海测试科技有限公司", salary: "10-15万/年", url: "https://jobs.example/job/a", text: "数据分析师 上海测试科技有限公司 10-15万/年" },
          { title: "市场分析师", company: "北京样例信息技术有限公司", salary: "8千-1.2万", url: "https://jobs.example/job/b", text: "市场分析师 北京样例信息技术有限公司 8千-1.2万" },
        ];
        context.__testFetchDocument = async (url) => {
          if (url.endsWith("/a")) throw new Error("detail failed");
          return {
            url,
            title: "detail",
            doc: {
              title: "detail",
              body: { innerText: "too short", textContent: "too short" },
              querySelectorAll() { return []; },
            },
          };
        };
        collectListPayload({
          maxJobs: 10,
          maxPages: 1,
          detailLimit: 2,
          __testListResult: {
            allJobs: originalJobs.map((job) => ({ ...job })),
            lastTitle: "Job list",
            lastUrl: "https://jobs.example/list",
          },
        }).then((payload) => {
          assert.equal(payload.jobCount, 2, "failed details should not drop job cards");
          assert.equal(payload.cardCount, 2, "card count should reflect original cards");
          assert.deepEqual(payload.jobs.map((job) => job.title), ["数据分析师", "市场分析师"]);
          assert.equal(payload.jobs[0].detailFetched, false, "failed detail should be marked on original card");
          assert.equal(payload.jobs[0].detailFetchError, "fetch_failed");
          assert.equal(payload.jobs[1].detailFetched, false, "empty detail should keep original card");
        }).catch((error) => {
          console.error(error);
          process.exit(1);
        });
        """
    )
    last_result = None
    for node in NODE_CANDIDATES:
        try:
            result = subprocess.run(
                [node, "-e", js],
                cwd=ROOT,
                text=True,
                encoding="utf-8",
                errors="replace",
                capture_output=True,
            )
        except OSError as exc:
            if "Access is denied" in str(exc) or "拒绝访问" in str(exc):
                continue
            raise
        last_result = result
        if result.returncode == 0:
            return
        if "Access is denied" not in f"{result.stdout}\n{result.stderr}" and "拒绝访问" not in f"{result.stdout}\n{result.stderr}":
            break
    result = last_result
    if result is None:
        raise AssertionError("capture_core.js smoke failed: no usable node executable found")
    if result.returncode != 0:
        raise AssertionError(f"capture_core.js smoke failed\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")


def main() -> None:
    tests = [
        test_exported_jd_keeps_structured_job_without_url,
        test_dedupe_keeps_multiline_text_shape,
        test_crawl_jd_urls_dedupes_fetches_and_preserves_order,
        test_51job_listing_text_parses_multiple_jobs,
        test_capture_core_js,
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")


if __name__ == "__main__":
    main()
