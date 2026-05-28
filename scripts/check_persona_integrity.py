"""Lightweight persona and role integrity checks for CareerPilot Web.

Run:
    .\.venv\Scripts\python.exe scripts/check_persona_integrity.py
"""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import fields
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
WEB_SRC = ROOT / "web" / "src"
API_SRC = ROOT / "careerpilot_api"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


Issue = tuple[str, str, str]


RECOMMENDATION_LEVELS = {
    "priority_apply",
    "apply_after_rewrite",
    "cautious_apply",
    "backup",
    "not_recommended",
}

RECOMMENDATION_LABELS = {"优先投递", "改简历后投", "谨慎投递", "备选观察", "不建议投"}

EXPECTED_TASK_ROLES = {
    "batch_jd_screening": "batch_jd_screener",
    "batch_result_dashboard": "batch_result_analyst",
    "single_jd_analysis": "jd_hr_analyst",
    "resume_match": "resume_match_reviewer",
    "resume_rewrite": "resume_rewrite_advisor",
    "application_strategy": "application_strategy_advisor",
    "interview_review": "interview_review_advisor",
    "report_summary": "report_analyst",
}

INAPPROPRIATE_SHARED_ROLE_PAIRS = {
    ("batch_jd_screening", "batch_result_dashboard"),
    ("batch_jd_screening", "resume_rewrite"),
    ("batch_result_dashboard", "resume_rewrite"),
    ("resume_rewrite", "application_strategy"),
    ("resume_match", "application_strategy"),
}

WEB_FORBIDDEN_PATTERNS = {
    "streamlit": re.compile(r"streamlit", re.IGNORECASE),
    "st.": re.compile(r"(?<![A-Za-z0-9_])st\."),
    "data-testid": re.compile(r"data-testid"),
}

MOCK_PERSONAL_PATTERNS = {
    "email": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "phone": re.compile(r"(?<!\d)(?:1[3-9]\d{9}|\d{3}[- ]?\d{3,4}[- ]?\d{4})(?!\d)"),
    "personal_name_label": re.compile(r"(姓名|Name)[:：]\s*(?!匿名|示例|Example|待补充)[\u4e00-\u9fffA-Za-z]{2,20}"),
    "real_resume_background": re.compile(r"(清华大学|北京大学|复旦大学|上海交通大学|浙江大学|腾讯|阿里|字节跳动|美团|百度|微软|谷歌)"),
}


def normalize(path: Path) -> str:
    return str(path.resolve().relative_to(ROOT)).replace("\\", "/")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def iter_files(base: Path, suffixes: tuple[str, ...]) -> Iterable[Path]:
    if not base.exists():
        return []
    return (path for path in base.rglob("*") if path.is_file() and path.suffix in suffixes)


def add_issue(issues: list[Issue], path: str | Path, issue_type: str, detail: str) -> None:
    issues.append((normalize(path) if isinstance(path, Path) else path, issue_type, detail))


def check_role_router(issues: list[Issue]) -> None:
    from careerpilot_api.product_roles import PRODUCT_ROLES
    from careerpilot_api.role_boundaries import ROLE_BOUNDARIES
    from careerpilot_api.role_router import ROLE_ROUTES

    route_role_ids = [route.role_id for route in ROLE_ROUTES.values()]
    route_role_set = set(route_role_ids)
    expected_role_set = set(EXPECTED_TASK_ROLES.values())
    configured_role_ids = set(PRODUCT_ROLES) | set(ROLE_BOUNDARIES)

    missing_expected = expected_role_set - route_role_set
    if missing_expected:
        add_issue(
            issues,
            API_SRC / "role_router.py",
            "unregistered_role_id",
            f"role_router缺少注册: {sorted(missing_expected)}",
        )

    missing_configured = configured_role_ids - route_role_set
    if missing_configured:
        add_issue(
            issues,
            API_SRC / "role_router.py",
            "unregistered_role_id",
            f"产品角色未在role_router中注册: {sorted(missing_configured)}",
        )

    for task_type, expected_role_id in EXPECTED_TASK_ROLES.items():
        route = ROLE_ROUTES.get(task_type)
        if route is None:
            add_issue(issues, API_SRC / "role_router.py", "missing_task_type", f"缺少task_type: {task_type}")
            continue
        if route.role_id != expected_role_id:
            add_issue(
                issues,
                API_SRC / "role_router.py",
                "wrong_role_route",
                f"{task_type} 应映射到 {expected_role_id}，当前为 {route.role_id}",
            )

    duplicated_roles = {role_id for role_id in route_role_ids if route_role_ids.count(role_id) > 1}
    for left, right in INAPPROPRIATE_SHARED_ROLE_PAIRS:
        left_route = ROLE_ROUTES.get(left)
        right_route = ROLE_ROUTES.get(right)
        if left_route and right_route and left_route.role_id == right_route.role_id:
            add_issue(
                issues,
                API_SRC / "role_router.py",
                "inappropriate_shared_role_id",
                f"{left} 与 {right} 不应共用 role_id={left_route.role_id}",
            )

    if duplicated_roles:
        duplicates = {
            role_id: [task for task, route in ROLE_ROUTES.items() if route.role_id == role_id]
            for role_id in sorted(duplicated_roles)
        }
        add_issue(
            issues,
            API_SRC / "role_router.py",
            "shared_role_id_review_required",
            f"多个task_type共用role_id，需要确认是否合适: {duplicates}",
        )


def check_forbidden_outputs(issues: list[Issue]) -> None:
    from careerpilot_api.role_boundaries import ROLE_BOUNDARIES
    from careerpilot_api.role_router import ROLE_ROUTES, SAFE_FALLBACK_ROUTE

    for role_id, boundary in ROLE_BOUNDARIES.items():
        if not getattr(boundary, "forbidden_outputs", ()):
            add_issue(issues, API_SRC / "role_boundaries.py", "missing_forbidden_outputs", f"{role_id}缺少forbidden_outputs")

    for task_type, route in ROLE_ROUTES.items():
        if not getattr(route, "forbidden_outputs", ()):
            add_issue(issues, API_SRC / "role_router.py", "missing_forbidden_outputs", f"{task_type}缺少forbidden_outputs")

    if not getattr(SAFE_FALLBACK_ROUTE, "forbidden_outputs", ()):
        add_issue(issues, API_SRC / "role_router.py", "missing_forbidden_outputs", "fallback route缺少forbidden_outputs")


def check_rewrite_schema(issues: list[Issue]) -> None:
    from careerpilot_api.schemas import RewriteSuggestion

    field_names = {field.name for field in fields(RewriteSuggestion)}
    if "evidence_status" not in field_names:
        add_issue(issues, API_SRC / "schemas.py", "missing_rewrite_field", "RewriteSuggestion缺少evidence_status")

    types_source = read_text(WEB_SRC / "types.ts")
    if "evidence_status:" not in types_source:
        add_issue(issues, WEB_SRC / "types.ts", "missing_rewrite_field", "前端RewriteSuggestion缺少evidence_status")


def check_web_forbidden_terms(issues: list[Issue]) -> None:
    for path in iter_files(WEB_SRC, (".ts", ".tsx", ".css")):
        source = read_text(path)
        for issue_type, pattern in WEB_FORBIDDEN_PATTERNS.items():
            match = pattern.search(source)
            if match:
                add_issue(issues, path, "forbidden_web_term", f"web端文件包含 {issue_type}")


def check_mock_personal_data(issues: list[Issue]) -> None:
    mock_files = [
        *(WEB_SRC.glob("*mock*.ts")),
        *(WEB_SRC.glob("*Mock*.ts")),
        WEB_SRC / "pages" / "resume" / "ResumeMatchPage.tsx",
        API_SRC / "mock_service.py",
    ]
    for path in sorted({item for item in mock_files if item.exists()}):
        source = read_text(path)
        for issue_type, pattern in MOCK_PERSONAL_PATTERNS.items():
            for match in pattern.finditer(source):
                if issue_type == "phone":
                    window = source[max(0, match.start() - 80) : match.end() + 80]
                    if not re.search(r"电话|手机|联系方式|phone|mobile|contact", window, re.IGNORECASE):
                        continue
                add_issue(issues, path, "mock_data_personal_info", f"{issue_type}: {match.group(0)}")


def dict_contains_all_recommendation_levels(node: ast.AST) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    keys = set()
    for key in node.keys:
        if isinstance(key, ast.Constant) and isinstance(key.value, str):
            keys.add(key.value)
    return RECOMMENDATION_LEVELS.issubset(keys)


def dict_contains_recommendation_labels(node: ast.AST) -> bool:
    if not isinstance(node, ast.Dict):
        return False
    values = set()
    for value in node.values:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            values.add(value.value)
    return len(RECOMMENDATION_LABELS & values) >= 4


def check_duplicate_recommendation_labels(issues: list[Issue]) -> None:
    canonical = {
        normalize(API_SRC / "schemas.py"),
        normalize(WEB_SRC / "constants" / "recommendationLabels.ts"),
        normalize(WEB_SRC / "constants" / "actionButtons.ts"),
    }
    for path in [*iter_files(API_SRC, (".py",)), *iter_files(WEB_SRC, (".ts", ".tsx"))]:
        rel = normalize(path)
        source = read_text(path)
        if path.suffix == ".py":
            try:
                tree = ast.parse(source)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if dict_contains_all_recommendation_levels(node) and dict_contains_recommendation_labels(node) and rel not in canonical:
                    add_issue(
                        issues,
                        path,
                        "duplicate_recommendation_level_definition",
                        "发现包含全部推荐等级的重复字典，请复用统一枚举/标签配置",
                    )
        elif rel not in canonical:
            if all(f"{level}:" in source for level in RECOMMENDATION_LEVELS):
                add_issue(
                    issues,
                    path,
                    "duplicate_recommendation_level_definition",
                    "发现包含全部推荐等级的重复对象，请复用recommendationLabels",
                )


def check_dashboard_role_misuse(issues: list[Issue]) -> None:
    dashboard_files = [
        WEB_SRC / "pages" / "jd" / "BatchJDResultsPage.tsx",
        *(path for path in iter_files(WEB_SRC, (".ts", ".tsx")) if "Dashboard" in path.name),
    ]
    for path in sorted({item for item in dashboard_files if item.exists()}):
        source = read_text(path)
        if "resume_rewrite_advisor" in source or "buildRewriteSuggestions" in source or "ResumeSuggestionPanel" in source:
            add_issue(
                issues,
                path,
                "dashboard_role_misuse",
                "数据分析看板不应调用简历改写角色或改写组件",
            )


def check_application_strategy_state_mutation(issues: list[Issue]) -> None:
    strategy_files = [
        WEB_SRC / "pages" / "decision" / "ApplicationStrategyPage.tsx",
        API_SRC / "decision_service.py",
    ]
    mutation_patterns = {
        "application_status_assignment": re.compile(r"\.application_status\s*="),
        "application_status_spread_override": re.compile(r"application_status\s*:"),
        "application_status_setter": re.compile(r"setApplicationStatus|updateApplicationStatus", re.IGNORECASE),
    }
    allowed_user_confirmation_patterns = ("onClick", "button", "confirm", "用户点击")

    for path in strategy_files:
        if not path.exists():
            continue
        source = read_text(path)
        for issue_type, pattern in mutation_patterns.items():
            for match in pattern.finditer(source):
                window = source[max(0, match.start() - 200) : match.end() + 200]
                if path.name == "ApplicationStrategyPage.tsx" and any(token in window for token in allowed_user_confirmation_patterns):
                    continue
                add_issue(
                    issues,
                    path,
                    "application_strategy_state_mutation",
                    f"投递策略可能直接修改投递状态: {issue_type}",
                )


def run_checks() -> list[Issue]:
    issues: list[Issue] = []
    check_role_router(issues)
    check_forbidden_outputs(issues)
    check_duplicate_recommendation_labels(issues)
    check_web_forbidden_terms(issues)
    check_mock_personal_data(issues)
    check_rewrite_schema(issues)
    check_dashboard_role_misuse(issues)
    check_application_strategy_state_mutation(issues)
    return issues


def main() -> int:
    issues = run_checks()
    if not issues:
        print("PASS persona integrity checks")
        return 0

    print(f"FAIL persona integrity checks: {len(issues)} issue(s)")
    for path, issue_type, detail in issues:
        print(f"- {path} | {issue_type} | {detail}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
