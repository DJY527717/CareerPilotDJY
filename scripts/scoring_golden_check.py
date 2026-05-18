from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app  # noqa: E402


@dataclass(frozen=True)
class GoldenCase:
    case_id: str
    label: str
    jd: str
    resume: str
    minimum: int
    maximum: int
    note: str


GOLDEN_CASES = [
    GoldenCase(
        case_id="data_strong_project",
        label="数据岗强项目证据",
        jd="数据分析实习生。岗位要求：必须掌握 SQL、Python、Power BI，负责数据看板建设和业务分析。",
        resume="项目经历\n负责 SQL 数据看板搭建，使用 Python 清洗用户数据，沉淀 Power BI 报表，提升分析效率 25%。",
        minimum=70,
        maximum=92,
        note="核心技能出现在项目经历中，并带有动作和量化结果。",
    ),
    GoldenCase(
        case_id="data_weak_project",
        label="数据岗弱经历证据",
        jd="数据分析岗位。必须掌握 SQL、Python，负责指标拆解、看板分析和经营复盘。",
        resume="项目经历\n使用 SQL 分析用户数据，参与 Python 数据处理。",
        minimum=48,
        maximum=68,
        note="提到工具，但缺少清晰交付物、结果或量化影响。",
    ),
    GoldenCase(
        case_id="data_skill_list_only",
        label="数据岗仅技能栏罗列",
        jd="数据分析岗位。必须掌握 SQL、Python、Excel，负责数据看板和业务分析。",
        resume="技能\nSQL / Python / Excel / Power BI",
        minimum=25,
        maximum=55,
        note="只在技能栏罗列，不能被误判为强项目匹配。",
    ),
    GoldenCase(
        case_id="data_hard_gap",
        label="数据岗明显硬缺口",
        jd="数据分析岗位。必须掌握 SQL、Python、Power BI、Excel，负责指标体系和业务分析。",
        resume="项目经历\n负责用户访谈和活动资料整理，完成会议纪要与调研报告。",
        minimum=0,
        maximum=42,
        note="JD 核心工具和数据证据基本缺失，应保持低分。",
    ),
    GoldenCase(
        case_id="product_strong",
        label="产品岗强匹配",
        jd="产品经理实习生。负责需求分析、用户研究、竞品分析、PRD 输出和项目推进，要求能跨部门沟通。",
        resume="项目经历\n负责产品经理相关项目，主导需求分析、用户研究和竞品分析，输出 PRD 与原型，跨部门推进开发上线，留存率提升 12%。",
        minimum=72,
        maximum=100,
        note="产品核心动作完整，有用户研究、PRD、推进和结果证据。",
    ),
    GoldenCase(
        case_id="product_keyword_only",
        label="产品岗关键词堆叠",
        jd="产品经理实习生。负责需求分析、用户研究、竞品分析和 PRD 输出。",
        resume="技能\n产品经理 / 需求分析 / PRD / Axure / 用户研究",
        minimum=25,
        maximum=60,
        note="关键词在技能区，缺少项目动作，应明显低于强匹配。",
    ),
    GoldenCase(
        case_id="operation_strong",
        label="运营岗强匹配",
        jd="增长运营实习生。负责活动运营、用户转化、留存分析和复盘，要求能基于数据优化运营策略。",
        resume="实习经历\n负责社群活动运营，设计转化路径并复盘留存数据，3 周内拉新 800 人，活动转化率提升 18%。",
        minimum=68,
        maximum=92,
        note="运营动作、指标和结果完整。",
    ),
    GoldenCase(
        case_id="engineering_hard_gap",
        label="工程岗硬缺口",
        jd="后端开发实习生。必须熟悉 Java、Spring、数据库、API 开发，负责服务端接口开发和部署。",
        resume="项目经历\n负责市场调研、用户访谈和报告撰写，协助团队整理需求文档。",
        minimum=0,
        maximum=40,
        note="工程核心证据缺失，应被硬门槛压低。",
    ),
    GoldenCase(
        case_id="lca_strong",
        label="LCA 岗强匹配",
        jd="LCA 技术实习生。要求熟悉生命周期评价、ISO14067、产品碳足迹和 openLCA，负责清单数据整理与报告交付。",
        resume="科研/论文\n负责产品碳足迹项目，基于 ISO14067 完成生命周期评价，使用 openLCA 搭建模型并梳理清单数据，交付 1 份 LCA 报告。",
        minimum=72,
        maximum=94,
        note="LCA 方法、标准、软件和交付证据完整。",
    ),
    GoldenCase(
        case_id="cbam_without_english",
        label="CBAM 合规缺英文证据",
        jd="CBAM 出海合规实习。要求了解欧盟 CBAM、电池法、英文材料整理和海外合规沟通。",
        resume="项目经历\n参与 CBAM 政策研究，整理欧盟碳边境调节机制要求，输出中文政策摘要和企业申报风险清单。",
        minimum=48,
        maximum=76,
        note="CBAM 相关但英文证据不足，应是中等而非强满分。",
    ),
]


def run_case(case: GoldenCase, *, fast: bool) -> tuple[bool, dict[str, object]]:
    jd_analysis = app.analyze_jd(case.jd)
    result = app.match_resume_to_jd(jd_analysis, case.resume, fast=fast)
    score = int(result.get("overall_score", 0))
    passed = case.minimum <= score <= case.maximum
    return passed, {
        "id": case.case_id,
        "label": case.label,
        "score": score,
        "expected": f"{case.minimum}-{case.maximum}",
        "passed": passed,
        "coverage": result.get("coverage_score"),
        "evidence": result.get("evidence_score"),
        "hard_requirement": result.get("hard_requirement_score"),
        "ceilings": result.get("score_ceiling_reasons", []),
        "note": case.note,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run CareerPilot scoring golden samples.")
    parser.add_argument("--slow", action="store_true", help="Use the full semantic scorer instead of fast mode.")
    args = parser.parse_args()

    failures = []
    rows = []
    for case in GOLDEN_CASES:
        passed, row = run_case(case, fast=not args.slow)
        rows.append(row)
        if not passed:
            failures.append(row)

    for row in rows:
        status = "PASS" if row["passed"] else "FAIL"
        ceilings = "；".join(row["ceilings"]) if row["ceilings"] else "-"
        print(
            f"{status} {row['id']}: {row['score']} "
            f"(expected {row['expected']}) | coverage={row['coverage']} "
            f"evidence={row['evidence']} hard={row['hard_requirement']} | ceiling={ceilings}"
        )

    print(f"\n{len(rows) - len(failures)}/{len(rows)} golden samples passed.")
    if failures:
        print("Failed cases: " + ", ".join(str(row["id"]) for row in failures))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
