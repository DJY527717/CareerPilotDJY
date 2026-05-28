"""JD analysis service for the stdlib Web API.

This module keeps the API layer free of Streamlit state/UI while reusing the
existing pure matching parser in ``careerpilot.matching.jd_parser``.
"""

from __future__ import annotations

import html
import re
from typing import Any

from careerpilot.matching.jd_parser import (
    JOB_FAMILY_RULES,
    MATCH_TOOL_ALIASES,
    build_structured_jd,
)
from careerpilot.matching.schema import MatchingServices
from careerpilot_api.schemas import ActionItemSchema, JobCardSchema, ScoreItemSchema


FULLWIDTH_TRANSLATION = str.maketrans(
    {
        "：": ":",
        "，": ",",
        "。": ".",
        "、": "/",
        "｜": "|",
        "＋": "+",
        "－": "-",
        "～": "~",
        "—": "-",
        "–": "-",
        "　": " ",
    }
)

CITY_NAMES = [
    "北京",
    "上海",
    "广州",
    "深圳",
    "杭州",
    "苏州",
    "南京",
    "成都",
    "重庆",
    "武汉",
    "西安",
    "天津",
    "长沙",
    "郑州",
    "合肥",
    "宁波",
    "厦门",
    "青岛",
    "无锡",
    "佛山",
    "东莞",
]

UNKNOWN_TITLE = "待确认岗位"
UNKNOWN_COMPANY = "待确认公司"
UNKNOWN_LOCATION = "待确认"
UNKNOWN_SALARY = "待确认"

SKILL_ALIASES: dict[str, list[str]] = {
    "数据分析": ["数据分析", "经营分析", "商业分析", "指标", "数据口径", "看板"],
    "业务分析": ["业务分析", "需求分析", "问题拆解", "结构化分析", "行业研究"],
    "产品能力": ["产品经理", "产品助理", "PRD", "原型", "用户研究", "竞品分析"],
    "项目管理": ["项目管理", "项目推进", "PMO", "交付", "进度管理"],
    "运营": ["运营", "用户运营", "内容运营", "活动运营", "社群运营", "增长"],
    "市场营销": ["市场", "营销", "品牌", "投放", "渠道", "Campaign", "SEO", "SEM"],
    "沟通协作": ["沟通", "协作", "跨部门", "汇报", "推动", "协调"],
    "财务分析": ["财务分析", "预算", "成本", "审计", "投研", "估值", "风控"],
    "前端": ["前端", "React", "Vue", "TypeScript", "JavaScript", "HTML", "CSS"],
    "后端": ["后端", "Java", "Go", "API", "服务端", "Spring"],
    "机器学习/AI": ["机器学习", "AI", "算法", "模型", "NLP", "LLM"],
    "LCA": ["LCA", "生命周期", "碳足迹", "openLCA", "SimaPro", "GaBi"],
    "英文能力": ["英语", "英文", "English", "CET-6", "六级", "雅思", "托福"],
}

for tool, aliases in MATCH_TOOL_ALIASES.items():
    SKILL_ALIASES.setdefault(tool, aliases)

FAMILY_LABELS = {
    "data_analysis": "数据/商业分析岗",
    "product_management": "产品岗",
    "operations": "运营岗",
    "consulting": "咨询/项目岗",
    "finance": "财务/金融岗",
    "marketing": "市场/销售岗",
    "software_engineering": "研发/工程岗",
    "sustainability_esg": "ESG/可持续岗",
    "design": "设计岗",
    "sales": "销售/商务岗",
    "general": "通用岗位",
}


def analyze_jd_for_api(text: str) -> dict[str, object]:
    """Analyze a single JD and return the stable Web API response shape."""
    clean = normalize_text_without_urls(text)
    basic = extract_basic_info(clean)
    category = infer_category(clean, basic["岗位名"])
    jd_analysis = {
        "raw_text": clean,
        "basic": basic,
        "category": category,
    }
    structured = build_structured_jd(jd_analysis, None, _matching_services())
    requirements = build_requirements(clean, structured)
    keywords = build_keywords(structured)
    risks = build_risks(clean, structured, basic)
    score_value = score_analysis(clean, structured, requirements, keywords)
    family = str(structured.get("job_family") or "general")
    title = known_or(str(structured.get("job_title") or basic.get("岗位名") or ""), UNKNOWN_TITLE)
    company = known_or(str(basic.get("公司名") or ""), UNKNOWN_COMPANY)
    location = known_or(str(structured.get("location") or basic.get("地点") or ""), UNKNOWN_LOCATION)
    salary_label = known_or(salary_to_label(structured.get("salary")) or str(basic.get("薪资") or ""), UNKNOWN_SALARY)
    return {
        "summary": build_summary(title, family, requirements, keywords, risks),
        "input_length": len(clean),
        "requirements": requirements,
        "keywords": keywords,
        "risks": risks,
        "suggested_actions": build_actions(risks, requirements),
        "recommended_job": JobCardSchema(
            id="jd-analysis-result",
            title=title,
            company=company,
            location=location,
            track=FAMILY_LABELS.get(family, "通用岗位"),
            match_score=score_value,
            salary=salary_label,
            highlights=(keywords[:3] or requirements[:3] or ["等待补充岗位信息"]),
        ),
        "scores": [
            ScoreItemSchema(id="score-info", label="信息完整度", value=score_value, note="根据标题、地点、要求和关键词完整度计算"),
            ScoreItemSchema(id="score-requirements", label="要求清晰度", value=requirement_score(structured), note="根据硬性要求和职责描述清晰度计算"),
            ScoreItemSchema(id="score-action", label="行动优先级", value=action_priority_score(risks), note="用于判断后续是否需要补充信息或人工确认"),
        ],
    }


def normalize_unicode_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value)).translate(FULLWIDTH_TRANSLATION)
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\ufeff]", "", text)
    return text


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", normalize_unicode_text(value)).strip()


def normalize_multiline_text(value: Any) -> str:
    lines = []
    for line in normalize_unicode_text(value).splitlines():
        clean = re.sub(r"[ \t\r\f\v]+", " ", line).strip()
        if clean and (not lines or lines[-1] != clean):
            lines.append(clean)
    return "\n".join(lines).strip()


def normalize_text_without_urls(value: Any) -> str:
    text = normalize_multiline_text(value)
    text = re.sub(r"https?://\S+|www\.\S+", " ", text, flags=re.I)
    return normalize_multiline_text(text)


def text_contains(text: str, keyword: str) -> bool:
    clean = normalize_text(text)
    target = normalize_text(keyword)
    if not clean or not target:
        return False
    has_cjk = bool(re.search(r"[\u4e00-\u9fff]", target))
    is_short_english = bool(re.fullmatch(r"[A-Za-z0-9_+#.-]{1,3}", target))
    if not has_cjk and is_short_english:
        pattern = rf"(?<![A-Za-z0-9_]){re.escape(target)}(?![A-Za-z0-9_])"
        return bool(re.search(pattern, clean, flags=re.I))
    return target.lower() in clean.lower()


def split_preference_items(items: list[Any]) -> list[str]:
    output: list[str] = []
    for item in items:
        for part in re.split(r"[,，/、;；|]\s*", str(item)):
            clean = normalize_text(part)
            if clean and clean not in output:
                output.append(clean)
    return output


def score_keywords(text: str, keywords: list[str]) -> int:
    return sum(1 for keyword in keywords if text_contains(text, keyword))


def extract_by_patterns(text: str, patterns: list[str], default: str = "未识别") -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            value = normalize_text(match.group(1)).strip(" :，,;；|")
            if value:
                return value[:80]
    return default


def extract_job_title(text: str) -> str:
    return extract_by_patterns(
        text,
        [
            r"(?:岗位名称|职位名称|招聘职位|岗位|职位|Job Title|Position|Role|Title)\s*[:：-]\s*([^,.;，。；;\n]{2,80}?)(?=\s+(?:公司名称|企业名称|公司|企业|工作地点|地点|城市|薪资|学历|经验|链接|URL|JD|岗位职责|职责|任职要求|要求|Company|Location|Responsibilities|Requirements)\s*[:：-]|[,.;，。；;\n]|$)",
            r"招聘\s*([^,.;，。；;\n]{2,40}(?:工程师|顾问|专员|分析师|经理|实习生|助理))",
            r"(?:Hiring|Seeking|Recruiting)\s+(?:a\s+|an\s+)?([A-Za-z][A-Za-z0-9 /&+-]{2,70}(?:Analyst|Engineer|Manager|Designer|Consultant|Specialist|Intern|Associate))",
            r"([^,.;，。；;\n]{2,40}(?:产品|运营|数据|研发|算法|前端|后端|市场|销售|财务|法务|人力|供应链|咨询|项目|合规)[^,.;，。；;\n]{0,20}(?:工程师|顾问|专员|分析师|经理|实习生|助理|管培生)?)",
        ],
    )


def extract_company(text: str) -> str:
    return extract_by_patterns(
        text,
        [
            r"(?:公司名称|公司名|企业名称|企业名|单位名称|雇主|Company|Employer|Organization)\s*[:：-]\s*([^,.;，。；;\n]{2,80})",
            r"([\u4e00-\u9fa5A-Za-z0-9（）()·&\-]{2,60}(?:有限公司|有限责任公司|股份有限公司|集团|公司|研究院|事务所|咨询|科技))",
        ],
    )


def extract_location(text: str) -> str:
    explicit = extract_by_patterns(
        text,
        [
            r"(?:工作地点|地点|办公地点|城市|base|Base|BASE|Location|Work Location)\s*[:：-]?\s*([^,.;，。；;\n]{2,40})",
        ],
    )
    if explicit != "未识别":
        return explicit
    found = [city for city in CITY_NAMES if text_contains(text, city)]
    return " / ".join(found[:3]) if found else "未识别"


def extract_education(text: str) -> str:
    levels = []
    if re.search(r"博士|PhD", text, flags=re.I):
        levels.append("博士")
    if re.search(r"硕士|研究生|master", text, flags=re.I):
        levels.append("硕士")
    if re.search(r"本科|学士|bachelor", text, flags=re.I):
        levels.append("本科")
    if re.search(r"大专|专科", text, flags=re.I):
        levels.append("大专")
    if re.search(r"\bBS\b|\bBA\b|Bachelor", text, flags=re.I) and "本科" not in levels:
        levels.append("本科")
    if re.search(r"\bMS\b|\bMA\b|Master", text, flags=re.I) and "硕士" not in levels:
        levels.append("硕士")
    return " / ".join(levels) if levels else "未识别"


def extract_experience(text: str) -> str:
    return extract_by_patterns(
        text,
        [
            r"((?:\d+\s*[-~至到]\s*)?\d+\s*年(?:以上)?(?:工作)?经验)",
            r"(\d+\s*[-~to]+\s*\d*\s*years?(?:\s+of)?\s+experience)",
            r"(internship|new graduate|entry level)",
            r"(经验不限)",
            r"(应届生|校招|秋招|实习)",
        ],
    )


def extract_basic_info(text: str) -> dict[str, str]:
    from careerpilot.matching.preference_fit import extract_salary_from_jd

    salary = extract_salary_from_jd(text)
    salary_label = salary_to_label(salary)
    return {
        "公司名": extract_company(text),
        "岗位名": extract_job_title(text),
        "地点": extract_location(text),
        "薪资": salary_label or "未识别",
        "学历要求": extract_education(text),
        "经验要求": extract_experience(text),
    }


def infer_category(text: str, title: str) -> str:
    combined = f"{title} {text}"
    scores = {family: score_keywords(combined, split_preference_items(words)) for family, words in JOB_FAMILY_RULES.items()}
    best = max(scores, key=scores.get) if scores else "general"
    if scores.get(best, 0) <= 0:
        return "待人工判断"
    return FAMILY_LABELS.get(best, "通用岗位")


def jd_skill_list(jd_analysis: dict[str, Any] | None) -> list[str]:
    text = normalize_text((jd_analysis or {}).get("raw_text", ""))
    hits = [
        skill
        for skill, aliases in SKILL_ALIASES.items()
        if any(text_contains(text, alias) for alias in aliases)
    ]
    return hits[:18]


def build_requirements(text: str, structured: dict[str, Any]) -> list[str]:
    section_requirements = extract_labeled_sections(text)
    hard = [str(item.get("text", "")) for item in structured.get("hard_requirements", []) if item.get("text")]
    core = [str(item) for item in structured.get("core_responsibilities", [])]
    template = [str(item) for item in (structured.get("template_hints", {}) or {}).get("responsibilities", [])]
    candidates = [item for item in section_requirements + hard + core + template if is_clean_extracted_phrase(item)]
    return unique_strings(candidates, 8) or ["岗位要求暂不完整，建议补充更详细的职责和任职条件。"]


def extract_labeled_sections(text: str) -> list[str]:
    output: list[str] = []
    for line in normalize_multiline_text(text).splitlines():
        clean = normalize_text(line)
        if not re.search(r"岗位职责|工作职责|职位描述|工作内容|任职要求|岗位要求|要求|Responsibilities|Requirements|Qualifications", clean, flags=re.I):
            continue
        value = re.sub(r"^(?:岗位职责|工作职责|职位描述|工作内容|任职要求|岗位要求|要求|Responsibilities|Requirements|Qualifications)\s*[:：-]?", "", clean, flags=re.I)
        for part in re.split(r"[.;；。]\s*|(?:\d+[.、])|(?:[-*]\s+)", value):
            item = normalize_text(part).strip(" -:;,.，。；")
            if len(item) >= 6 or len(item.split()) >= 3:
                output.append(item)
    return output


def build_keywords(structured: dict[str, Any]) -> list[str]:
    template_skills = [str(item) for item in (structured.get("template_hints", {}) or {}).get("skills", [])]
    candidates = [
        str(item)
        for item in (
            structured.get("skills", [])
            + structured.get("tools", [])
            + structured.get("soft_skills", [])
            + structured.get("industry_background", [])
            + template_skills
        )
        if is_clean_extracted_phrase(str(item), max_length=32)
    ]
    return unique_strings(candidates, 10) or ["岗位关键词待确认"]


def build_risks(text: str, structured: dict[str, Any], basic: dict[str, str]) -> list[str]:
    risks: list[str] = []
    if not normalize_text(text):
        risks.append("未收到岗位描述文本，仅返回低置信度占位分析。")
    elif len(text) < 80:
        risks.append("岗位描述较短，分析结果需要人工确认。")
    if basic.get("岗位名") in {"", "未识别"}:
        risks.append("未识别到明确岗位名称。")
    if basic.get("地点") in {"", "未识别"}:
        risks.append("未识别到明确工作地点。")
    if not structured.get("hard_requirements"):
        risks.append("硬性要求不够明确，建议补充学历、经验、语言或到岗要求。")
    if any(text_contains(text, keyword) for keyword in ["职责模糊", "销售指标", "无转正", "纯杂务", "长期出差"]):
        risks.append("文本中出现需要进一步核实的岗位风险信号。")
    return unique_strings(risks, 5) or ["暂无明显结构性风险，仍建议结合真实岗位来源人工复核。"]


def build_actions(risks: list[str], requirements: list[str]) -> list[ActionItemSchema]:
    actions = [
        ActionItemSchema(id="action-review", title="核对岗位信息", detail="确认岗位名称、公司、地点、薪资和硬性要求是否完整。", priority="高"),
        ActionItemSchema(id="action-map", title="映射简历证据", detail="围绕已识别要求，准备能支撑关键词的经历或项目证据。", priority="中"),
    ]
    if risks and "暂无明显" not in risks[0]:
        actions.insert(1, ActionItemSchema(id="action-fill", title="补充缺失字段", detail="先补充描述较短或未识别的岗位字段，再做简历匹配。", priority="高"))
    if len(requirements) >= 3:
        actions.append(ActionItemSchema(id="action-prioritize", title="排序核心要求", detail="将硬性条件、核心职责和加分项拆开，决定投递前优先补强项。", priority="中"))
    return actions[:4]


def build_summary(title: str, family: str, requirements: list[str], keywords: list[str], risks: list[str]) -> str:
    family_label = FAMILY_LABELS.get(family, "通用岗位")
    requirement_count = len(requirements)
    keyword_preview = "、".join(keywords[:3])
    risk_note = "存在需确认信息" if risks and "暂无明显" not in risks[0] else "未发现明显结构性风险"
    return f"已识别为{family_label}：{title}。当前提取到 {requirement_count} 条要求，关键词包括 {keyword_preview}；{risk_note}。"


def score_analysis(text: str, structured: dict[str, Any], requirements: list[str], keywords: list[str]) -> int:
    if not normalize_text(text):
        return 15
    score = 35 if len(text) < 80 else 42
    score += min(len(text) // 30, 18)
    score += min(len(requirements) * 4, 18)
    score += min(len(keywords) * 2, 12)
    if structured.get("job_title"):
        score += 5
    if structured.get("location") and structured.get("location") != "未识别":
        score += 5
    return clamp(score)


def requirement_score(structured: dict[str, Any]) -> int:
    hard_count = len(structured.get("hard_requirements", []) or [])
    core_count = len(structured.get("core_responsibilities", []) or [])
    return clamp(45 + hard_count * 7 + core_count * 5)


def action_priority_score(risks: list[str]) -> int:
    if not risks or "暂无明显" in risks[0]:
        return 62
    return clamp(72 + min(len(risks), 4) * 5)


def known_or(value: str, fallback: str) -> str:
    clean = normalize_text(value)
    if not clean or clean == "未识别":
        return fallback
    return clean


def salary_to_label(value: Any) -> str:
    if not isinstance(value, dict):
        return str(value or "")
    label = str(value.get("label") or value.get("raw") or "").strip()
    if label:
        return label
    minimum = value.get("min")
    maximum = value.get("max")
    period = value.get("period")
    if minimum is not None and maximum is not None and period:
        return f"{minimum}-{maximum}/{period}"
    return ""


def unique_strings(items: list[str], limit: int) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = normalize_text(item).strip(" -:;,.，。；")
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
        if len(output) >= limit:
            break
    return output


def is_clean_extracted_phrase(value: str, max_length: int = 96) -> bool:
    clean = normalize_text(value)
    if not clean or len(clean) > max_length:
        return False
    label_hits = sum(1 for label in ["公司名称", "岗位名称", "工作地点", "岗位职责", "任职要求"] if label in clean)
    if label_hits:
        return False
    return True


def clamp(value: int, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(value)))


def _matching_services() -> MatchingServices:
    return MatchingServices(
        normalize_text=normalize_text,
        normalize_multiline_text=normalize_multiline_text,
        text_contains=text_contains,
        score_keywords=score_keywords,
        split_preference_items=split_preference_items,
        jd_skill_list=jd_skill_list,
        extract_job_title=extract_job_title,
        parse_resume_sections=lambda _text: {},
        normalize_resume_lines=lambda _text: [],
        sectioned_resume_evidence_lines=lambda _text: [],
        first_matching_lines=lambda _lines, _aliases, _limit: [],
        resume_skill_hits=lambda _text: [],
        capability_group_hits=lambda _text: {},
        semantic_groups_for_terms=lambda _text, _terms=None: [],
        semantic_similarity_fast=lambda _left, _right: 0.0,
        semantic_similarity=lambda _left, _right: 0.0,
        capability_overlap_similarity=lambda _left, _right: 0.0,
        remove_duplicate_information=lambda value: (normalize_multiline_text(value), 0),
        legacy_matcher=lambda *_args, **_kwargs: {},
        skill_aliases=SKILL_ALIASES,
        semantic_capability_groups={},
        resume_action_terms=[],
        resume_result_terms=[],
        english_evidence_terms=[],
        resume_project_sections=set(),
        resume_skill_list_sections=set(),
    )
