from __future__ import annotations

import re
from typing import Any

from .schema import JsonDict, MatchingServices, unique_items
from .preference_fit import extract_salary_from_jd


SUPPORTED_MATCH_JOB_FAMILIES = {
    "data_analysis",
    "product_management",
    "operations",
    "consulting",
    "finance",
    "marketing",
    "software_engineering",
    "sustainability_esg",
    "design",
    "sales",
    "general",
}

JOB_FAMILY_CATEGORY_MAP = {
    "数据/商业分析岗": "data_analysis",
    "产品岗": "product_management",
    "运营岗": "operations",
    "市场/销售岗": "marketing",
    "咨询/项目岗": "consulting",
    "咨询岗": "consulting",
    "研发/工程岗": "software_engineering",
    "设计岗": "design",
    "财务/金融岗": "finance",
    "法务/合规岗": "consulting",
    "人力/行政岗": "operations",
    "供应链/采购岗": "operations",
    "LCA技术岗": "sustainability_esg",
    "产品碳足迹岗": "sustainability_esg",
    "ESG岗": "sustainability_esg",
    "碳核算岗": "sustainability_esg",
    "CBAM/出海合规岗": "sustainability_esg",
    "泛运营岗": "operations",
}

JOB_FAMILY_RULES = {
    "data_analysis": ["数据分析", "商业分析", "经营分析", "BI", "SQL", "Python", "指标", "看板", "Tableau", "Power BI"],
    "product_management": ["产品经理", "产品助理", "需求分析", "PRD", "原型", "用户研究", "竞品分析", "路线图"],
    "operations": ["运营", "用户运营", "内容运营", "活动运营", "社群运营", "增长", "拉新", "留存", "转化"],
    "consulting": ["咨询", "顾问", "项目交付", "解决方案", "客户访谈", "尽调", "研究报告", "PMO"],
    "finance": ["财务", "金融", "投研", "审计", "预算", "成本", "估值", "风控", "报表"],
    "marketing": ["市场", "营销", "品牌", "投放", "渠道", "Campaign", "SEO", "SEM", "传播"],
    "software_engineering": ["开发", "工程师", "前端", "后端", "Java", "Go", "React", "API", "算法", "测试"],
    "sustainability_esg": ["ESG", "碳", "LCA", "生命周期", "碳足迹", "碳核算", "CBAM", "可持续", "GHG", "EPD"],
    "design": ["设计", "UI", "UX", "交互", "视觉", "Figma", "用户体验", "作品集"],
    "sales": ["销售", "商务", "BD", "客户开发", "商机", "线索", "续约", "大客户"],
}

JOB_FAMILY_TEMPLATES: dict[str, dict[str, list[str]]] = {
    "data_analysis": {
        "responsibilities": ["指标拆解与业务分析", "数据清洗、建模或看板建设", "输出数据结论并推动业务动作"],
        "skills": ["数据分析", "业务分析", "SQL", "Python", "Excel", "Power BI"],
        "hidden": ["需要能把业务问题转成可验证指标", "需要有数据口径、异常定位和结论表达能力"],
    },
    "product_management": {
        "responsibilities": ["需求分析与用户研究", "竞品分析、PRD 或原型输出", "跨部门推进产品方案落地"],
        "skills": ["产品能力", "用户研究", "项目管理", "沟通协作"],
        "hidden": ["需要证明不是只懂术语，而是真做过需求判断和方案取舍"],
    },
    "operations": {
        "responsibilities": ["活动、内容、社群或用户运营执行", "围绕拉新、转化、留存做复盘优化", "沉淀运营策略和可量化结果"],
        "skills": ["运营", "数据分析", "沟通协作", "项目管理"],
        "hidden": ["需要有指标意识和复盘闭环，不能只有执行描述"],
    },
    "consulting": {
        "responsibilities": ["信息收集、行业研究或客户访谈", "结构化分析并输出报告/方案", "推进项目交付与沟通汇报"],
        "skills": ["项目管理", "业务分析", "沟通协作"],
        "hidden": ["需要体现结构化拆解、客户语境和交付物质量"],
    },
    "finance": {
        "responsibilities": ["财务、预算、投研或风控分析", "使用数据和报表支持判断", "输出结论、模型或管理建议"],
        "skills": ["财务分析", "Excel", "业务分析"],
        "hidden": ["需要严谨的数据口径和财务/金融基础"],
    },
    "marketing": {
        "responsibilities": ["市场调研、品牌传播或渠道投放", "分析用户和竞品并设计触达策略", "复盘 Campaign 或增长效果"],
        "skills": ["市场营销", "数据分析", "用户研究"],
        "hidden": ["需要把创意、渠道和数据结果连接起来"],
    },
    "software_engineering": {
        "responsibilities": ["完成模块、接口、前后端或算法实现", "参与测试、部署和问题排查", "用工程结果支撑业务需求"],
        "skills": ["前端", "后端", "机器学习/AI", "Python"],
        "hidden": ["需要真实代码项目、技术选型和可运行结果"],
    },
    "sustainability_esg": {
        "responsibilities": ["碳核算、LCA、ESG 或合规资料整理", "基于标准/政策输出模型、清单或报告", "支持企业减碳、披露或合规交付"],
        "skills": ["LCA", "ISO14067", "GHG Protocol", "CBAM", "供应链碳管理", "英文能力"],
        "hidden": ["需要能说清标准、边界、数据来源和交付物"],
    },
    "design": {
        "responsibilities": ["用户体验或视觉问题定义", "原型、界面或设计规范输出", "基于反馈迭代设计方案"],
        "skills": ["设计", "用户研究", "产品能力"],
        "hidden": ["需要作品集、设计过程和验证结果"],
    },
    "sales": {
        "responsibilities": ["客户开发、需求识别和商机推进", "维护客户关系并推动转化/续约", "整理销售过程和结果数据"],
        "skills": ["销售/商务", "沟通协作", "市场营销"],
        "hidden": ["需要能证明客户沟通、推进节奏和转化结果"],
    },
    "general": {
        "responsibilities": ["理解岗位任务并拆解执行", "跨角色沟通协作", "输出可检查的交付物"],
        "skills": ["项目管理", "沟通协作", "业务分析"],
        "hidden": ["需要把泛泛职责改写成具体动作和结果"],
    },
}

HARD_REQUIREMENT_PATTERNS = [
    ("education", r"(本科|硕士|研究生|博士|大专|学历)[^。；;\n]{0,30}"),
    ("experience", r"(\d+\s*[-~至到]?\s*\d*\s*年(?:以上)?(?:工作)?经验|经验不限|应届|校招|实习)[^。；;\n]{0,24}"),
    ("language", r"(英语|英文|English|CET-6|六级|雅思|托福|口语)[^。；;\n]{0,30}"),
    ("location", r"(base|Base|工作地点|办公地点|地点|城市|驻场|到岗)[^。；;\n]{0,40}"),
    ("certificate", r"(证书|资格|CPA|CFA|FRM|基金从业|证券从业|法律职业资格|法考)[^。；;\n]{0,35}"),
    ("availability", r"(每周\s*\d\s*天|至少\s*\d+\s*个?月|实习\s*\d+\s*个?月|尽快到岗|可到岗)[^。；;\n]{0,35}"),
]

MATCH_TOOL_ALIASES = {
    "SQL": ["SQL", "数据库", "MySQL", "PostgreSQL", "Hive"],
    "Python": ["Python", "pandas", "numpy", "sklearn", "爬虫"],
    "Excel": ["Excel", "VLOOKUP", "Power Query", "数据透视表"],
    "Power BI": ["Power BI", "Tableau", "BI", "FineBI"],
    "Axure": ["Axure", "原型"],
    "Figma": ["Figma"],
    "Java": ["Java", "Spring", "Spring Boot"],
    "JavaScript": ["JavaScript", "TypeScript", "React", "Vue", "Node.js"],
    "openLCA": ["openLCA", "open LCA"],
    "SimaPro": ["SimaPro"],
    "GaBi": ["GaBi"],
}


def match_sentence_split(text: str, services: MatchingServices) -> list[str]:
    normalized = services.normalize_multiline_text(text)
    parts = re.split(r"[\n\r]+|(?<=[。！？；;])", normalized)
    sentences: list[str] = []
    for part in parts:
        clean = services.normalize_text(part).strip(" -:：;；,，")
        if 4 <= len(clean) <= 360:
            sentences.append(clean)
    if not sentences and normalized:
        sentences = [services.normalize_text(normalized)[:360]]
    return unique_items(sentences, services, 80)


def extract_match_tools(text: str, services: MatchingServices) -> list[str]:
    clean = services.normalize_text(text)
    tools = [tool for tool, aliases in MATCH_TOOL_ALIASES.items() if any(services.text_contains(clean, alias) for alias in aliases)]
    return unique_items(tools, services)


def extract_match_industries(text: str, services: MatchingServices) -> list[str]:
    keywords = [
        "互联网", "电商", "金融", "银行", "证券", "咨询", "制造", "新能源", "汽车", "快消", "零售", "教育",
        "医疗", "医药", "SaaS", "ToB", "ToC", "供应链", "物流", "ESG", "可持续", "碳", "出海", "欧盟",
    ]
    clean = services.normalize_text(text)
    return [item for item in keywords if services.text_contains(clean, item)]


def infer_match_job_family(jd_analysis: JsonDict, optional_job_family: str | None, services: MatchingServices) -> str:
    optional = services.normalize_text(optional_job_family or "")
    if optional in SUPPORTED_MATCH_JOB_FAMILIES:
        return optional
    category = str(jd_analysis.get("category", "") or "")
    if category in JOB_FAMILY_CATEGORY_MAP:
        return JOB_FAMILY_CATEGORY_MAP[category]
    text = services.normalize_text((jd_analysis.get("basic", {}) or {}).get("岗位名", "") + " " + jd_analysis.get("raw_text", ""))
    scores = {
        family: services.score_keywords(text, services.split_preference_items(words))
        for family, words in JOB_FAMILY_RULES.items()
    }
    best = max(scores, key=scores.get) if scores else "general"
    return best if scores.get(best, 0) > 0 else "general"


def extract_jd_hard_requirements(text: str, basic: JsonDict, services: MatchingServices) -> list[dict[str, str]]:
    requirements: list[dict[str, str]] = []
    for req_type, pattern in HARD_REQUIREMENT_PATTERNS:
        for match in re.finditer(pattern, text, flags=re.I):
            requirements.append({"type": req_type, "text": services.normalize_text(match.group(0))})
    if basic.get("学历要求") and basic.get("学历要求") != "未识别":
        requirements.append({"type": "education", "text": f"学历要求：{basic.get('学历要求')}"})
    if basic.get("经验要求") and basic.get("经验要求") != "未识别":
        requirements.append({"type": "experience", "text": f"经验要求：{basic.get('经验要求')}"})
    if basic.get("地点") and basic.get("地点") != "未识别":
        requirements.append({"type": "location", "text": f"工作地点：{basic.get('地点')}"})
    return unique_items(requirements, services, 14)


def build_structured_jd(jd_analysis: JsonDict, optional_job_family: str | None, services: MatchingServices) -> JsonDict:
    text = services.normalize_text(jd_analysis.get("raw_text", ""))
    basic = jd_analysis.get("basic", {}) or {}
    job_family = infer_match_job_family(jd_analysis, optional_job_family, services)
    template = JOB_FAMILY_TEMPLATES.get(job_family, JOB_FAMILY_TEMPLATES["general"])
    sentences = match_sentence_split(text, services)
    title = str(basic.get("岗位名") or services.extract_job_title(text) or "")
    seniority = "intern" if re.search(r"实习|校招|应届|intern", text, flags=re.I) else "experienced" if re.search(r"\d+\s*年|社招|资深|高级", text) else "unspecified"

    responsibility_markers = ["负责", "参与", "支持", "搭建", "分析", "推进", "输出", "交付", "制定", "优化", "设计", "维护"]
    core_responsibilities = [
        sentence for sentence in sentences
        if any(marker in sentence for marker in responsibility_markers)
        and not re.match(r"^(任职要求|岗位要求|要求|资格)", sentence)
    ]
    if len(core_responsibilities) < 2:
        core_responsibilities.extend(template["responsibilities"])

    exact_jd_skills = [
        skill for skill in services.jd_skill_list(jd_analysis)
        if any(services.text_contains(text, alias) for alias in services.skill_aliases.get(skill, [skill]))
    ]
    skills = unique_items(exact_jd_skills + template["skills"], services, 18)
    tools = extract_match_tools(text, services)
    industry_background = extract_match_industries(text, services)
    project_experience = [
        sentence for sentence in sentences
        if any(marker in sentence for marker in ["项目", "案例", "经验", "经历", "落地", "交付", "报告", "看板", "模型", "系统"])
    ][:10]
    soft_skills = _extract_match_soft_skills(text, services)
    bonus_requirements = [
        sentence for sentence in sentences
        if any(marker in sentence for marker in ["优先", "加分", "plus", "preferred", "bonus"])
    ][:8]
    hard_requirements = extract_jd_hard_requirements(text, basic, services)
    keywords = unique_items(
        skills
        + tools
        + soft_skills
        + industry_background
        + [item for item in JOB_FAMILY_RULES.get(job_family, []) if services.text_contains(text, item)]
        + [req["text"] for req in hard_requirements[:4]],
        services,
        36,
    )
    return {
        "raw_text": text,
        "job_title": title,
        "location": str(basic.get("地点", "") or ""),
        "salary": extract_salary_from_jd(text),
        "job_family": job_family,
        "seniority": seniority,
        "core_responsibilities": unique_items(core_responsibilities, services, 12),
        "hard_requirements": hard_requirements,
        "skills": skills,
        "tools": tools,
        "industry_background": industry_background,
        "project_experience": unique_items(project_experience, services, 10),
        "soft_skills": soft_skills,
        "bonus_requirements": bonus_requirements,
        "hidden_requirements": list(template["hidden"]),
        "keywords": keywords,
    }


def _extract_match_soft_skills(text: str, services: MatchingServices) -> list[str]:
    soft_map = {
        "沟通协作": ["沟通", "协作", "跨部门", "汇报", "推动", "协调"],
        "结构化表达": ["结构化", "报告", "PPT", "presentation", "表达"],
        "学习能力": ["学习能力", "自驱", "快速学习", "主动"],
        "抗压执行": ["抗压", "执行力", "细致", "责任心", "高强度"],
    }
    clean = services.normalize_text(text)
    return [label for label, aliases in soft_map.items() if any(services.text_contains(clean, alias) for alias in aliases)]
