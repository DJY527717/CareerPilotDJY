"""Rule-based mock service for the CareerPilot Web API.

The resume/JD matching functions intentionally model an HR screening review:
they look for hard requirements, evidence behind skills, responsibility scope,
business results, writing quality, and uncertainty. No external model is used.
"""

from __future__ import annotations

import re
from typing import Any

from careerpilot_api.schemas import (
    ActionItemSchema,
    JDAnalysis,
    JDAnalyzeResponse,
    JobCardSchema,
    ModeSchema,
    ResumeMatchResponse,
    ResumeParseResponse,
    ResumeProfile,
    ScoreItemSchema,
    UserSummarySchema,
    WorkspaceSchema,
)


USER_SUMMARY = UserSummarySchema(
    name="未设置用户",
    resume_name="尚未上传简历",
    target="尚未设置求职目标",
    location="待确认",
    stage="演示模式",
)

WORKSPACES = [
    WorkspaceSchema(
        key="settings",
        label="设置与资料",
        title="设置与资料",
        subtitle="维护通用档案、当前简历和求职偏好。",
        status="演示模式",
        modes=[
            ModeSchema(key="settings-profile", label="个人档案"),
            ModeSchema(key="settings-resume", label="当前简历"),
            ModeSchema(key="settings-preferences", label="求职偏好"),
        ],
    ),
    WorkspaceSchema(
        key="jd",
        label="JD分析",
        title="JD分析",
        subtitle="分析岗位描述，整理职责、要求、关键词和后续动作。",
        status="待确认",
        modes=[
            ModeSchema(key="jd-single", label="单条JD分析"),
            ModeSchema(key="jd-batch", label="批量JD筛选"),
            ModeSchema(key="jd-monitor", label="招聘趋势"),
        ],
    ),
    WorkspaceSchema(
        key="resume",
        label="简历优化",
        title="简历解析与JD匹配",
        subtitle="从资深HR视角判断简历可信度、经历证据、岗位风险和面试推进建议。",
        status="待确认",
        modes=[
            ModeSchema(key="resume-match", label="简历解析与匹配"),
            ModeSchema(key="resume-rewrite", label="目标JD改简历"),
            ModeSchema(key="resume-gap", label="能力缺口"),
        ],
    ),
    WorkspaceSchema(
        key="decision",
        label="求职决策",
        title="求职决策",
        subtitle="比较机会质量、投递优先级和流程状态。",
        status="待确认",
        modes=[
            ModeSchema(key="decision-offer", label="机会评估"),
            ModeSchema(key="decision-internship", label="岗位比较"),
            ModeSchema(key="decision-pipeline", label="投递管理"),
        ],
    ),
    WorkspaceSchema(
        key="report",
        label="面试报告",
        title="面试报告",
        subtitle="记录面试过程，生成复盘摘要和数据看板。",
        status="待确认",
        modes=[
            ModeSchema(key="report-interview", label="面试记录"),
            ModeSchema(key="report-dashboard", label="数据看板"),
        ],
    ),
]

JOBS = [
    JobCardSchema(
        id="job-1",
        title="数据分析岗位",
        company="示例公司",
        location="待确认",
        track="业务分析",
        match_score=82,
        salary="待确认",
        highlights=["数据看板建设", "业务分析报告", "跨团队协作"],
    ),
    JobCardSchema(
        id="job-2",
        title="产品运营岗位",
        company="示例公司",
        location="待确认",
        track="产品运营",
        match_score=76,
        salary="待确认",
        highlights=["用户研究支持", "项目协作经历", "指标复盘"],
    ),
    JobCardSchema(
        id="job-3",
        title="演示岗位",
        company="示例公司",
        location="待确认",
        track="通用岗位",
        match_score=69,
        salary="待确认",
        highlights=["职责清晰", "要求待确认", "适合演示"],
    ),
]

SCORES = [
    ScoreItemSchema(id="score-1", label="岗位匹配", value=82, note="基于演示岗位信息的占位评分"),
    ScoreItemSchema(id="score-2", label="信息完整度", value=74, note="等待用户补充真实岗位描述"),
    ScoreItemSchema(id="score-3", label="行动优先级", value=80, note="用于前端排序演示"),
]

ACTIONS = [
    ActionItemSchema(id="action-1", title="补充目标岗位", detail="添加一段岗位描述后，可生成更具体的分析结果。", priority="高"),
    ActionItemSchema(id="action-2", title="上传或粘贴简历", detail="当前为演示模式，尚未绑定用户简历。", priority="高"),
    ActionItemSchema(id="action-3", title="确认求职偏好", detail="填写岗位类型、工作方式和其他基本偏好。", priority="中"),
]

SKILL_ALIASES: dict[str, list[str]] = {
    "数据分析": ["数据分析", "业务分析", "经营分析", "指标", "漏斗", "留存", "转化"],
    "SQL": ["SQL", "数据库", "查询", "取数"],
    "Python": ["Python", "pandas", "numpy", "脚本"],
    "Excel": ["Excel", "透视表", "函数", "VLOOKUP"],
    "数据看板": ["数据看板", "dashboard", "BI", "可视化", "报表"],
    "用户研究": ["用户研究", "用户访谈", "问卷", "调研", "需求分析"],
    "产品运营": ["产品运营", "活动运营", "内容运营", "用户运营", "增长"],
    "项目协作": ["项目协作", "跨部门", "协同", "推进", "对齐", "交付"],
    "业务报告": ["业务报告", "分析报告", "复盘", "汇报", "洞察"],
    "英语": ["英语", "英文", "CET-6", "六级", "雅思", "托福", "English"],
}

SECTION_PATTERNS = {
    "education": r"(教育经历|教育背景|学历|院校|专业|Education)",
    "work": r"(工作经历|实习经历|任职|公司|Work Experience|Experience)",
    "project": r"(项目经历|项目经验|Project)",
    "skill": r"(技能|专业技能|工具|Skills)",
    "certificate": r"(证书|认证|Certificate|Certification)",
    "language": r"(语言|Language|英语|English|CET)",
}

ACTION_TERMS = ["负责", "主导", "参与", "搭建", "分析", "推进", "协同", "优化", "输出", "支持", "设计", "完成"]
RESULT_TERMS = ["提升", "降低", "增长", "节省", "转化", "留存", "完成", "上线", "落地", "复盘", "产出"]
VAGUE_TERMS = ["熟悉", "了解", "掌握", "良好", "较强", "具备", "参与多个", "相关经验"]


def get_me() -> UserSummarySchema:
    return USER_SUMMARY


def get_workspaces() -> list[WorkspaceSchema]:
    return WORKSPACES


def get_settings_summary() -> dict[str, object]:
    return {
        "user": USER_SUMMARY,
        "resume_status": "尚未上传简历",
        "target_status": "尚未设置求职目标",
        "profile_status": "待确认",
    }


def get_ranked_jobs() -> list[JobCardSchema]:
    return sorted(JOBS, key=lambda job: job.match_score, reverse=True)


def get_bootstrap_data() -> dict[str, Any]:
    return {
        "user": USER_SUMMARY,
        "workspaces": WORKSPACES,
        "settings": get_settings_summary(),
        "jobs": get_ranked_jobs(),
    }


def parse_resume(resume_text: str, file_name: str | None = None) -> ResumeParseResponse:
    text = normalize_multiline(resume_text)
    lines = split_lines(text)
    profile = ResumeProfile(
        basicInfo=extract_basic_info(lines, file_name),
        education=extract_section_lines(lines, "education"),
        workExperience=extract_section_lines(lines, "work"),
        projects=extract_section_lines(lines, "project"),
        skills=extract_skills(text),
        certificates=extract_section_lines(lines, "certificate"),
        languages=extract_languages(text),
        rawText=text,
    )
    return ResumeParseResponse(profile=profile, parsingNotes=build_resume_notes(profile))


def analyze_jd(jd_text: str) -> JDAnalyzeResponse:
    text = normalize_multiline(jd_text)
    lines = split_lines(text)
    skills = extract_skills(text)
    required = [skill for skill in skills if is_required_context(text, skill)]
    preferred = [skill for skill in skills if skill not in required]
    responsibilities = extract_jd_items(lines, ["职责", "工作内容", "负责", "Responsibilities"])
    experience = extract_requirement_items(text, r"\d+\s*年|经验|实习|校招|应届|experience|years?")
    education = extract_requirement_items(text, r"本科|硕士|博士|大专|学历|专业|bachelor|master|degree")
    analysis = JDAnalysis(
        title=extract_title(lines),
        company=extract_company(lines),
        location=extract_labeled_value(lines, ["地点", "工作地点", "Location"]) or "待确认",
        seniority=extract_seniority(text),
        responsibilities=responsibilities or ["职责描述较少，建议补充岗位日常任务和交付要求。"],
        requiredSkills=required or skills[:5] or ["核心技能待确认"],
        preferredSkills=preferred[:6] or ["加分项待确认"],
        experienceRequirements=experience or ["经验年限待确认"],
        educationRequirements=education or ["学历与专业要求待确认"],
        keywords=unique((skills + responsibilities + experience + education)[:18]),
    )
    return JDAnalyzeResponse(analysis=analysis, analysisNotes=build_jd_notes(analysis, text))


def match_resume_to_jd(resume_text: str, jd_text: str) -> ResumeMatchResponse:
    resume = parse_resume(resume_text).profile
    jd = analyze_jd(jd_text).analysis
    resume_text_clean = resume.rawText
    jd_text_clean = normalize_multiline(jd_text)

    hard_score, hard_gaps = score_hard_requirements(resume_text_clean, jd)
    skill_score, skill_strengths, skill_gaps, unsupported_skills = score_skills(resume, jd)
    experience_score, experience_strengths, experience_gaps = score_experience(resume, jd)
    achievement_score, achievement_notes, achievement_gaps = score_achievements(resume)
    quality_score, quality_strengths, quality_gaps = score_resume_quality(resume)
    keyword_score = score_keyword_overlap(resume_text_clean, jd)

    overall = clamp(round(
        hard_score * 0.18
        + skill_score * 0.22
        + experience_score * 0.22
        + achievement_score * 0.15
        + quality_score * 0.13
        + keyword_score * 0.10
    ))
    risk_level = risk_from_scores(overall, [hard_gaps, skill_gaps, experience_gaps, achievement_gaps, quality_gaps])
    decision = decision_from_score(overall, risk_level)
    risks = build_match_risks(
        hard_gaps=hard_gaps,
        skill_gaps=skill_gaps,
        experience_gaps=experience_gaps,
        achievement_gaps=achievement_gaps,
        quality_gaps=quality_gaps,
        unsupported_skills=unsupported_skills,
    )
    strengths = unique(skill_strengths + experience_strengths + achievement_notes + quality_strengths, 6)
    gaps = unique(hard_gaps + skill_gaps + experience_gaps + achievement_gaps + quality_gaps, 8)

    return ResumeMatchResponse(
        overallScore=overall,
        requirementFitScore=hard_score,
        skillFitScore=skill_score,
        experienceRelevanceScore=experience_score,
        achievementEvidenceScore=achievement_score,
        resumeQualityScore=quality_score,
        keywordMatchScore=keyword_score,
        riskLevel=risk_level,
        hrDecision=decision,
        hrSummary=build_hr_summary(overall, decision, strengths, gaps, risks),
        strengths=strengths or ["当前简历尚未形成明显强匹配证据，需要补充真实经历后再判断。"],
        gaps=gaps or ["暂未发现明显缺口，但仍建议人工核对岗位硬性条件。"],
        risks=risks,
        interviewRecommendation=build_interview_recommendation(decision, risk_level, gaps),
        likelyInterviewQuestions=build_interview_questions(jd, unsupported_skills, gaps),
        resumeRewriteSuggestions=build_rewrite_suggestions(jd, unsupported_skills, gaps),
        recommendedActions=build_recommended_actions(decision, gaps, risks),
    )


def parse_resume_for_api(payload: dict[str, Any]) -> ResumeParseResponse:
    return parse_resume(str(payload.get("resume_text") or payload.get("text") or ""), _optional_str(payload.get("file_name")))


def analyze_jd_for_api(payload: dict[str, Any] | str) -> JDAnalyzeResponse:
    if isinstance(payload, dict):
        text = str(payload.get("jd_text") or payload.get("text") or "")
    else:
        text = str(payload)
    return analyze_jd(text)


def match_resume_for_api(payload: dict[str, Any]) -> ResumeMatchResponse:
    return match_resume_to_jd(str(payload.get("resume_text") or ""), str(payload.get("jd_text") or ""))


def normalize_multiline(value: Any) -> str:
    text = str(value or "").replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    lines = [line.strip() for line in text.split("\n")]
    return "\n".join(line for line in lines if line)


def normalize_inline(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def split_lines(text: str) -> list[str]:
    return [line.strip(" -•*0123456789.、\t") for line in text.splitlines() if line.strip()]


def extract_basic_info(lines: list[str], file_name: str | None) -> dict[str, str]:
    return {
        "candidateName": "匿名示例档案" if lines else "尚未识别",
        "fileName": file_name or "尚未上传简历",
        "targetRole": extract_labeled_value(lines, ["求职目标", "目标岗位", "Target"]) or "尚未设置求职目标",
        "contact": "未在演示结果中展示个人联系方式",
    }


def extract_section_lines(lines: list[str], section: str) -> list[str]:
    pattern = SECTION_PATTERNS[section]
    hits = [line for line in lines if re.search(pattern, line, flags=re.I)]
    contextual = []
    if section in {"work", "project"}:
        contextual = [line for line in lines if any(term in line for term in ACTION_TERMS) and len(line) >= 12]
    if section == "skill":
        contextual = [line for line in lines if any(skill.lower() in line.lower() for aliases in SKILL_ALIASES.values() for skill in aliases)]
    return unique(hits + contextual, 8) or ["尚未识别到明确内容"]


def extract_skills(text: str) -> list[str]:
    lower = text.lower()
    skills = [skill for skill, aliases in SKILL_ALIASES.items() if any(alias.lower() in lower for alias in aliases)]
    return unique(skills, 12)


def extract_languages(text: str) -> list[str]:
    languages = []
    if re.search(r"英语|英文|English|CET|六级|四级|雅思|托福", text, flags=re.I):
        languages.append("英语")
    if re.search(r"日语|Japanese|N[1-5]", text, flags=re.I):
        languages.append("日语")
    return languages or ["尚未识别到明确语言能力"]


def extract_title(lines: list[str]) -> str:
    value = extract_labeled_value(lines, ["岗位名称", "职位名称", "岗位", "职位", "Title", "Role"])
    if value:
        return value
    for line in lines[:5]:
        if re.search(r"分析|运营|产品|项目|专员|助理|实习|经理|岗位", line):
            return line[:40]
    return "演示岗位"


def extract_company(lines: list[str]) -> str:
    return extract_labeled_value(lines, ["公司", "公司名称", "Company"]) or "示例公司"


def extract_labeled_value(lines: list[str], labels: list[str]) -> str:
    label_pattern = "|".join(re.escape(label) for label in labels)
    for line in lines:
        match = re.search(rf"(?:{label_pattern})\s*[:：]\s*(.+)", line, flags=re.I)
        if match:
            return normalize_inline(match.group(1))[:80]
    return ""


def extract_seniority(text: str) -> str:
    if re.search(r"实习|intern", text, flags=re.I):
        return "实习/入门"
    if re.search(r"应届|校招|new graduate", text, flags=re.I):
        return "应届/校招"
    if re.search(r"3\s*年|5\s*年|资深|高级|senior", text, flags=re.I):
        return "有经验/资深"
    return "待确认"


def extract_jd_items(lines: list[str], labels: list[str]) -> list[str]:
    output = []
    label_pattern = "|".join(labels)
    for line in lines:
        if re.search(label_pattern, line, flags=re.I) or any(term in line for term in ACTION_TERMS):
            output.append(clean_item(line))
    return unique([item for item in output if len(item) >= 6], 8)


def extract_requirement_items(text: str, pattern: str) -> list[str]:
    output = []
    for line in split_lines(text):
        if re.search(pattern, line, flags=re.I):
            output.append(clean_item(line))
    return unique(output, 6)


def is_required_context(text: str, skill: str) -> bool:
    aliases = SKILL_ALIASES.get(skill, [skill])
    for line in split_lines(text):
        if any(alias.lower() in line.lower() for alias in aliases) and re.search(r"必须|熟练|要求|掌握|required|must", line, flags=re.I):
            return True
    return False


def build_resume_notes(profile: ResumeProfile) -> list[str]:
    notes = []
    if not profile.rawText:
        notes.append("尚未提供简历文本，当前仅返回空结构。")
    if profile.skills and profile.skills != ["尚未识别到明确内容"]:
        notes.append("已识别技能标签，但仍需判断这些技能是否在经历中有证据支撑。")
    if has_quantified_result(profile.rawText):
        notes.append("简历中出现量化结果，成果说服力相对更好。")
    else:
        notes.append("暂未识别到清晰量化结果，HR会关注职责是否真正产生业务结果。")
    if profile.workExperience == ["尚未识别到明确内容"] and profile.projects == ["尚未识别到明确内容"]:
        notes.append("工作、实习或项目经历不够明确，岗位匹配判断会有较高不确定性。")
    return notes


def build_jd_notes(analysis: JDAnalysis, text: str) -> list[str]:
    notes = []
    if not text:
        notes.append("尚未提供JD文本，当前返回演示结构。")
    if analysis.requiredSkills == ["核心技能待确认"]:
        notes.append("JD未清晰写出硬性技能，后续匹配需要人工确认核心要求。")
    if analysis.experienceRequirements == ["经验年限待确认"]:
        notes.append("未识别到明确年限要求，硬性条件评分会降低权重。")
    notes.append("已将职责、技能、经验和学历拆开，便于后续判断简历证据是否支撑岗位。")
    return notes


def score_hard_requirements(resume_text: str, jd: JDAnalysis) -> tuple[int, list[str]]:
    score = 78
    gaps = []
    jd_requirements = " ".join(jd.experienceRequirements + jd.educationRequirements + [jd.location, jd.seniority])
    if re.search(r"本科|bachelor", jd_requirements, flags=re.I) and not re.search(r"本科|硕士|博士|bachelor|master|degree", resume_text, flags=re.I):
        score -= 18
        gaps.append("JD有本科及以上或明确学历要求，但简历中没有稳定可识别的学历信息。")
    if re.search(r"硕士|master", jd_requirements, flags=re.I) and not re.search(r"硕士|博士|master|PhD", resume_text, flags=re.I):
        score -= 18
        gaps.append("JD倾向硕士及以上背景，简历未提供相应学历证据。")
    if re.search(r"英语|English|CET|六级", jd_requirements, flags=re.I) and not re.search(r"英语|English|CET|六级|雅思|托福", resume_text, flags=re.I):
        score -= 12
        gaps.append("JD提到语言能力，但简历未写出可验证的语言水平或使用场景。")
    if re.search(r"\d+\s*年", jd_requirements) and not re.search(r"\d+\s*年|实习|项目", resume_text):
        score -= 15
        gaps.append("JD有经验要求，简历未清楚写出经历时长或对应阶段。")
    return clamp(score), gaps


def score_skills(resume: ResumeProfile, jd: JDAnalysis) -> tuple[int, list[str], list[str], list[str]]:
    required = [skill for skill in jd.requiredSkills + jd.preferredSkills if skill in SKILL_ALIASES]
    if not required:
        required = [skill for skill in jd.keywords if skill in SKILL_ALIASES]
    resume_skills = set(resume.skills)
    experience_text = "\n".join(resume.workExperience + resume.projects)
    strengths, gaps, unsupported = [], [], []
    matched_with_evidence = 0
    matched_only_listed = 0
    for skill in unique(required, 10):
        if skill not in resume_skills:
            gaps.append(f"JD关注{skill}，但简历中没有明确出现。")
            continue
        if skill_has_evidence(skill, experience_text):
            matched_with_evidence += 1
            strengths.append(f"{skill}不仅出现在技能标签中，也能在项目或经历片段里找到支撑。")
        else:
            matched_only_listed += 1
            unsupported.append(skill)
            gaps.append(f"{skill}目前更像技能栏罗列，缺少项目任务、责任边界或结果证明。")
    denominator = max(1, len(unique(required, 10)))
    score = round((matched_with_evidence * 100 + matched_only_listed * 58) / denominator)
    return clamp(score), strengths, gaps, unsupported


def score_experience(resume: ResumeProfile, jd: JDAnalysis) -> tuple[int, list[str], list[str]]:
    experience_text = "\n".join(resume.workExperience + resume.projects)
    jd_terms = jd.responsibilities + jd.keywords
    hits = [term for term in jd_terms if term and normalize_inline(term)[:8] in experience_text]
    action_hit_count = sum(1 for term in ACTION_TERMS if term in experience_text)
    score = 42 + min(len(hits) * 8, 28) + min(action_hit_count * 4, 20)
    strengths, gaps = [], []
    if hits:
        strengths.append("经历内容与JD职责存在直接交集，HR能看到一定岗位相关性。")
    if action_hit_count >= 3:
        strengths.append("简历使用了行动动词，能看出候选人承担过具体推进或交付动作。")
    if not hits:
        gaps.append("工作、实习或项目经历与JD职责的直接连接不够清楚。")
    if action_hit_count < 2:
        gaps.append("经历描述缺少具体动作，HR难以判断候选人到底负责了哪一部分。")
    return clamp(score), strengths, gaps


def score_achievements(resume: ResumeProfile) -> tuple[int, list[str], list[str]]:
    text = resume.rawText
    quantified = len(re.findall(r"\d+%|\d+\s*(?:人|次|天|周|月|年|个|份|条|小时|万元|元)", text))
    result_terms = sum(1 for term in RESULT_TERMS if term in text)
    score = 38 + min(quantified * 12, 36) + min(result_terms * 5, 20)
    notes, gaps = [], []
    if quantified:
        notes.append("简历包含量化数字，成果可信度优于纯职责描述。")
    if result_terms:
        notes.append("经历中出现结果导向表达，便于HR判断业务产出。")
    if quantified == 0:
        gaps.append("成果缺少数字或业务指标，建议补充规模、效率、转化、节省时间等量化信息。")
    if result_terms == 0:
        gaps.append("经历偏过程描述，未清楚说明最终产出和业务影响。")
    return clamp(score), notes, gaps


def score_resume_quality(resume: ResumeProfile) -> tuple[int, list[str], list[str]]:
    text = resume.rawText
    if not text:
        return 20, [], ["尚未提供简历文本，无法判断表达质量。"]
    line_count = len(split_lines(text))
    vague_count = sum(text.count(term) for term in VAGUE_TERMS)
    section_count = sum(1 for value in [resume.education, resume.workExperience, resume.projects, resume.skills] if value and value != ["尚未识别到明确内容"])
    score = 44 + min(line_count * 2, 18) + section_count * 8 - min(vague_count * 5, 20)
    strengths, gaps = [], []
    if section_count >= 3:
        strengths.append("简历结构包含教育、经历、项目或技能模块，基础信息可读性较好。")
    if vague_count <= 2:
        strengths.append("空泛形容词占比不高，表达相对克制。")
    if section_count < 3:
        gaps.append("简历结构不够完整，HR需要更多上下文才能判断候选人画像。")
    if vague_count > 2:
        gaps.append("简历存在较多空泛词，建议用具体任务和结果替代“熟悉、了解、具备”等表述。")
    return clamp(score), strengths, gaps


def score_keyword_overlap(resume_text: str, jd: JDAnalysis) -> int:
    keywords = [keyword for keyword in jd.keywords if keyword and len(keyword) <= 24]
    if not keywords:
        return 45
    hits = sum(1 for keyword in keywords if keyword.lower() in resume_text.lower())
    return clamp(round(hits / len(keywords) * 100))


def skill_has_evidence(skill: str, experience_text: str) -> bool:
    aliases = SKILL_ALIASES.get(skill, [skill])
    if not any(alias.lower() in experience_text.lower() for alias in aliases):
        return False
    return any(term in experience_text for term in ACTION_TERMS + RESULT_TERMS) or has_quantified_result(experience_text)


def has_quantified_result(text: str) -> bool:
    return bool(re.search(r"\d+%|\d+\s*(?:人|次|天|周|月|年|个|份|条|小时|万元|元)", text))


def build_match_risks(
    *,
    hard_gaps: list[str],
    skill_gaps: list[str],
    experience_gaps: list[str],
    achievement_gaps: list[str],
    quality_gaps: list[str],
    unsupported_skills: list[str],
) -> list[str]:
    risks = []
    if hard_gaps:
        risks.append("硬性条件存在不确定性：学历、经验、语言或地点信息需要进一步核验。")
    if unsupported_skills:
        risks.append(f"核心技能证据不足：{', '.join(unsupported_skills[:4])}目前没有被经历充分证明。")
    if experience_gaps:
        risks.append("经历与岗位职责的映射不够直接，可能被HR判断为方向相关但深度不足。")
    if achievement_gaps:
        risks.append("成果说服力偏弱，缺少量化结果会影响简历可信度和面试邀约概率。")
    if quality_gaps:
        risks.append("简历表达质量仍需打磨，避免看起来像岗位职责堆砌。")
    return risks or ["暂无明显高风险，但建议在投递前人工核对岗位硬性要求。"]


def risk_from_scores(overall: int, gap_groups: list[list[str]]) -> str:
    gap_count = sum(len(group) for group in gap_groups)
    if overall >= 78 and gap_count <= 3:
        return "low"
    if overall < 58 or gap_count >= 8:
        return "high"
    return "medium"


def decision_from_score(score: int, risk_level: str) -> str:
    if score >= 76 and risk_level == "low":
        return "recommend_interview"
    if score >= 58 and risk_level in {"low", "medium"}:
        return "maybe"
    return "not_recommended"


def build_hr_summary(score: int, decision: str, strengths: list[str], gaps: list[str], risks: list[str]) -> str:
    if decision == "recommend_interview":
        lead = "从HR初筛角度看，这份简历已经能支撑进入面试："
    elif decision == "maybe":
        lead = "从HR初筛角度看，这份简历有一定相关性，但还不足以稳定通过："
    else:
        lead = "从HR初筛角度看，当前版本不建议直接投递或推进面试："
    strength = strengths[0] if strengths else "暂未看到足够强的岗位证据"
    gap = gaps[0] if gaps else risks[0]
    return f"{lead}综合分{score}。主要依据是{strength}；主要顾虑是{gap}"


def build_interview_recommendation(decision: str, risk_level: str, gaps: list[str]) -> str:
    if decision == "recommend_interview":
        return "建议进入面试，但面试前应准备技能证据、项目边界和量化成果的追问答案。"
    if decision == "maybe":
        return f"建议先补强简历再投递。当前风险等级为{risk_level}，优先处理：{gaps[0] if gaps else '核心经历证据'}"
    return "不建议用当前版本直接投递。先重写经历证据和岗位匹配点，否则容易在HR初筛阶段被判断为不够匹配。"


def build_interview_questions(jd: JDAnalysis, unsupported_skills: list[str], gaps: list[str]) -> list[str]:
    questions = []
    for skill in unsupported_skills[:3]:
        questions.append(f"你在什么项目中实际使用过{skill}？你的具体责任、产出和结果是什么？")
    if jd.responsibilities:
        questions.append(f"JD提到“{jd.responsibilities[0]}”，你过往经历中最接近的案例是什么？")
    if gaps:
        questions.append("简历里这段经历的业务目标、你的责任边界和最终结果分别是什么？")
    return unique(questions, 5) or ["请说明一个最能证明岗位匹配度的项目案例。"]


def build_rewrite_suggestions(jd: JDAnalysis, unsupported_skills: list[str], gaps: list[str]) -> list[str]:
    suggestions = []
    for skill in unsupported_skills[:4]:
        suggestions.append(f"把{skill}从技能清单移到相关项目中，用“动作-方法-结果”写出证据。")
    if jd.responsibilities:
        suggestions.append(f"围绕JD核心职责“{jd.responsibilities[0]}”重排经历，把最相关案例放在前两条。")
    if any("量化" in gap or "成果" in gap for gap in gaps):
        suggestions.append("每段经历至少补一个指标：规模、频次、周期、效率提升、转化变化或交付数量。")
    suggestions.append("删除或改写空泛表述，用具体工具、协作对象、交付物和业务影响替代。")
    return unique(suggestions, 6)


def build_recommended_actions(decision: str, gaps: list[str], risks: list[str]) -> list[str]:
    actions = []
    if decision != "recommend_interview":
        actions.append("先不要批量投递，用目标JD重写简历前三段经历。")
    actions.append("为每个JD核心技能补一条真实经历证据，避免只在技能栏罗列。")
    if gaps:
        actions.append(f"优先修复最高影响缺口：{gaps[0]}")
    if risks:
        actions.append(f"面试前准备风险解释：{risks[0]}")
    actions.append("完成改写后再次运行匹配评估，确认HR判断是否从maybe提升到recommend_interview。")
    return unique(actions, 5)


def clean_item(value: str) -> str:
    return normalize_inline(re.sub(r"^(岗位职责|工作内容|任职要求|要求|职责|[-*•\d.、]+)\s*[:：]?", "", value, flags=re.I))


def unique(items: list[str], limit: int = 99) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for item in items:
        clean = normalize_inline(item)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        output.append(clean)
        if len(output) >= limit:
            break
    return output


def clamp(value: int, minimum: int = 0, maximum: int = 100) -> int:
    return max(minimum, min(maximum, int(value)))


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
