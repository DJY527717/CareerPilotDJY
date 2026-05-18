import io
from typing import Any, Callable

import pandas as pd


def _requirement_name(item: dict[str, Any]) -> str:
    return str(item.get("requirement") or item.get("jd_requirement") or item.get("target_requirement") or "").strip()


def _resume_overall_score(resume_match: dict[str, Any] | None) -> Any:
    if not resume_match:
        return "待分析"
    # Legacy fallback is only for old saved records that predate overall_score.
    return resume_match["overall_score"] if "overall_score" in resume_match else resume_match.get("score", "")


def _resume_final_rank_score(resume_match: dict[str, Any] | None) -> Any:
    if not resume_match:
        return ""
    # Legacy fallback is only for old saved records that predate final_rank_score/overall_score.
    if "final_rank_score" in resume_match:
        return resume_match["final_rank_score"]
    if "overall_score" in resume_match:
        return resume_match["overall_score"]
    return resume_match.get("score", "")


def _matched_evidence_labels(resume_match: dict[str, Any]) -> list[str]:
    labels = []
    for item in resume_match.get("matched_evidence", []) or []:
        requirement = _requirement_name(item)
        evidence = str(item.get("resume_evidence") or "").strip()
        if requirement and evidence:
            labels.append(f"{requirement}: {evidence}")
        elif requirement:
            labels.append(requirement)
    # Legacy fallback is only for old saved records that lack matched_evidence.
    if "matched_evidence" not in resume_match:
        labels = [str(item).strip() for item in resume_match.get("matched_skills", []) or []]
    return list(dict.fromkeys(item for item in labels if item))


def _missing_requirement_labels(resume_match: dict[str, Any]) -> list[str]:
    labels = [_requirement_name(item) for item in resume_match.get("missing_requirements", []) or []]
    # Legacy fallback is only for old saved records that lack missing_requirements.
    if "missing_requirements" not in resume_match:
        labels = [str(item).strip() for item in resume_match.get("missing_skills", []) or []]
    return list(dict.fromkeys(item for item in labels if item))


def _weak_requirement_labels(resume_match: dict[str, Any]) -> list[str]:
    labels = [_requirement_name(item) for item in resume_match.get("weak_requirements", []) or []]
    return list(dict.fromkeys(item for item in labels if item))


def build_report_frames(
    *,
    session_state: Any,
    get_active_profile: Callable[[], dict[str, Any]],
    get_active_resume: Callable[[], dict[str, Any]],
    build_job_action_plan: Callable[[dict[str, Any], dict[str, Any] | None], dict[str, Any]],
    normalize_resume_lines: Callable[[str], list[str]],
    target_preferences_text: Callable[[], str],
    parse_resume_content: Callable[[str], dict[str, Any]],
    resume_parse_quality: Callable[[dict[str, Any]], dict[str, Any]],
    public_export_df: Callable[[pd.DataFrame], pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    jd_analysis = session_state.get("jd_analysis")
    resume_match = session_state.get("resume_match")
    gap_analysis = session_state.get("gap_analysis")
    interview_analysis = session_state.get("interview_analysis")
    internship_analysis = session_state.get("internship_analysis")
    custom_resume = session_state.get("custom_resume")
    recruitment_monitor = session_state.get("recruitment_monitor")
    offer_prediction = session_state.get("offer_prediction")
    batch_jd_analysis = session_state.get("batch_jd_analysis")

    frames: dict[str, pd.DataFrame] = {}
    active_profile = get_active_profile()
    active_resume = get_active_resume()
    target_meta = session_state.get("target_jd_meta", {})
    if jd_analysis:
        action_plan = build_job_action_plan(jd_analysis, resume_match)
        basic = jd_analysis.get("basic", {})
        frames["投递包摘要"] = pd.DataFrame(
            [
                {
                    "目标岗位": basic.get("岗位名", ""),
                    "目标公司": basic.get("公司名", ""),
                    "来源": target_meta.get("source", ""),
                    "岗位方向": jd_analysis.get("category", ""),
                    "优先关注": "是" if jd_analysis.get("value", {}).get("is_high_value") else "待确认",
                    "待核实风险": "是" if jd_analysis.get("value", {}).get("is_generic_esg") else "否",
                    "简历版本": active_resume.get("name", ""),
                    "简历匹配度": _resume_overall_score(resume_match),
                    "推荐评分": _resume_final_rank_score(resume_match),
                    "投递建议": action_plan["decision"],
                    "下一步": " / ".join(action_plan["actions"]),
                    "面试准备": " / ".join(action_plan["interview_focus"]),
                }
            ]
        )
    frames["目标意向"] = pd.DataFrame([{"目标意向名称": active_profile["name"], "目标意向内容": active_profile["content"]}])
    frames["目标偏好"] = pd.DataFrame([{"偏好": line} for line in normalize_resume_lines(target_preferences_text())])
    if active_resume.get("content"):
        parsed_resume = parse_resume_content(active_resume["content"])
        quality = resume_parse_quality(parsed_resume)
        frames["当前简历状态"] = pd.DataFrame(
            [
                {
                    "简历名称": active_resume.get("name", ""),
                    "更新时间": active_resume.get("updated_at", ""),
                    "内容行数": len(parsed_resume.get("lines", [])),
                    "简历栏目数": len(parsed_resume.get("sections", {})),
                    "已识别技能": " / ".join(parsed_resume.get("skills", [])[:12]),
                    "质量": quality["label"],
                    "质量说明": quality["detail"],
                }
            ]
        )
    if jd_analysis:
        frames["岗位基本信息"] = pd.DataFrame([jd_analysis["basic"]])
        frames["岗位关键词"] = jd_analysis["skills"]
        frames["岗位判断"] = pd.DataFrame(
            [
                {
                    "岗位方向": jd_analysis.get("category", ""),
                    "优先关注依据": jd_analysis.get("value", {}).get("high_score", ""),
                    "风险依据": jd_analysis.get("value", {}).get("low_score", ""),
                    "判断": jd_analysis.get("value", {}).get("label", ""),
                }
            ]
        )
    if resume_match:
        frames["简历匹配"] = pd.DataFrame(
            [
                {
                    "匹配度": _resume_overall_score(resume_match),
                    "推荐评分": _resume_final_rank_score(resume_match),
                    "已匹配证据": " / ".join(_matched_evidence_labels(resume_match)),
                    "待补充要求": " / ".join(_missing_requirement_labels(resume_match)),
                    "证据偏弱要求": " / ".join(_weak_requirement_labels(resume_match)),
                }
            ]
        )
    if batch_jd_analysis is not None and not batch_jd_analysis.empty:
        frames["批量岗位分析"] = public_export_df(batch_jd_analysis)
    if custom_resume:
        frames["定制简历"] = pd.DataFrame(
            [
                {
                    "目标岗位": custom_resume["job_title"],
                    "岗位方向": custom_resume["category"],
                    "关键词": custom_resume["keyword_line"],
                    "直接可用版本": custom_resume.get("ready_resume_text", ""),
                    "摘要": custom_resume["summary"],
                    "核心能力": " / ".join(custom_resume.get("skills_section", [])),
                    "Bullet": " / ".join(custom_resume.get("experience_bullets", custom_resume["bullets"])),
                    "投递说明": custom_resume.get("application_pitch", ""),
                    "风险提醒": " / ".join(custom_resume["risk_notes"]),
                }
            ]
        )
    if recruitment_monitor is not None and not recruitment_monitor.empty:
        frames["行业招聘监测"] = public_export_df(recruitment_monitor)
    if offer_prediction:
        frames["Offer预测"] = pd.DataFrame(
            [
                {
                    "简历通过率": offer_prediction["简历通过率"],
                    "进入面试概率": offer_prediction["进入面试概率"],
                    "拿Offer概率": offer_prediction["拿 offer 概率"],
                    "推进判断": offer_prediction.get("decision_tier", ""),
                    "主要瓶颈": offer_prediction.get("bottleneck", ""),
                    "最弱阶段": offer_prediction.get("weakest_stage", ""),
                    "阶段诊断": " / ".join(
                        f"{name}:{score}" for name, score in offer_prediction.get("stage_scores", {}).items()
                    ),
                    "可信度": offer_prediction.get("confidence", ""),
                    "判断范围": offer_prediction.get("scope_note", ""),
                    "岗位族": " / ".join(offer_prediction.get("career_families", [])),
                    "风险因素": " / ".join(offer_prediction.get("risk_flags", [])),
                    "待补充项数": offer_prediction["shortcomings"],
                    "预测依据": " / ".join(offer_prediction["drivers"]),
                    "提升建议": " / ".join(offer_prediction["actions"]),
                    "投递步骤": " / ".join(offer_prediction.get("application_steps", [])),
                }
            ]
        )
    if gap_analysis:
        if gap_analysis.get("action_rows"):
            frames["不足行动包"] = pd.DataFrame(gap_analysis["action_rows"])
        if gap_analysis.get("weekly_plan"):
            frames["一周补强计划"] = pd.DataFrame(gap_analysis["weekly_plan"])
        gap_rows = []
        for category, items in gap_analysis["current_gaps"].items():
            for item in items:
                gap_rows.append({"分类": category, "不足项": item})
        frames["不足清单"] = pd.DataFrame(gap_rows)
        frames["努力方向"] = pd.DataFrame(gap_analysis["priorities"], columns=["优先级", "建议"])
    if interview_analysis:
        rows = []
        rows.append(
            {
                "分类": "准备成熟度",
                "面经问题": f"{interview_analysis.get('readiness_score', 0)} / 100 - {interview_analysis.get('readiness_label', '')}",
            }
        )
        if interview_analysis.get("readiness_breakdown"):
            rows.append(
                {
                    "分类": "成熟度拆解",
                    "面经问题": " / ".join(
                        f"{name}:{score}" for name, score in interview_analysis.get("readiness_breakdown", {}).items()
                    ),
                }
            )
        for focus in interview_analysis.get("preparation_focus", []):
            rows.append({"分类": "优先补齐", "面经问题": focus})
        for answer in interview_analysis.get("answer_templates", []):
            rows.append({"分类": "回答框架", "面经问题": answer})
        for category, questions in interview_analysis["buckets"].items():
            for question in questions:
                rows.append({"分类": category, "面经问题": question})
        for category, questions in interview_analysis["generated_questions"].items():
            for question in questions:
                rows.append({"分类": category, "面经问题": question})
        frames["面试问题"] = pd.DataFrame(rows)
    if internship_analysis:
        frames["实习评估"] = pd.DataFrame(
            [
                {
                    "实习价值评分": internship_analysis["score"],
                    "结论": internship_analysis["verdict"],
                    "决策建议": internship_analysis["decision"],
                    "识别方向": " / ".join(internship_analysis.get("signal_profile", {}).get("career_families", [])),
                    "有利原因": " / ".join(internship_analysis["reasons"]),
                    "风险点": " / ".join(internship_analysis["risks"]),
                    "建议产出": " / ".join(internship_analysis["recommended_outputs"]),
                    "沟通话术": " / ".join(internship_analysis.get("negotiation_script", [])),
                    "第一周计划": " / ".join(internship_analysis.get("first_week_plan", [])),
                }
            ]
        )
        frames["实习各项判断"] = internship_analysis["dimension_scores"]
    return frames


def build_excel_report(*, frames: dict[str, pd.DataFrame]) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if not frames:
            pd.DataFrame([{"提示": "暂无分析结果"}]).to_excel(writer, sheet_name="报告", index=False)
        for name, df in frames.items():
            safe_name = name[:31]
            df.to_excel(writer, sheet_name=safe_name, index=False)
    return output.getvalue()


def build_pdf_report(*, frames: dict[str, pd.DataFrame]) -> bytes | None:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

        pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=16 * mm, leftMargin=16 * mm, topMargin=16 * mm, bottomMargin=16 * mm)
        styles = getSampleStyleSheet()
        for style in styles.byName.values():
            style.fontName = "STSong-Light"
        story = [Paragraph("CareerPilot 投递包", styles["Title"]), Spacer(1, 8)]

        for name, df in frames.items():
            story.append(Paragraph(name, styles["Heading2"]))
            show_df = df.copy().astype(str).head(20)
            if show_df.empty:
                story.append(Paragraph("暂无数据", styles["BodyText"]))
                story.append(Spacer(1, 6))
                continue
            table_data = [show_df.columns.tolist()] + show_df.values.tolist()
            table = Table(table_data, repeatRows=1)
            table.setStyle(
                TableStyle(
                    [
                        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EAF2F8")),
                        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                        ("FONTNAME", (0, 0), (-1, -1), "STSong-Light"),
                        ("FONTSIZE", (0, 0), (-1, -1), 8),
                        ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ]
                )
            )
            story.append(table)
            story.append(Spacer(1, 8))
        doc.build(story)
        return buffer.getvalue()
    except Exception:
        return None
