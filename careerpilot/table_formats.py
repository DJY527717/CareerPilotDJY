from typing import Any, Callable

import pandas as pd


PUBLIC_EXPORT_HIDDEN_COLUMNS = {
    "JD原文",
    "原文片段",
    "fingerprint",
    "区域",
    "地域优先级",
    "地域修正",
    "泛ESG风险",
    "来源",
    "页面",
    "最佳意向",
    "最佳意向分",
    "读取质量",
    "技术匹配分",
    "语义相似分",
    "简历匹配分",
    "JD简历相似分",
    "岗位加分",
    "目标偏好加分",
    "风险扣分",
    "行业偏好分",
    "城市偏好分",
    "筛选评分依据",
    "筛选证据",
    "重复清理",
    "置信度",
    "高价值词命中",
    "低价值风险词命中",
}


USER_HIDDEN_TABLE_COLUMNS = {
    "记录ID",
    "信息集ID",
    "id",
    "fingerprint",
    "JD原文",
    "原文片段",
    "raw_text",
    "text",
    "path",
    "页面",
    "来源路径",
    "技术匹配分",
    "语义相似分",
    "简历匹配分",
    "JD简历相似分",
    "岗位加分",
    "目标偏好加分",
    "风险扣分",
    "行业偏好分",
    "城市偏好分",
    "筛选评分依据",
    "筛选证据",
    "重复清理",
    "置信度",
    "高价值词命中",
    "低价值风险词命中",
    "最佳意向",
    "最佳意向分",
    "地域修正",
    "地域优先级",
    "读取质量",
}


USER_TABLE_RENAMES = {
    "意向匹配度": "推荐评分",
    "match_score": "推荐评分",
    "综合分": "推荐评分",
    "匹配度": "匹配度",
    "company": "公司",
    "job_title": "岗位",
    "url": "链接",
    "salary": "薪资",
    "location": "地点",
    "category": "岗位方向",
    "岗位分类": "岗位方向",
    "分类": "岗位方向",
    "is_high_value": "优先关注",
    "高价值": "优先关注",
    "高价值岗位": "优先关注",
    "岗位价值": "优先关注",
    "is_generic_esg": "待核实风险",
    "低价值风险": "待核实风险",
    "风险标签": "风险提示",
    "技能关键词": "岗位关键词",
    "简历能力命中": "已覆盖能力",
    "未覆盖简历能力": "待补充能力",
    "薪资判断": "薪资备注",
    "岗位推荐分": "岗位推荐分",
    "简历匹配分": "简历匹配分",
    "职业方向分": "职业方向分",
    "偏好匹配分": "偏好匹配分",
    "薪资匹配分": "薪资匹配分",
    "薪资匹配": "薪资匹配",
    "薪资提醒": "薪资提醒",
    "投递建议": "建议",
    "下一步动作": "下一步",
    "是否招实习": "招实习",
    "是否招应届生": "招应届",
    "应届生": "招应届",
    "是否新发现": "新增岗位",
    "公司层级": "公司层级",
    "匹配原因": "推荐理由",
    "筛选评分依据": "评分说明",
    "筛选证据": "关键依据",
    "applied": "已投递",
    "interview_status": "面试状态",
    "offer_status": "Offer状态",
    "queue_date": "计划日期",
    "next_action": "下一步",
    "notes": "备注",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}


LONG_TEXT_COLUMNS = {
    "岗位",
    "岗位关键词",
    "已覆盖能力",
    "待补充能力",
    "风险提示",
    "推荐理由",
    "评分说明",
    "关键依据",
    "下一步",
    "备注",
    "薪资备注",
}

MEDIUM_TEXT_COLUMNS = {
    "公司",
    "岗位方向",
    "薪资",
    "地点",
    "面试状态",
    "Offer状态",
}


LEGACY_COLUMN_ALIASES = {
    "推荐评分": "意向匹配度",
    "综合分": "意向匹配度",
    "match_score": "意向匹配度",
    "岗位价值分": "岗位加分",
    "价值评分": "岗位加分",
    "风险分": "风险扣分",
    "岗位方向": "岗位分类",
    "分类": "岗位分类",
    "优先关注": "高价值",
    "高价值岗位": "高价值",
    "岗位价值": "高价值",
    "待核实风险": "低价值风险",
    "泛ESG风险": "低价值风险",
    "推荐理由": "匹配原因",
    "评分说明": "筛选评分依据",
    "关键依据": "筛选证据",
    "已覆盖能力": "简历能力命中",
    "待补充能力": "未覆盖简历能力",
    "岗位关键词": "技能关键词",
    "薪资备注": "薪资判断",
    "建议": "投递建议",
    "下一步": "下一步动作",
    "招实习": "是否招实习",
    "招应届": "应届生",
}


def normalize_legacy_table_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df
    output = df.copy()
    for legacy, current in LEGACY_COLUMN_ALIASES.items():
        if legacy not in output.columns or legacy == current:
            continue
        if current not in output.columns:
            output[current] = output[legacy]
        else:
            current_values = output[current]
            legacy_values = output[legacy]
            blank_mask = current_values.isna() | current_values.astype(str).str.strip().isin(["", "nan", "None"])
            output.loc[blank_mask, current] = legacy_values.loc[blank_mask]
        output = output.drop(columns=[legacy])
    return output


def public_export_df(df: pd.DataFrame) -> pd.DataFrame:
    output = normalize_legacy_table_columns(df)
    output = output.drop(columns=[col for col in PUBLIC_EXPORT_HIDDEN_COLUMNS if col in output.columns], errors="ignore")
    output = output.rename(columns={key: value for key, value in USER_TABLE_RENAMES.items() if key in output.columns})
    if "链接" in output.columns:
        output = output[[col for col in output.columns if col != "链接"] + ["链接"]]
    return output


def user_table_df(df: pd.DataFrame, columns: list[str] | None = None, *, hide_scores: bool = True) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    table = normalize_legacy_table_columns(df)
    if columns:
        table = table[[col for col in columns if col in table.columns]]
    hidden = set(USER_HIDDEN_TABLE_COLUMNS)
    if hide_scores:
        visible_score_columns = {"意向匹配度", "岗位推荐分", "简历匹配分", "职业方向分", "偏好匹配分", "薪资匹配分", "综合分", "价值评分", "匹配度"}
        hidden.difference_update(visible_score_columns)
        hidden.update(col for col in table.columns if str(col).endswith("分") and col not in visible_score_columns)
        hidden.update(
            col
            for col in table.columns
            if "相似" in str(col) or ("命中" in str(col) and col not in {"简历能力命中"})
        )
    table = table.drop(columns=[col for col in hidden if col in table.columns], errors="ignore")
    table = table.rename(columns={key: value for key, value in USER_TABLE_RENAMES.items() if key in table.columns})
    if "链接" in table.columns:
        table = table[[col for col in table.columns if col != "链接"] + ["链接"]]
    return table


def user_table_row_height(df: pd.DataFrame, *, normalize_text: Callable[[str], str]) -> int:
    if df is None or df.empty:
        return 28
    visible = df.drop(columns=[col for col in ["链接"] if col in df.columns], errors="ignore")
    text_lengths = visible.astype(str).map(lambda value: len(normalize_text(value))).to_numpy().flatten()
    max_len = int(max(text_lengths)) if len(text_lengths) else 0
    if max_len >= 120:
        return 44
    if max_len >= 56:
        return 38
    if max_len >= 24:
        return 32
    return 28


def user_table_height(df: pd.DataFrame, row_height: int) -> int | str:
    if df is None or df.empty:
        return "auto"
    return min(420, 36 + max(1, len(df)) * row_height)


def user_table_column_width(series: pd.Series, column: str, *, normalize_text: Callable[[str], str]) -> str:
    if column == "链接":
        return "medium"
    if column in LONG_TEXT_COLUMNS:
        return "large"
    if column in MEDIUM_TEXT_COLUMNS:
        return "medium"
    max_len = int(series.astype(str).map(lambda value: len(normalize_text(value))).max()) if len(series) else 0
    if max_len >= 36:
        return "large"
    if max_len >= 12:
        return "medium"
    return "small"


def user_table_column_config(
    df: pd.DataFrame,
    *,
    streamlit_module: Any,
    normalize_text: Callable[[str], str],
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config: dict[str, Any] = {}
    short_center_columns = {
        "序号",
        "推荐评分",
        "岗位推荐分",
        "简历匹配分",
        "职业方向分",
        "偏好匹配分",
        "薪资匹配分",
        "薪资匹配",
        "匹配度",
        "建议",
        "类型",
        "招实习",
        "招应届",
        "薪资",
        "地点",
        "优先关注",
        "待核实风险",
        "省份",
        "标准城市",
        "学历",
        "经验",
        "计划日期",
        "已投递",
        "面试状态",
        "Offer状态",
        "新增岗位",
    }
    for column in df.columns:
        width = user_table_column_width(df[column], str(column), normalize_text=normalize_text)
        max_len = int(df[column].astype(str).map(lambda value: len(normalize_text(value))).max()) if len(df[column]) else 0
        alignment = "center" if str(column) in short_center_columns or max_len <= 10 else "left"
        if column == "链接":
            config[column] = streamlit_module.column_config.LinkColumn(column, width=width, alignment="center")
        elif pd.api.types.is_numeric_dtype(df[column]):
            config[column] = streamlit_module.column_config.NumberColumn(column, width=width, alignment="center")
        else:
            config[column] = streamlit_module.column_config.TextColumn(column, width=width, alignment=alignment)
    if extra:
        config.update(extra)
    return config
