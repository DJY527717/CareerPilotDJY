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
    "岗位价值分",
    "风险分",
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
    "岗位价值分",
    "风险分",
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
    "意向匹配度": "综合分",
    "match_score": "综合分",
    "company": "公司",
    "job_title": "岗位",
    "url": "链接",
    "salary": "薪资",
    "location": "地点",
    "category": "分类",
    "is_high_value": "高价值",
    "is_generic_esg": "低价值风险",
    "applied": "已投递",
    "interview_status": "面试状态",
    "offer_status": "Offer状态",
    "queue_date": "队列日期",
    "next_action": "下一步动作",
    "notes": "备注",
    "created_at": "创建时间",
    "updated_at": "更新时间",
}


def public_export_df(df: pd.DataFrame) -> pd.DataFrame:
    output = df.drop(columns=[col for col in PUBLIC_EXPORT_HIDDEN_COLUMNS if col in df.columns], errors="ignore")
    output = output.rename(columns={"意向匹配度": "综合分"})
    if "链接" in output.columns:
        output = output[[col for col in output.columns if col != "链接"] + ["链接"]]
    return output


def user_table_df(df: pd.DataFrame, columns: list[str] | None = None, *, hide_scores: bool = True) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    table = df.copy()
    if columns:
        table = table[[col for col in columns if col in table.columns]]
    hidden = set(USER_HIDDEN_TABLE_COLUMNS)
    if hide_scores:
        hidden.update(col for col in table.columns if str(col).endswith("分") and col not in {"意向匹配度", "综合分", "价值评分", "匹配度"})
        hidden.update(col for col in table.columns if "相似" in str(col) or "命中" in str(col))
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
    max_len = int(series.astype(str).map(lambda value: len(normalize_text(value))).max()) if len(series) else 0
    if max_len >= 64:
        return "large"
    if max_len >= 18:
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
        "综合分",
        "匹配度",
        "投递建议",
        "类型",
        "是否招实习",
        "是否招应届生",
        "应届生",
        "薪资",
        "地点",
        "高价值",
        "高价值岗位",
        "低价值风险",
        "省份",
        "标准城市",
        "学历",
        "经验",
        "队列日期",
        "已投递",
        "面试状态",
        "Offer状态",
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
