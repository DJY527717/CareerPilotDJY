from __future__ import annotations

from typing import Any

import pandas as pd


def sort_batch_rank_rows(df: pd.DataFrame, high_value_col: str = "高价值") -> pd.DataFrame:
    if df.empty:
        return df
    if "final_rank_score" not in df.columns:
        return df.reset_index(drop=True)
    sort_score_col = "final_rank_score"
    sort_cols: list[str] = [sort_score_col]
    ascending: list[bool] = [False]
    if high_value_col in df.columns:
        sort_cols.append(high_value_col)
        ascending.append(False)
    return df.sort_values(sort_cols, ascending=ascending).reset_index(drop=True)


def row_rank_score(row: dict[str, Any]) -> int:
    return int(row.get("final_rank_score", 0) or 0)
