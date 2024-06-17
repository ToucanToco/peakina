"""
Module to add excel files support
"""

import logging
from functools import wraps
from typing import Any

import pandas as pd

LOGGER = logging.getLogger(__name__)


@wraps(pd.read_excel)
def read_excel(
    *args: Any,
    preview_nrows: int | None = None,
    preview_offset: int = 0,
    **kwargs: Any,
) -> pd.DataFrame:
    df_or_dict: dict[str, pd.DataFrame] | pd.DataFrame = pd.read_excel(*args, **kwargs)

    if isinstance(df_or_dict, dict):  # multiple sheets
        for sheet_name, sheet_df in df_or_dict.items():
            sheet_df["__sheet__"] = sheet_name
        df = pd.concat(list(df_or_dict.values()))
    else:
        df = df_or_dict

    if preview_nrows:
        return df[preview_offset : preview_offset + preview_nrows or 0]

    return df


def excel_meta(filepath: str, reader_kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Returns a dictionary with the meta information of the Excel file.
    """
    excel_file = pd.ExcelFile(filepath)
    sheet_names = excel_file.sheet_names

    df = read_excel(excel_file, **reader_kwargs)

    if (sheet_name := reader_kwargs.get("sheet_name")) is None:
        # multiple sheets together
        total_rows = sum(excel_file.parse(sheet_name).shape[0] for sheet_name in sheet_names)
    else:
        # single sheet
        total_rows = excel_file.parse(sheet_name).shape[0]

    return {
        "sheetnames": sheet_names,
        "df_rows": df.shape[0],
        "total_rows": total_rows,
    }
