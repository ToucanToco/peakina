"""
Module to add excel files support
"""
import logging
from typing import Any, Dict, Optional, Union

import pandas as pd

LOGGER = logging.getLogger(__name__)


def read_excel(
    filepath: str,
    *,
    preview_offset: Optional[int] = None,
    preview_nrows: Optional[int] = None,
    sheet_name: Optional[Union[str, int]] = 0,
    na_values: Any = None,
    keep_default_na: bool = False,
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    df = pd.read_excel(
        filepath,
        sheet_name=sheet_name,
        na_values=na_values,
        keep_default_na=keep_default_na,
        skiprows=skiprows,
        nrows=nrows,
    )
    # if there are several sheets, pf.read_excel returns a dict {sheet_name: df}
    if isinstance(df, dict):
        for sheet_name, _df in df.items():
            _df["__sheet__"] = sheet_name
        df = pd.concat(df.values(), sort=False)

    if preview_offset is not None and preview_nrows is not None:
        return df[preview_offset : preview_offset + preview_nrows]
    return df


def excel_meta(filepath: str, reader_kwargs: Dict[str, Any]) -> Dict[str, Any]:  # noqa: F821
    """
    Returns a dictionary with the meta information of the excel file.
    """
    reader_kwargs.pop("preview_offset", None)
    reader_kwargs.pop("preview_nrows", None)

    excel_file = pd.ExcelFile(filepath)
    df = pd.read_excel(excel_file, **reader_kwargs)
    return {
        "sheetnames": excel_file.sheet_names,
        "nrows": df.shape[0],
    }
