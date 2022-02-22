"""
Module to add excel files support
"""
import logging
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]

LOGGER = logging.getLogger(__name__)


@wraps(pd.read_excel)
def read_excel(
    filepath_or_buffer: "FilePathOrBuffer",
    *,
    # extra `peakina` reader kwargs
    preview_offset: int = 0,
    preview_nrows: Optional[int] = None,
    # change of default values
    keep_default_na: bool = False,  # pandas default: `True`
    **kwargs: Any,
) -> pd.DataFrame:
    df = pd.read_excel(
        filepath_or_buffer,
        keep_default_na=keep_default_na,
        **kwargs,
    )
    # if there are several sheets, pf.read_excel returns a dict {sheet_name: df}
    if isinstance(df, dict):
        for sheet_name, sheet_df in df.items():
            sheet_df["__sheet__"] = sheet_name
        df = pd.concat(df.values(), sort=False)

    if preview_nrows is not None or preview_offset:
        offset = None if preview_nrows is None else preview_offset + preview_nrows
        return df[preview_offset:offset]
    return df


def excel_meta(
    filepath_or_buffer: "FilePathOrBuffer", reader_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Returns a dictionary with the meta information of the excel file.
    """
    excel_file = pd.ExcelFile(filepath_or_buffer)
    sheet_names = excel_file.sheet_names

    df = read_excel(excel_file, **reader_kwargs)

    if (sheet_name := reader_kwargs.get("sheet_name", 0)) is None:
        # multiple sheets together
        return {
            "sheetnames": sheet_names,
            "df_rows": df.shape[0],
            "total_rows": sum(excel_file.parse(sheet_name).shape[0] for sheet_name in sheet_names),
        }
    else:
        # single sheet
        return {
            "sheetnames": sheet_names,
            "df_rows": df.shape[0],
            "total_rows": excel_file.parse(sheet_name).shape[0],
        }
