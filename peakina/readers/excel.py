"""
Module to add excel files support
"""

import logging
from typing import Any

import fastexcel as fe
import pandas as pd

LOGGER = logging.getLogger(__name__)


def _translate_pd_single_dtype(dtype: str) -> fe.DType:
    dtype = dtype.lower()
    if dtype in ("int", "integer"):
        return "int"
    elif dtype in ("float", "double"):
        return "float"
    elif dtype in ("str", "string"):
        return "string"
    elif dtype in ("bool", "boolean"):
        return "boolean"
    elif dtype in ("date", "datetime") or dtype.startswith("datetime"):
        return "datetime"
    elif dtype in ("time", "timedelta"):
        return "duration"
    else:
        raise ValueError(f"Unsupported dtype: {dtype}")


def _translate_pd_dtype_kwarg(dtype: str | dict[str, str]) -> fe.DType | fe.DTypeMap:
    if isinstance(dtype, str):
        return _translate_pd_single_dtype(dtype)
    else:
        return {k: _translate_pd_single_dtype(v) for k, v in dtype.items()}


def read_excel(
    path_or_data: Any,
    preview_nrows: int | None = None,
    preview_offset: int = 0,
    **kwargs: Any,
) -> pd.DataFrame:
    # Adapting pandas kwargs to fastexcel kwargs
    # By default, pandas.read_excel will only read the first sheet.
    sheet_id: str | int = (
        kwargs.get("sheet") or kwargs.get("sheet_name") or kwargs.get("sheetname") or 0
    )
    skip_rows = kwargs.get("skip_rows") or kwargs.get("skiprows")
    if "nrows" in kwargs:
        n_rows = kwargs["nrows"]
    else:
        n_rows = preview_nrows + preview_offset if preview_nrows else None
    if "dtypes" in kwargs:
        dtypes: fe.DType | fe.DTypeMap | None = _translate_pd_dtype_kwarg(kwargs["dtypes"])
    if "dtype" in kwargs:
        dtypes = _translate_pd_dtype_kwarg(kwargs["dtype"])
    else:
        dtypes = None

    excel_file = fe.read_excel(path_or_data)

    sheet = excel_file.load_sheet(sheet_id, skip_rows=skip_rows, n_rows=n_rows, dtypes=dtypes)
    df = sheet.to_pandas()
    if preview_offset:
        df = df.iloc[preview_offset:]
    if preview_nrows:
        df = df.iloc[:preview_nrows]
    if (skip_footer := kwargs.get("skipfooter")) is not None:
        df = df.iloc[:-skip_footer]

    return df


def excel_meta(filepath: str, reader_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Returns a dictionary with the meta information of the Excel file."""
    excel_file = fe.read_excel(filepath)
    return {"sheetnames": excel_file.sheet_names}
