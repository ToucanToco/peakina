"""
Module to add xml support
"""
from io import StringIO
from itertools import islice
from typing import Any, Dict

import pandas as pd
import xlrd
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException


def read_excel(filepath: str, **kwargs: Any) -> pd.DataFrame:
    """
    The read_excel function is using openpyxl to parse the csv file and read it

    """

    preview: bool = kwargs.get("preview", False)
    preview_args: Dict[str, Any] = kwargs.get("preview_args", {})

    sheet_name: str = kwargs.get("sheet_name", None)
    na_values: str = kwargs.get("na_values", None)
    keep_default_na: str = kwargs.get("keep_default_na", None)
    skiprows: int = kwargs.get("skiprows", 0)
    nrows: int = kwargs.get("nrows", 2)

    try:
        wb = load_workbook(filepath, read_only=True)
        s = wb[sheet_name if sheet_name else wb.sheetnames[0]]
        row_to_iterate = s.rows
    except InvalidFileException:
        wb = xlrd.open_workbook(filepath)
        s = wb[sheet_name if sheet_name else wb.sheet_names()[0]]
        row_to_iterate = [s.row(rx) for rx in range(s.nrows)]

    row_subset = []

    if preview:
        nrows, offset = preview_args.get("nrows", 2), preview_args.get("offset", 0)
        row_to_iterate = list(islice(row_to_iterate, offset, offset + nrows))

    for i, row in enumerate(row_to_iterate):
        if i < skiprows:
            continue

        row_sub = ",".join([str(cell.value) if cell.value else "" for cell in row])
        row_subset.append(f"{row_sub}\n")

        if i == nrows:
            break

    try:
        wb.close()
    except AttributeError:
        pass

    return pd.read_csv(
        StringIO("\n".join(row_subset)), na_values=na_values, keep_default_na=keep_default_na
    )
