"""
Module to add xml support
"""
from io import StringIO
from typing import Any, Dict

import pandas as pd
import xlrd
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException


def _yielder(preview: bool, s: Any, nrows: int = 2, offset: int = 0) -> Any:
    """
    A generator for old excel types
    """
    if preview:
        to_iter = (offset, offset + nrows)
    else:
        to_iter = (0, s.nrows)

    for rx in range(to_iter[0], to_iter[1]):
        try:
            yield s.row(rx)
        except Exception:
            break


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

    nrows, offset = preview_args.get("nrows", 2), preview_args.get("offset", 0)

    try:
        wb = load_workbook(filepath, read_only=True)
        s = wb[sheet_name if sheet_name else wb.sheetnames[0]]
        if preview:
            row_to_iterate = s.iter_rows(min_row=offset, max_row=offset + nrows, values_only=True)
        else:
            row_to_iterate = s.iter_rows(values_only=True)
    except InvalidFileException:
        wb = xlrd.open_workbook(filepath)
        s = wb[sheet_name if sheet_name else wb.sheet_names()[0]]
        row_to_iterate = list(_yielder(preview, s, nrows, offset))

    row_subset = []
    row_number = 0
    for row in row_to_iterate:
        if row_number < skiprows:
            continue

        cells = []
        for cell in row:
            val = cell.value if type(cell) not in [str, int] else cell
            cells.append(str(val) if val else "")
        row_subset.append(f'{",".join(cells)}\n')

        if row_number == nrows:
            break
        row_number += 1

    try:
        wb.close()
    except AttributeError:
        pass

    return pd.read_csv(
        StringIO("\n".join(row_subset)), na_values=na_values, keep_default_na=keep_default_na
    )
