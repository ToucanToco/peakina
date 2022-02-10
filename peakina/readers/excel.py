"""
Module to add xml support
"""
from io import StringIO
from itertools import chain
from typing import Any, Dict, List

import pandas as pd
import xlrd
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException


def _yielder(preview: Dict[str, int], sheet_name: Any, limit: int = 2, offset: int = 0) -> Any:
    """
    A generator for old excel types
    """

    if bool(preview):
        to_iter = range(offset, offset + limit)
    else:
        to_iter = range(sheet_name.nrows)

    for rx in to_iter:
        yield sheet_name.row(rx)


def _read_old_xls_format(wb: Any, sh_name: str, preview: Dict[str, int]) -> Any:
    """ """

    if bool(preview):
        limit, offset = preview.get("nrows", 1), preview.get("offset", 0)
        return list(_yielder(preview, wb[1][sh_name], limit, offset))

    return list(_yielder(preview, wb[1][sh_name]))


def _read_new_xls_format(wb: Any, sh_name: str, preview: Dict[str, int]) -> Any:
    """ """
    sh_iter: Any = iter(())

    if bool(preview):
        limit, offset = preview.get("nrows", 1), preview.get("offset", 0)
        sh_iter = chain(
            sh_iter,
            wb[1][sh_name].iter_rows(min_row=offset, max_row=offset + limit, values_only=True),
        )

    sh_iter = chain(sh_iter, wb[1][sh_name].iter_rows(values_only=True))

    return sh_iter


def _get_row_to_iterate(wb: Any, sheet_name: str, preview: Dict[str, int]) -> Any:
    """ """
    if wb[0] == "new":
        return _read_new_xls_format(wb, sheet_name, preview)

    return _read_old_xls_format(wb, sheet_name, preview)


def _read_sheets(
    wb: Any,
    sheetnames: List[Any],
    preview: Dict[str, int],
    nrows: int,
    skiprows: int,
) -> List[Any]:
    """ """
    row_subset = []
    for sh_name in sheetnames:
        row_number = 0
        row_to_iterate = _get_row_to_iterate(wb, sh_name, preview)
        for row in row_to_iterate:
            if row_number < skiprows:
                continue

            cells = [
                str(cell.value) if type(cell) not in [str, int, float] else str(cell)
                for cell in row
            ]

            if len(sheetnames) > 1:
                if row_number == 0:
                    row_subset.append(f'{",".join([*cells, "__sheet__"])}\n')
                else:
                    row_subset.append(f'{",".join([*cells, sh_name])}\n')
            else:
                row_subset.append(f'{",".join(cells)}\n')

            row_number += 1

            if row_number == nrows:
                break

    if wb[0] == "new":
        wb[1].close()

    # cleaning superflues rows with __sheet__
    if len(sheetnames) > 1:
        row_subset[1:] = [x for x in row_subset[1:] if "__sheet__" not in x]

    return row_subset


def read_excel(
    filepath: str,
    *,
    preview: Dict[str, int] = {},
    sheet_name: str = "",
    na_values: str = "",
    keep_default_na: Any = None,
    skiprows: int = 0,
    nrows: int = 50,
) -> pd.DataFrame:
    """
    The read_excel function is using openpyxl to parse the csv file and read it

    """

    try:
        wb = ("new", load_workbook(filepath, read_only=True))
    except InvalidFileException:
        wb = ("old", xlrd.open_workbook(filepath))

    sheetnames = (
        [sheet_name]
        if sheet_name and len(sheet_name)
        else (wb[1].sheetnames if wb[0] == "new" else wb[1].sheet_names())
    )

    row_subset = _read_sheets(wb, sheetnames, preview, nrows, skiprows)

    return pd.read_csv(
        StringIO("\n".join(row_subset)),
        nrows=nrows,
        na_values=na_values,
        keep_default_na=keep_default_na,
    )
