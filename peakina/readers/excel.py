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


def _yielder(preview: bool, sheet_name: Any, nrows: int = 2, offset: int = 0) -> Any:
    """
    A generator for old excel types
    """
    if preview:
        to_iter = range(offset, offset + nrows)
    else:
        to_iter = range(sheet_name.nrows)

    for rx in to_iter:
        try:
            yield sheet_name.row(rx)
        except Exception:
            break


def _read_old_xls_format(
    wb: Any, sheetnames: List[Any], preview: bool = False, nrows: int = 2, offset: int = 0
) -> Any:
    """ """
    sh_iter: Any = iter(())

    for sh_name in sheetnames:
        if preview:
            sh_iter = chain(sh_iter, _yielder(preview, wb[1][sh_name], nrows, offset))

        sh_iter = chain(sh_iter, _yielder(preview, wb[1][sh_name]))

    return sh_iter


def _read_new_xls_format(
    wb: Any, sheetnames: List[Any], preview: bool = False, nrows: int = 2, offset: int = 0
) -> Any:
    """ """
    sh_iter: Any = iter(())

    for sh_name in sheetnames:
        if preview:
            sh_iter = chain(
                sh_iter,
                wb[1][sh_name].iter_rows(min_row=offset, max_row=offset + nrows, values_only=True),
            )

        sh_iter = chain(sh_iter, wb[1][sh_name].iter_rows(values_only=True))

    return sh_iter


def _get_row_to_iterate(
    wb: Any, sheetnames: List[Any], preview: bool, nrows: int, offset: int, skiprows: int
) -> Any:
    """ """
    if wb[0] == "new":
        return _read_new_xls_format(wb, sheetnames, preview, nrows, offset)

    return _read_old_xls_format(wb, sheetnames, preview, nrows, offset)


def _read_sheets(
    wb: Any, sheetnames: List[Any], preview: bool, nrows: int, offset: int, skiprows: int
) -> List[Any]:
    """ """
    row_to_iterate = _get_row_to_iterate(wb, sheetnames, preview, nrows, offset, skiprows)

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

    if wb[0] == "new":
        wb[1].close()

    return row_subset


def read_excel(
    filepath: str,
    *,
    preview: bool = False,
    preview_args: Dict[str, Any] = {},
    sheet_name: str = "",
    na_values: str = "",
    keep_default_na: str = "",
    skiprows: int = 0,
) -> pd.DataFrame:
    """
    The read_excel function is using openpyxl to parse the csv file and read it

    """
    nrows, offset = preview_args.get("nrows", 2), preview_args.get("offset", 0)

    try:
        wb = ("new", load_workbook(filepath, read_only=True))
    except InvalidFileException:
        wb = ("old", xlrd.open_workbook(filepath))

    sheetnames = (
        [sheet_name]
        if sheet_name and len(sheet_name)
        else (wb[1].sheetnames if wb[0] == "new" else wb[1].sheet_names())
    )

    row_subset = _read_sheets(wb, sheetnames, preview, nrows, offset, skiprows)

    return pd.read_csv(
        StringIO("\n".join(row_subset)), na_values=na_values, keep_default_na=keep_default_na
    )
