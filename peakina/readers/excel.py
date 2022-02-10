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


def _yielder(preview: bool, sheet_name: Any, limit: int = 2, offset: int = 0) -> Any:
    """
    A generator for old excel types
    """
    if preview:
        to_iter = range(offset, offset + limit)
    else:
        to_iter = range(sheet_name.nrows)

    for rx in to_iter:
        try:
            yield sheet_name.row(rx)
        except Exception:
            break


def _read_old_xls_format(wb: Any, sh_name: str, preview: bool, limit: int, offset: int) -> Any:
    """ """
    sh_iter: Any = iter(())

    if preview:
        sh_iter = chain(sh_iter, _yielder(preview, wb[1][sh_name], limit, offset))

    sh_iter = chain(sh_iter, _yielder(preview, wb[1][sh_name]))

    return sh_iter


def _read_new_xls_format(wb: Any, sh_name: str, preview: bool, limit: int, offset: int) -> Any:
    """ """
    sh_iter: Any = iter(())

    if preview:
        sh_iter = chain(
            sh_iter,
            wb[1][sh_name].iter_rows(min_row=offset, max_row=offset + limit, values_only=True),
        )

    sh_iter = chain(sh_iter, wb[1][sh_name].iter_rows(values_only=True))

    return sh_iter


def _get_row_to_iterate(wb: Any, sheet_name: str, preview: bool, limit: int, offset: int) -> Any:
    """ """
    if wb[0] == "new":
        return _read_new_xls_format(wb, sheet_name, preview, limit, offset)

    return _read_old_xls_format(wb, sheet_name, preview, limit, offset)


def _read_sheets(
    wb: Any,
    sheetnames: List[Any],
    preview: bool,
    nrows: int,
    limit: int,
    offset: int,
    skiprows: int,
) -> List[Any]:
    """ """
    row_subset = []
    columns_heads_appended = False
    for sh_name in sheetnames:
        row_to_iterate = _get_row_to_iterate(wb, sh_name, preview, limit, offset)
        cells = []
        row_number = 0
        for row in row_to_iterate:
            if row_number < skiprows:
                continue

            for cell in row:
                val = cell.value if type(cell) not in [str, int] else cell
                cells.append(str(val) if val else "")

            if row_number == 0 and not columns_heads_appended:
                row_subset.append(f'{",".join([*cells, "__sheet__"])}\n')
                columns_heads_appended = True

            row_number += 1

            if row_number == nrows:
                break

        row_subset.append(f'{",".join([*cells, sh_name])}\n')

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
    keep_default_na: Any = None,
    skiprows: int = 0,
    nrows: int = 50,
) -> pd.DataFrame:
    """
    The read_excel function is using openpyxl to parse the csv file and read it

    """
    limit, offset = preview_args.get("nrows", 1), preview_args.get("offset", 0)

    try:
        wb = ("new", load_workbook(filepath, read_only=True))
    except InvalidFileException:
        wb = ("old", xlrd.open_workbook(filepath))

    sheetnames = (
        [sheet_name]
        if sheet_name and len(sheet_name)
        else (wb[1].sheetnames if wb[0] == "new" else wb[1].sheet_names())
    )
    row_subset = _read_sheets(wb, sheetnames, preview, nrows, limit, offset, skiprows)

    df = pd.read_csv(
        StringIO("\n".join(row_subset)), na_values=na_values, keep_default_na=keep_default_na
    )

    if len(sheetnames) == 1:
        del df["__sheet__"]

    return df
