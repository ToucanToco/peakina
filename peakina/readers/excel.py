"""
Module to add excel files support
"""
import enum
from io import StringIO
from typing import Any, Generator, List, Optional, Tuple, Union

import pandas as pd
import xlrd
from openpyxl import load_workbook
from openpyxl.utils.exceptions import InvalidFileException

from peakina.readers.common import PreviewArgs


class EXCEL_TYPE(enum.Enum):
    NEW = "new"
    OLD = "old"


def _old_xls_rows_iterator(
    wb: Any, sh_name: str, preview: Optional[PreviewArgs]
) -> Generator[Any, Any, Any]:

    if preview:
        to_iter = range(preview.offset, preview.offset + preview.nrows)
    else:
        to_iter = range(wb[sh_name].nrows)

    for rx in to_iter:
        yield wb[sh_name].row(rx)


def _new_xls_rows_iterator(
    wb: Any, sh_name: str, preview: Optional[PreviewArgs]
) -> Generator[Any, Any, Any]:

    yield wb[sh_name].iter_rows(
        min_row=preview.offset if preview else None,
        max_row=preview.offset + preview.nrows if preview else None,
        values_only=True,
    )


def _get_rows_iterator(
    wb: Any, excel_type: EXCEL_TYPE, sheet_name: str, preview: Optional[PreviewArgs]
) -> Generator[Any, Any, Any]:

    if excel_type is EXCEL_TYPE.OLD:
        return _old_xls_rows_iterator(wb, sheet_name, preview)

    return _new_xls_rows_iterator(wb, sheet_name, preview)


def _build_row_subset(
    row: Union[List[Any], Tuple[Any]],
    sh_name: str,
    sheetnames: List[str],
    row_number: int,
    row_subset: List[str],
) -> Tuple[int, List[str]]:

    cells = [str(cell.value) if type(cell) not in [str, int, float] else str(cell) for cell in row]

    if len(sheetnames) > 1:
        # TO add the column names at the top
        if row_number == 0:
            row_subset.append(f'{",".join([*cells, "__sheet__"])}\n')
        else:
            row_subset.append(f'{",".join([*cells, sh_name])}\n')
    else:
        row_subset.append(f'{",".join(cells)}\n')

    row_number += 1

    return row_number, row_subset


def _get_row_subset_per_sheet(
    wb: Any,
    sh_name: str,
    sheetnames: List[str],
    preview: Optional[PreviewArgs],
    excel_type: EXCEL_TYPE,
    row_subset: List[str],
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
) -> List[str]:

    row_iterator = _get_rows_iterator(wb, excel_type, sh_name, preview)

    if excel_type is EXCEL_TYPE.NEW:
        for row_number, gen in enumerate(row_iterator):
            for row in gen:
                if skiprows:
                    if row_number < skiprows:
                        continue
                row_number, row_subset = _build_row_subset(
                    row, sh_name, sheetnames, row_number, row_subset
                )
                if nrows:
                    if row_number == nrows:
                        break
    else:
        for row_number, row in enumerate(row_iterator):
            if skiprows:
                if row_number < skiprows:
                    continue
            row_number, row_subset = _build_row_subset(
                row, sh_name, sheetnames, row_number, row_subset
            )
            if nrows:
                if row_number == nrows:
                    break
    return row_subset


def _read_sheets(
    wb: Any,
    excel_type: EXCEL_TYPE,
    sheet_names: List[Any],
    preview: Optional[PreviewArgs],
    nrows: Optional[int] = None,
    skiprows: Optional[int] = None,
) -> List[Any]:
    """
    This method will loop over sheets, read content and return a list of rows
    depending on your inputs

    """

    row_subset: List[str] = []
    for sh_name in sheet_names:
        row_subset = _get_row_subset_per_sheet(
            wb, sh_name, sheet_names, preview, excel_type, row_subset, skiprows, nrows
        )

    if excel_type is EXCEL_TYPE.NEW:
        wb.close()

    # cleaning extras rows with __sheet__
    if len(sheet_names) > 1:
        row_subset[1:] = [x for x in row_subset[1:] if "__sheet__" not in x]

    return row_subset


def read_excel(
    filepath: str,
    *,
    preview: Optional[PreviewArgs] = None,
    sheet_name: str = "",
    na_values: Any = None,
    keep_default_na: bool = False,
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Uses openpyxl (with xlrd as fallback) to convert the excel sheet into a csv string.
    This csv is then read by pandas to make a DataFrame.
    
    Using this two steps, we are able to obtain better performance than pd.read_excel alone. Also, these two libraries are able to read only the top of each sheet, so we can create previews without reading the whole file.

    """

    column_names = []

    try:
        excel_type, wb = EXCEL_TYPE.NEW, load_workbook(filepath, read_only=True)
        all_sheet_names = wb.sheetnames

        if preview:
            # we get column names with the iterator
            for sh_name in all_sheet_names:
                column_names += [c.value for c in next(wb[sh_name].iter_rows(min_row=1, max_row=1))]

    except InvalidFileException:
        excel_type, wb = EXCEL_TYPE.OLD, xlrd.open_workbook(filepath)
        all_sheet_names = wb.sheet_names()

        if preview:
            for sh_name in all_sheet_names:
                column_names += [c.value for c in wb[sh_name].row(0)]

    sheet_names = [sheet_name] if sheet_name else all_sheet_names

    row_subset = _read_sheets(wb, excel_type, sheet_names, preview, nrows, skiprows)

    kwargs = {}
    if preview:
        kwargs = {
            "header": None,
            "names": column_names,
        }

    return pd.read_csv(
        StringIO("\n".join(row_subset)),
        nrows=nrows,
        na_values=na_values,
        keep_default_na=keep_default_na,
        **kwargs,
    )
