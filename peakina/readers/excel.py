"""
Module to add excel files support
"""
from io import StringIO
from typing import Any, Generator, List, Optional, Tuple, Union

import openpyxl
import pandas as pd
import xlrd
from openpyxl.utils.exceptions import InvalidFileException


from peakina.readers.common import PreviewArgs


def _old_xls_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
) -> Generator[Any, Any, Any]:

    if preview_nrows is not None and preview_offset is not None:
        to_iter = range(preview_offset, preview_offset + preview_nrows)
    else:
        to_iter = range(wb.sheet_by_name(sh_name).nrows)

    for rx in to_iter:
        yield wb.sheet_by_name(sh_name).row(rx)


def _new_xls_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
) -> Generator[Any, Any, Any]:

    yield wb[sh_name].iter_rows(
        min_row=preview_offset if preview_offset else None,
        max_row=preview_offset + preview_nrows if preview_offset is not None and preview_nrows is not None else None,
        values_only=True,
    )


def _get_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sheet_name: str,
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
) -> Generator[Any, Any, Any]:

    if isinstance(wb, xlrd.book.Book):
        return _old_xls_rows_iterator(wb, sheet_name, preview_offset, preview_nrows)

    return _new_xls_rows_iterator(wb, sheet_name, preview_offset, preview_nrows)


def quote_if_needed(value: str) -> str:
    """
    Quote the value if needed
    """
    if value.find(",") != -1:
        return f'"{value}"'
    return value

def _build_row_subset(
    row: Union[List[Any], Tuple[Any]],
    sh_name: str,
    sheetnames: List[str],
    row_number: int,
    row_subset: List[str],
) -> Tuple[int, List[str]]:

    cells = [quote_if_needed(str(cell.value)) if type(cell) not in [str, int, float] else quote_if_needed(str(cell)) for cell in row]

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
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    sheetnames: List[str],
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
    row_subset: List[str],
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
) -> List[str]:

    row_iterator = _get_rows_iterator(wb, sh_name, preview_offset, preview_nrows)

    if isinstance(wb, openpyxl.workbook.Workbook):
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
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sheet_names: List[Any],
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
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
            wb, sh_name, sheet_names, preview_offset, preview_nrows, row_subset, skiprows, nrows
        )

    if isinstance(wb, openpyxl.workbook.Workbook):
        wb.close()

    # cleaning extras rows with __sheet__
    if len(sheet_names) > 1:
        row_subset[1:] = [x for x in row_subset[1:] if "__sheet__" not in x]

    return row_subset


def read_excel(
    filepath: str,
    *,
    preview_offset: Optional[int] = None,
    preview_nrows: Optional[int] = None,
    sheet_name: str = "",
    na_values: Any = None,
    keep_default_na: bool = False,
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Uses openpyxl (with xlrd as fallback) to convert the excel sheet into a csv string.
    This csv is then read by pandas to make a DataFrame.

    Using this two steps, we are able to obtain better performance than pd.read_excel alone.
    Also, these two libraries are able to read only the top of each sheet,
    so we can create previews without reading the whole file.

    """

    column_names = []

    try:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        all_sheet_names = wb.sheetnames

        if preview_offset is not None and preview_nrows is not None:
            # we get column names with the iterator
            for sh_name in all_sheet_names:
                column_names += [c.value for c in next(wb[sh_name].iter_rows(min_row=1, max_row=1))]

    except InvalidFileException:
        wb = xlrd.open_workbook(filepath) # I used another variable to avoid bugs in pycharm autocomplete.
        all_sheet_names = wb.sheet_names()

        if preview_offset is not None and preview_nrows is not None:
            for sh_name in all_sheet_names:
                column_names += [c.value for c in wb.sheet_by_name(sh_name).row(0)]

    sheet_names = [sheet_name] if sheet_name else all_sheet_names

    row_subset = _read_sheets(wb, sheet_names, preview_offset, preview_nrows, nrows, skiprows)
    kwargs = {}
    if preview_offset is not None and preview_nrows is not None:
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


def excel_meta(filepath: str, datasource: 'DataSource') -> dict:
    """
    Returns a dictionary with the meta information of the excel file.
    """
    try:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        sheet_names = wb.sheetnames
        if 'sheet_name' in datasource.reader_kwargs and datasource.reader_kwargs['sheet_name']:
            nrows = wb[datasource.reader_kwargs['sheet_name']].max_row
        else:
            nrows = wb[sheet_names[0]].max_row
    except InvalidFileException:
        wb = xlrd.open_workbook(filepath)
        sheet_names = wb.sheet_names()
        if 'sheet_name' in datasource.reader_kwargs and datasource.reader_kwargs['sheet_name']:
            nrows = wb.sheet_by_name(datasource.reader_kwargs['sheet_name']).nrows
        else:
            nrows = wb.sheet_by_index(0).nrows

    return {
        "sheetnames": sheet_names,
        "nrows": nrows,
    }