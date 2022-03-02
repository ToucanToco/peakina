"""
Module to add excel files support
"""
import datetime
import logging
from io import StringIO
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import openpyxl
import pandas as pd
import xlrd
from openpyxl.utils.exceptions import InvalidFileException

LOGGER = logging.getLogger(__name__)


def _old_xls_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    preview_nrows: Optional[int],
    preview_offset: Optional[int],
) -> Generator[Any, Any, Any]:
    """
    Depending on paginations inputs (preview_rows, preview_offset), we want to
    get an iterator object to loop on target rows, here we're returning an iterator
    using yield for each iteration in the workbook

    """

    if preview_nrows is None and preview_offset is not None:
        to_iter = range(preview_offset, wb.sheet_by_name(sh_name).nrows)
    elif preview_nrows is not None and preview_offset is not None:
        to_iter = range(preview_offset, preview_offset + preview_nrows + 1)
    elif preview_nrows is not None and preview_offset is None:
        to_iter = range(preview_nrows + 1)
    else:
        to_iter = range(wb.sheet_by_name(sh_name).nrows)

    for rx in to_iter:
        try:
            yield wb.sheet_by_name(sh_name).row(rx)
        except IndexError:
            break


def _new_xls_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    preview_nrows: Optional[int],
    preview_offset: Optional[int],
) -> Generator[Any, Any, Any]:
    """
    Depending on paginations inputs (preview_rows, preview_offset), we want to
    get an iterator object to loop on target rows, here we're returning an iterator
    from the iter_rows built-in function from openpyxl

    """

    # +1 are here because this is 1-based indexing
    if preview_nrows is not None and preview_offset is not None:
        max_row = preview_offset + 1 + preview_nrows
    elif preview_nrows is not None and preview_offset is None:
        max_row = preview_nrows + 1
    else:
        max_row = None

    if preview_offset:
        min_row = preview_offset + 1
    else:
        min_row = None

    # Then we return the generator
    yield wb[sh_name].iter_rows(
        min_row=min_row,
        max_row=max_row,
        values_only=True,
    )


def _get_rows_iterator(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sheet_name: str,
    preview_nrows: Optional[int],
    preview_offset: Optional[int],
) -> Generator[Any, Any, Any]:
    """
    Depending on the excel type either it's the new format or the old one,
    this method will return an iterator to read on its rows
    """

    if isinstance(wb, xlrd.book.Book):
        return _old_xls_rows_iterator(wb, sheet_name, preview_nrows, preview_offset)

    return _new_xls_rows_iterator(wb, sheet_name, preview_nrows, preview_offset)


def _build_row_subset(
    row: Union[List[Any], Tuple[Any]],
    sh_name: str,
    sheetnames: List[str],
    row_number: int,
    row_subset: List[str],
) -> List[str]:
    """
    This method will build each row and add an extra row for the sheet_name
    If we're in an excel with multiple sheets

    """

    def _infer_type(cell_value: Any) -> Any:
        value = str(cell_value)
        if type(cell_value) in [int, float, str]:
            # we're removing "," from cells because we're going to be using comma as seperator for our csv payload
            # and if we keep some cells with comma, it could generate fake mismatch errors on columns...
            value = str(cell_value).replace(",", " ")
        elif type(cell_value) == bool:
            # we're assuming "True" and "False" will be considered as booleans
            value = f'"{cell_value}"'
        elif type(cell_value) in [datetime.datetime]:
            # in teh context of only preview, i think it's okay to
            # just have a representation of the date
            value = cell_value.strftime("%m/%d/%Y %H:%M:%S")

        return value

    cells = [
        _infer_type(cell.value)
        if type(cell) not in [str, int, float, bool, datetime.datetime] and cell is not None
        else _infer_type(cell)
        for cell in row
    ]

    if len(sheetnames) > 1:
        row_subset.append(f'{",".join([*cells, sh_name])}\n')
    else:
        row_subset.append(f'{",".join(cells)}\n')

    return row_subset


def _get_row_subset_per_sheet(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    sheetnames: List[str],
    preview_nrows: Optional[int],
    preview_offset: Optional[int],
    row_subset: List[str],
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
    skipfooter: int = 0,
) -> List[str]:
    """
    This method will get an iterator from the workbook and
    construct a list of row inside row_subset
    """
    # we get the row iterator from here
    row_iterator = _get_rows_iterator(wb, sh_name, preview_nrows, preview_offset)

    def __loop_and_fill_row_subsets(row_subset: List[str], loop_on: Any) -> List[str]:
        headers_skipped = False
        for row_number, row in loop_on:
            # We want to skip the headers if we're in another sheet
            if not headers_skipped:
                headers_skipped = True
                continue
            if skiprows:
                if row_number <= skiprows:
                    continue
            row_subset = _build_row_subset(row, sh_name, sheetnames, row_number, row_subset)
            if nrows:
                if row_number == nrows:
                    break

        return row_subset

    if isinstance(wb, openpyxl.workbook.Workbook):
        for row_iter in row_iterator:
            row_subset = __loop_and_fill_row_subsets(row_subset, enumerate(row_iter))
    else:
        row_subset = __loop_and_fill_row_subsets(row_subset, enumerate(row_iterator))

    # to handle the skipfooter
    lines_to_keep = len(row_subset) - skipfooter

    return row_subset[:lines_to_keep]


def _read_sheets(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sheet_names: List[Any],
    preview_nrows: Optional[int],
    preview_offset: Optional[int],
    nrows: Optional[int] = None,
    skiprows: Optional[int] = None,
    skipfooter: int = 0,
) -> List[Any]:
    """
    This method will loop over sheets, read content and return a list of rows
    depending on your inputs

    """

    row_subset: List[str] = []

    for sh_name in sheet_names:
        row_subset = _get_row_subset_per_sheet(
            wb,
            sh_name,
            sheet_names,
            preview_nrows,
            preview_offset,
            row_subset,
            skiprows,
            nrows,
            skipfooter,
        )

    if isinstance(wb, openpyxl.workbook.Workbook):
        wb.close()

    return row_subset


def read_excel(
    filepath: str,
    *,
    preview_nrows: Optional[int] = None,
    preview_offset: Optional[int] = None,
    sheet_name: str = "",
    na_values: Any = None,
    keep_default_na: bool = False,
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
    skipfooter: int = 0,
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

        # we get column names with the iterator
        for sh_name in all_sheet_names:
            for column_list in [list(c) for c in wb[sh_name].iter_rows(min_row=1, max_row=1)]:
                for co in column_list:
                    if co.value not in column_names:
                        column_names.append(co.value)

    except InvalidFileException as e:
        LOGGER.info(f"Failed to read file {filepath} with openpyxl. Trying xlrd.", exc_info=e)
        wb = xlrd.open_workbook(filepath)
        all_sheet_names = wb.sheet_names()

        for sh_name in all_sheet_names:
            column_names += [
                c.value for c in wb.sheet_by_name(sh_name).row(0) if c.value not in column_names
            ]

    sheet_names = [sheet_name] if sheet_name else all_sheet_names
    if len(all_sheet_names) > 1:
        sheet_names = [all_sheet_names[0]] if sheet_name == "" else sheet_names

    row_subset = _read_sheets(
        wb, sheet_names, preview_nrows, preview_offset, nrows, skiprows, skipfooter
    )

    if sheet_name is None:
        if "__sheet__" not in column_names:  # type: ignore
            column_names.append("__sheet__")

    columns_kwargs = {
        "header": None,
        "names": column_names,
    }

    return pd.read_csv(
        StringIO("\n".join(row_subset)),
        nrows=nrows,
        na_values=na_values,
        keep_default_na=keep_default_na,
        true_values=["True"],
        false_values=["False"],
        **columns_kwargs,
    )


def excel_meta(filepath: str, reader_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a dictionary with the meta information of the excel file.
    """

    total_rows = 0
    try:
        wb = openpyxl.load_workbook(filepath, read_only=True)
        for sheet in wb.worksheets:
            total_rows += sheet.max_row
        sheet_names = wb.sheetnames
    except InvalidFileException as e:
        LOGGER.info(f"Failed to read file {filepath} with openpyxl. Trying xlrd.", exc_info=e)
        wb = xlrd.open_workbook(filepath)
        sheet_names = wb.sheet_names()
        for sheet in sheet_names:
            total_rows += wb.sheet_by_name(sheet).nrows

    # to not count headers of sheets as rows:
    total_rows -= len(sheet_names)

    df = read_excel(filepath, **reader_kwargs)

    return {
        "sheetnames": sheet_names,
        "df_rows": df.shape[0],
        "total_rows": total_rows,
    }
