"""
Module to add excel files support
"""
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
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
) -> Generator[Any, Any, Any]:

    if preview_offset and preview_offset == 0:
        preview_offset = 1  # skip the header

    if preview_offset and preview_nrows is None:
        to_iter = range(preview_offset, wb.sheet_by_name(sh_name).nrows)
    elif preview_nrows is not None and preview_offset is not None:
        to_iter = range(preview_offset + 1, preview_offset + preview_nrows + 1)
    elif preview_nrows is not None and preview_offset is None:
        to_iter = range(preview_nrows + 1)
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

    if preview_offset == 0:
        preview_offset = 1  # skip the header

    # +1 are here because this is 1-based indexing
    yield wb[sh_name].iter_rows(
        min_row=preview_offset + 1 if preview_offset else None,
        max_row=preview_offset + 1 + preview_nrows
        if preview_offset is not None and preview_nrows is not None
        else None,
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
    line_of_merged_sheets: int = 0,
) -> List[str]:

    cells = [
        quote_if_needed(str(cell.value))
        if type(cell) not in [str, int, float] and cell is not None
        else quote_if_needed(str(cell))
        for cell in row
    ]

    if len(sheetnames) > 1:
        # TO add the column names at the top
        if row_number == 0 and line_of_merged_sheets == 0:
            row_subset.append(f'{",".join([*cells, "__sheet__"])}\n')
        elif row_number > 0:
            row_subset.append(f'{",".join([*cells, sh_name])}\n')
    else:
        row_subset.append(f'{",".join(cells)}\n')

    return row_subset


def _get_row_subset_per_sheet(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sh_name: str,
    sheetnames: List[str],
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
    row_subset: List[str],
    skiprows: Optional[int] = None,
    nrows: Optional[int] = None,
    skipfooter: Optional[int] = None,
    line_of_merged_sheets: int = 0,
) -> List[str]:

    row_iterator = _get_rows_iterator(wb, sh_name, preview_offset, preview_nrows)

    row_iter_enumerated = list(enumerate(row_iterator))

    bottom_lines_to_skip = len(row_iter_enumerated) - (
        skipfooter if isinstance(skipfooter, int) else 0
    )

    def __loop_and_fill_row_subsets(row_subset: List[str], loop_on: Any) -> List[str]:
        for row_number, row in loop_on:
            if skiprows:
                if row_number < skiprows:
                    continue
            row_subset = _build_row_subset(
                row, sh_name, sheetnames, row_number, row_subset, line_of_merged_sheets
            )
            if nrows:
                if row_number == nrows:
                    break
        return row_subset

    if isinstance(wb, openpyxl.workbook.Workbook):
        row_subset = __loop_and_fill_row_subsets(
            row_subset, enumerate(row_iter_enumerated[:bottom_lines_to_skip][0][1])
        )
    else:
        row_subset = __loop_and_fill_row_subsets(
            row_subset, row_iter_enumerated[:bottom_lines_to_skip]
        )

    return row_subset


def _read_sheets(
    wb: Union[openpyxl.workbook.Workbook, xlrd.book.Book],
    sheet_names: List[Any],
    preview_offset: Optional[int],
    preview_nrows: Optional[int],
    nrows: Optional[int] = None,
    skiprows: Optional[int] = None,
    skipfooter: Optional[int] = None,
) -> List[Any]:
    """
    This method will loop over sheets, read content and return a list of rows
    depending on your inputs

    """

    # since we merge sheets in case of multiple sheets, we don't want column_names
    # inside the list  of row_subset
    # cleaning extras rows with __sheet__
    line_of_merged_sheets = 0
    row_subset: List[str] = []

    for sh_name in sheet_names:
        row_subset = _get_row_subset_per_sheet(
            wb,
            sh_name,
            sheet_names,
            preview_offset,
            preview_nrows,
            row_subset,
            skiprows,
            nrows,
            skipfooter,
            line_of_merged_sheets,
        )
        line_of_merged_sheets += 1

    if isinstance(wb, openpyxl.workbook.Workbook):
        wb.close()

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
    skipfooter: Optional[int] = None,
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
            if sheet_name is None or sheet_name == "":
                column_names = [
                    c.value for c in next(wb[all_sheet_names[0]].iter_rows(min_row=1, max_row=1))
                ]
            else:
                # we get column names with the iterator
                for sh_name in all_sheet_names:
                    column_names += [
                        c.value for c in next(wb[sh_name].iter_rows(min_row=1, max_row=1))
                    ]

    except InvalidFileException as e:
        LOGGER.info(f"Failed to read file {filepath} with openpyxl. Trying xlrd.", exc_info=e)
        wb = xlrd.open_workbook(
            filepath
        )  # I used another variable to avoid bugs in pycharm autocomplete.
        all_sheet_names = wb.sheet_names()

        if preview_offset is not None and preview_nrows is not None:
            if sheet_name is None or sheet_name == "":
                column_names = [
                    c.value
                    for c in wb.sheet_by_name(all_sheet_names[0]).row(0)
                    if c.value not in column_names
                ]
            else:
                for sh_name in all_sheet_names:
                    column_names += [
                        c.value
                        for c in wb.sheet_by_name(sh_name).row(0)
                        if c.value not in column_names
                    ]

    sheet_names = [sheet_name] if sheet_name else all_sheet_names
    sheet_names = [all_sheet_names[0]] if sheet_name == "" else sheet_names

    row_subset = _read_sheets(
        wb, sheet_names, preview_offset, preview_nrows, nrows, skiprows, skipfooter
    )
    kwargs = {}
    if preview_offset is not None and preview_nrows is not None:
        kwargs = {
            "header": None,
            "names": column_names,
        }

    return pd.read_csv(
        StringIO("\n".join(row_subset)),
        na_values=na_values,
        keep_default_na=keep_default_na,
        **kwargs,
    )


def excel_meta(filepath: str, reader_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a dictionary with the meta information of the excel file.
    """

    excel_file = pd.ExcelFile(filepath)
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
