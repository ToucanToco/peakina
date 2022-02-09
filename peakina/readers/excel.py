from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
from openpyxl import load_workbook


def read_excel(
    filepath: str, sep: str = ",", encoding: Optional[str] = None, **kwargs: Any
) -> pd.DataFrame:
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

    wb = load_workbook(filepath, read_only=True)
    s = wb[sheet_name if sheet_name else wb.sheetnames[0]]
    row_subset = []

    row_to_iterate = s.rows

    if preview:
        nrows, offset = preview_args.get("nrows", 2), preview_args.get("offset", 0)
        row_to_iterate = row_to_iterate[offset : offset + nrows]

    for i, row in enumerate(row_to_iterate):
        if i < skiprows:
            continue
        cells = []
        for cell in row:
            val = cell.value
            cells.append(str(val) if val else "")
        row_subset.append(f'{",".join(cells)}\n')
        if i == nrows:
            break
    wb.close()
    return pd.read_csv(
        StringIO("\n".join(row_subset)), na_values=na_values, keep_default_na=keep_default_na
    )
