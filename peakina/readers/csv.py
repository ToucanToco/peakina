"""
Module to add csv support
"""
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


@wraps(pd.read_csv)
def read_csv(
    filepath_or_buffer: "FilePathOrBuffer",
    *,
    # extra `peakina` reader kwargs
    preview_offset: int = 0,
    preview_nrows: Optional[int] = None,
    # change of default values
    keep_default_na: bool = False,  # pandas default: `True`
    error_bad_lines: bool = False,  # pandas default: `True`
    **kwargs: Any,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks
    """
    if preview_nrows is not None or preview_offset:
        chunks = pd.read_csv(
            filepath_or_buffer,
            keep_default_na=keep_default_na,
            error_bad_lines=error_bad_lines,
            **kwargs,
            # keep the first row 0 (as the header) and then skip everything else up to row `preview_offset`
            skiprows=range(1, preview_offset + 1),
            nrows=preview_nrows,
            chunksize=PREVIEW_CHUNK_SIZE,
        )
        return next(chunks)

    return pd.read_csv(
        filepath_or_buffer,
        keep_default_na=keep_default_na,
        error_bad_lines=error_bad_lines,
        **kwargs,
    )


def _line_count(filepath_or_buffer: "FilePathOrBuffer") -> int:
    with open(filepath_or_buffer) as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.read  # loop optimization

        buf = read_f(buf_size)
        while buf:
            lines += buf.count("\n")
            buf = read_f(buf_size)

        return lines


def csv_meta(
    filepath_or_buffer: "FilePathOrBuffer", reader_kwrgs: Dict[str, Any]
) -> Dict[str, Any]:
    total_rows = _line_count(filepath_or_buffer)

    preview_offset = reader_kwrgs.pop("preview_offset", 0)
    preview_nrows = reader_kwrgs.pop("preview_nrows", None)
    if preview_nrows is not None:
        if preview_nrows <= preview_offset:
            df_rows = preview_nrows
        else:
            df_rows = min(total_rows, preview_nrows - preview_offset)
    else:
        df_rows = total_rows - preview_offset

    return {"total_rows": _line_count(filepath_or_buffer), "df_rows": df_rows}
