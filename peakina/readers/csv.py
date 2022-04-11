"""
Module to add csv support
"""
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]


@wraps(pd.read_csv)
def read_csv(
    filepath_or_buffer: "FilePathOrBuffer",
    *,
    # extra `peakina` reader kwargs
    preview_offset: int = 0,
    preview_nrows: Optional[int] = None,
    # change of default values
    error_bad_lines: bool = False,  # pandas default: `True`
    **kwargs: Any,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks
    """
    if preview_nrows is not None or preview_offset:
        if (skipfooter := kwargs.pop("skipfooter", None)) is None:
            skipfooter = 0

        # In case we don't have the native nrows given in kwargs, we're going
        # to use the provided preview_nrows
        # and skipfooter should be equal to 0 because of this:
        # cf : https://github.com/pandas-dev/pandas/blob/31c553f1e599e2695a33236e02511c2841ac9aa0/pandas/io/parsers/readers.py#L2209
        if (nrows := kwargs.pop("nrows", None)) is None and skipfooter == 0:
            nrows = preview_nrows

        # In case we don't have the native skiprows given in kwargs,
        # we're going to use the provided preview_offset as range(1, preview_offset + 1)
        if (skiprows := kwargs.pop("skiprows", None)) is None:
            skiprows = range(1, preview_offset + 1)

        chunks = pd.read_csv(
            filepath_or_buffer,
            error_bad_lines=error_bad_lines,
            **kwargs,
            # keep the first row 0 (as the header) and then skip everything else up to row `preview_offset`
            skiprows=skiprows,
            skipfooter=skipfooter,
            nrows=nrows,
        )
        # if the chunksize is not in kwargs, we want to return the iterator
        if kwargs.get("chunksize") is None:
            return next(chunks) if not isinstance(chunks, pd.DataFrame) else chunks
        return chunks

    return pd.read_csv(
        filepath_or_buffer,
        error_bad_lines=error_bad_lines,
        **kwargs,
    )


def _line_count(filepath_or_buffer: "FilePathOrBuffer", encoding: Optional[str]) -> int:
    with open(filepath_or_buffer, encoding=encoding) as f:
        lines = 0
        buf_size = 1024 * 1024
        read_f = f.read  # loop optimization

        trailing_newline: bool = False
        while True:
            buf = read_f(buf_size)
            lines += buf.count("\n")
            if not buf:
                break
            trailing_newline = buf.endswith("\n")

        if not trailing_newline:
            lines += 1

        return lines


def csv_meta(
    filepath_or_buffer: "FilePathOrBuffer", reader_kwargs: Dict[str, Any]
) -> Dict[str, Any]:
    total_rows = _line_count(filepath_or_buffer, reader_kwargs.get("encoding"))

    if "names" not in reader_kwargs and total_rows > 0:  # No header row
        total_rows = total_rows - 1

    if "nrows" in reader_kwargs:
        return {
            "total_rows": total_rows,
            "df_rows": reader_kwargs["nrows"],
        }

    start = 0 + reader_kwargs.get("skiprows", 0)
    end = total_rows - reader_kwargs.get("skipfooter", 0)

    preview_offset = reader_kwargs.get("preview_offset", 0)
    preview_nrows = reader_kwargs.get("preview_nrows", None)

    if preview_nrows is not None:
        return {
            "total_rows": total_rows,
            "df_rows": min(preview_nrows, max(end - start - preview_offset, 0)),
        }
    elif preview_offset:  # and `preview_nrows` is None
        return {
            "total_rows": total_rows,
            "df_rows": max(end - start - preview_offset, 0),
        }
    else:
        return {
            "total_rows": total_rows,
            "df_rows": end - start,
        }
