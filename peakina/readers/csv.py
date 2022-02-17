"""
Module to add csv support
"""
from typing import List, Optional

import pandas as pd

from peakina.readers.common import PreviewArgs

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


def _extract_columns(filepath: str, encoding: str, sep: str) -> List[str]:
    with open(filepath, buffering=10000, encoding=encoding) as ff:
        return ff.readline().replace("\n", "").split(sep)


def read_csv(
    filepath: str,
    *,
    sep: str = ",",
    keep_default_na: bool = False,
    encoding: str = "utf-8",
    preview_offset: Optional[int] = None,
    preview_nrows: Optional[int] = None,
    chunksize: Optional[int] = None,
    nrows: int = 500,
    error_bad_lines: bool = False,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """
    if preview_nrows is not None and preview_offset is not None:
        chunks = pd.read_csv(
            filepath,
            sep=sep,
            header=None,
            names=_extract_columns(filepath, encoding, sep),
            keep_default_na=keep_default_na,
            encoding=encoding,
            nrows=preview_nrows,
            skiprows=lambda idx: idx < preview_offset,
            chunksize=PREVIEW_CHUNK_SIZE,
            error_bad_lines=error_bad_lines,
        )
        return next(chunks)

    return pd.read_csv(
        filepath,
        nrows=nrows,
        sep=sep,
        chunksize=chunksize,
        encoding=encoding,
        keep_default_na=keep_default_na,
    )


def _line_count(filename) -> int:
    f = open(filename)
    lines = 0
    buf_size = 1024 * 1024
    read_f = f.read # loop optimization

    buf = read_f(buf_size)
    while buf:
        lines += buf.count('\n')
        buf = read_f(buf_size)

    return lines


def csv_meta(filepath: str, datasource: 'Datasource') -> dict:
    return {
        "nrows": _line_count(filepath),
    }