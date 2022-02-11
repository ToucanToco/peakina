"""
Module to add csv support
"""
from typing import Optional, TypedDict

import pandas as pd

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


class PreviewArgs(TypedDict, total=False):
    nrows: int
    offset: int


def read_csv(
    filepath: str,
    *,
    sep: str = ",",
    keep_default_na: bool = False,
    encoding: Optional[str] = None,
    preview: Optional[PreviewArgs] = None,
    chunksize: Optional[int] = None,
    nrows: int = 500,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """

    if preview:
        with pd.read_csv(
            filepath,
            sep=sep,
            keep_default_na=keep_default_na,
            encoding=encoding,
            nrows=preview.get("nrows", 50),
            skiprows=lambda idx: idx < preview.get("offset", 0),
            chunksize=PREVIEW_CHUNK_SIZE,
        ) as reader:
            return next(reader)

    return pd.read_csv(
        filepath,
        nrows=nrows,
        sep=sep,
        chunksize=chunksize,
        encoding=encoding,
        keep_default_na=keep_default_na,
    )
