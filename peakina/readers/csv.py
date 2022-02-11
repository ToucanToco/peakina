"""
Module to add csv support
"""
from typing import Optional

import pandas as pd

from peakina.readers.common import PreviewArgs

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


def read_csv(
    filepath: str,
    *,
    sep: str = ",",
    keep_default_na: bool = False,
    encoding: str = "utf-8",
    preview: Optional[PreviewArgs] = None,
    chunksize: Optional[int] = None,
    nrows: int = 500,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """

    if preview:
        chunks = pd.read_csv(
            filepath,
            sep=sep,
            keep_default_na=keep_default_na,
            encoding=encoding,
            nrows=preview.nrows,
            skiprows=lambda idx: idx < preview.offset,
            chunksize=PREVIEW_CHUNK_SIZE,
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
