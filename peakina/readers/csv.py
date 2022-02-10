"""
Module to add csv support
"""
from typing import Any, Dict, Optional

import pandas as pd

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


def read_csv(
    filepath: str,
    *,
    sep: str = ",",
    keep_default_na: Any = None,
    encoding: Optional[str] = None,
    preview: Dict[str, int] = {},
    chunksize: int = 0,
    nrows: int = 500,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """
    preview_dataframe: pd.DataFrame = pd.DataFrame()

    def _process_chunk(chunk: pd.DataFrame, preview_dataframe: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([preview_dataframe, chunk], ignore_index=True)

    if bool(preview):
        with pd.read_csv(
            filepath,
            sep=sep,
            keep_default_na=keep_default_na,
            encoding=encoding,
            nrows=preview.get("nrows", 50),
            skiprows=lambda idx: idx < preview.get("offset", 0),
            chunksize=PREVIEW_CHUNK_SIZE,
        ) as reader:
            for chunk in reader:
                preview_dataframe = _process_chunk(chunk, preview_dataframe)
            return preview_dataframe

    if chunksize > 0:
        return pd.read_csv(
            filepath,
            nrows=nrows,
            sep=sep,
            chunksize=chunksize,
            encoding=encoding,
            keep_default_na=keep_default_na,
        )
    else:
        return pd.read_csv(
            filepath, nrows=nrows, sep=sep, encoding=encoding, keep_default_na=keep_default_na
        )
