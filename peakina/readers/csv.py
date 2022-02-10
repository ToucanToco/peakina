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
    encoding: Optional[str] = None,
    preview: bool = False,
    preview_args: Dict[str, Any] = {},
    nrows: int = 50,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """
    preview_dataframe: pd.DataFrame = pd.DataFrame()

    def _process_chunk(chunk: pd.DataFrame, preview_dataframe: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([preview_dataframe, chunk], ignore_index=True)

    if preview:
        with pd.read_csv(
            filepath,
            sep=sep,
            encoding=encoding,
            nrows=preview_args.get("nrows", 50),
            skiprows=lambda idx: idx < preview_args.get("offset", 0),
            chunksize=PREVIEW_CHUNK_SIZE,
        ) as reader:
            for chunk in reader:
                preview_dataframe = _process_chunk(chunk, preview_dataframe)
            return preview_dataframe

    return pd.read_csv(filepath, nrows=nrows, sep=sep, encoding=encoding)
