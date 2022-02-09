"""
Module to add excel support
"""

from typing import Any, Dict

import pandas as pd

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


def read_excel(filepath: str, **kwargs: Any) -> pd.DataFrame:
    """
    The read_excel function is using openpyxl to parse the csv file and read it

    """

    preview: bool = kwargs.get("preview", False)
    preview_args: Dict[str, Any] = kwargs.get("preview_args", {})

    if preview:
        return pd.read_excel(
            filepath,
            **kwargs,
            nrows=preview_args.get("nrows", 50),
            skiprows=lambda idx: idx < preview_args.get("offset", 0),
            chunksize=PREVIEW_CHUNK_SIZE,
        )

    return pd.read_excel(filepath, **kwargs)
