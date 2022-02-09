from typing import Any, Dict, Optional

import pandas as pd

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


def read_csv(
    filepath: str, sep: str = ",", encoding: Optional[str] = None, **kwargs: Any
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks

    """

    preview: bool = kwargs.get("preview", False)
    preview_args: Dict[str, Any] = kwargs.get("preview_args", {})

    if preview:
        return pd.read_csv(
            filepath,
            **kwargs,
            nrows=preview_args.get("nrows", 50),
            skiprows=lambda idx: idx < preview_args.get("offset", 0),
            chunksize=PREVIEW_CHUNK_SIZE,
        )

    return pd.read_csv(filepath, **kwargs)
