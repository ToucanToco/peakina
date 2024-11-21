"""
Module to enhance pandas.read_json with JQ filter
"""

from functools import wraps
from typing import TYPE_CHECKING

import pandas as pd
import pyarrow.dataset as ds

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = str | bytes | PathLike[str] | PathLike[bytes]


@wraps(pd.read_parquet)
def read_parquet(
    path_or_buf: "FilePathOrBuffer",
    preview_offset: int = 0,
    preview_nrows: int | None = None,
    columns: list[int] | None = None,
) -> pd.DataFrame:
    dataset = ds.dataset(source=path_or_buf, format="parquet")

    if preview_nrows is not None:
        indices = range(preview_offset, preview_offset + preview_nrows)
        table = dataset.take(indices=indices, columns=columns)
    else:
        table = dataset.to_table(columns=columns)

    return table.to_pandas()
