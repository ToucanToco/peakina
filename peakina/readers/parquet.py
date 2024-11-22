"""
Module to enhance pandas.read_json with JQ filter
"""

from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow.dataset as ds

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = str | bytes | PathLike[str] | PathLike[bytes]


def read_parquet(
    path_or_buf: "FilePathOrBuffer",
    preview_offset: int = 0,
    preview_nrows: int | None = None,
    columns: list[int] | None = None,
    **kwargs: Any,
) -> pd.DataFrame:
    dataset = ds.dataset(source=path_or_buf, format="parquet")
    indices = None

    if preview_nrows is not None:
        indices = range(preview_offset, preview_offset + preview_nrows)
    elif preview_offset > 0:
        indices = range(preview_offset, dataset.count_rows())

    if indices is not None:
        table = dataset.take(indices=indices, columns=columns)
    else:
        table = dataset.to_table(columns=columns)

    return table.to_pandas()
