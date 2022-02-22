"""
Module to add csv support
"""
from functools import wraps
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]

# The chunksize value for previews
PREVIEW_CHUNK_SIZE = 1024


@wraps(pd.read_csv)
def read_csv(
    filepath_or_buffer: "FilePathOrBuffer",
    *,
    # extra `peakina` reader kwargs
    preview_offset: int = 0,
    preview_nrows: Optional[int] = None,
    # change of default values
    keep_default_na: bool = False,  # pandas default: `True`
    error_bad_lines: bool = False,  # pandas default: `True`
    **kwargs: Any,
) -> pd.DataFrame:
    """
    The read_csv method is able to make a preview by reading on chunks
    """
    if preview_nrows is not None:
        chunks = pd.read_csv(
            filepath_or_buffer,
            keep_default_na=keep_default_na,
            error_bad_lines=error_bad_lines,
            **kwargs,
            # keep the first row 0 (as the header) and then skip everything else up to row `preview_offset`
            skiprows=range(1, preview_offset + 1),
            nrows=preview_nrows,
            chunksize=PREVIEW_CHUNK_SIZE,
        )
        return next(chunks)

    return pd.read_csv(
        filepath_or_buffer,
        keep_default_na=keep_default_na,
        error_bad_lines=error_bad_lines,
        **kwargs,
    )


def csv_meta(
    filepath_or_buffer: "FilePathOrBuffer", reader_kwrgs: Dict[str, Any]
) -> Dict[str, Any]:

    preview_nrows, preview_offset = None, None
    if "preview_offset" in reader_kwrgs:
        preview_offset = reader_kwrgs.pop("preview_offset")

    if "preview_nrows" in reader_kwrgs:
        preview_nrows = reader_kwrgs.pop("preview_nrows")

    df_rows = pd.read_csv(
        filepath_or_buffer,
        **reader_kwrgs,
        skiprows=range(1, preview_offset + 1) if preview_offset else None,
        nrows=preview_nrows,
    ).shape[0]

    total_rows = pd.read_csv(filepath_or_buffer, **reader_kwrgs).shape[0]

    return {"total_rows": total_rows, "df_rows": df_rows}
