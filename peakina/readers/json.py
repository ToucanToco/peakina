"""
Module to enhance pandas.read_json with JQ filter
"""
import json
from functools import wraps
from typing import TYPE_CHECKING, Any, Optional, Union

import jq
import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]


def transform_with_jq(json_input: str, jq_filter: str) -> str:
    """Apply a jq filter on raw json input, outputs raw json (may be on several lines)"""
    return jq.text(jq_filter, json.loads(json_input))  # type: ignore[no-any-return]


@wraps(pd.read_json)
def read_json(
    path_or_buf: "FilePathOrBuffer",
    encoding: str = "utf-8",
    filter: Optional[str] = None,
    preview_offset: int = 0,
    preview_nrows: Optional[int] = None,
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    if filter is not None:
        with open(path_or_buf, encoding=encoding) as f:
            path_or_buf = transform_with_jq(f.read(), filter)

    # for the preview_nrows and the preview_offset, we're going to convert in to list here
    if preview_nrows is not None:
        # In case we don't have the native nrows given in kwargs, we're going
        # to use the provided preview_nrows
        if (nrows := kwargs.get("nrows")) is None:
            nrows = preview_nrows

        kwargs["nrows"] = nrows

    return pd.read_json(path_or_buf, encoding=encoding, *args, **kwargs)
