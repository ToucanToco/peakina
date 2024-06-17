"""
Module to enhance pandas.read_json with JQ filter
"""

import json
from functools import wraps
from typing import TYPE_CHECKING, Any

import jq
import pandas as pd

if TYPE_CHECKING:
    from os import PathLike

    FilePathOrBuffer = str | bytes | PathLike[str] | PathLike[bytes]


def transform_with_jq(json_input: str, jq_filter: str) -> str:
    """Apply a jq filter on raw json input, outputs raw json (may be on several lines)"""
    return jq.text(jq_filter, json.loads(json_input))  # type: ignore[no-any-return]


@wraps(pd.read_json)
def read_json(
    path_or_buf: "FilePathOrBuffer",
    encoding: str = "utf-8",
    filter: str | None = None,
    preview_offset: int = 0,
    preview_nrows: int | None = None,
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    if filter is None:
        filter = "."

    with open(path_or_buf, encoding=encoding) as f:
        path_or_buf = transform_with_jq(f.read(), filter)

    # In case we don't have the native nrows given in kwargs, we're going
    # to use the provided preview_nrows
    if (nrows := kwargs.get("nrows", preview_nrows)) is not None:
        if kwargs.get("lines") and kwargs.get("lines") is True:
            # cf: https://github.com/pandas-dev/pandas/blob/main/pandas/io/json/_json.py#L671
            kwargs["nrows"] = nrows
        else:
            data = json.loads(path_or_buf)
            if isinstance(data, list):
                path_or_buf = json.dumps(
                    data[preview_offset : nrows + preview_offset]
                )  # pragma: no cover

    return pd.read_json(path_or_buf, encoding=encoding, *args, **kwargs)
