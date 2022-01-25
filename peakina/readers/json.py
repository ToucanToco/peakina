"""
Module to enhance pandas.read_json with JQ filter
"""
import json
from functools import wraps
from os import PathLike
from typing import Any, Optional, Union

import pandas as pd
import pyjq

FilePathOrBuffer = Union[str, bytes, PathLike[str], PathLike[bytes]]


def transform_with_jq(json_input: str, jq_filter: str) -> str:
    """Apply a jq filter on raw json input, outputs raw json (may be on several lines)"""
    return "\n".join(json.dumps(x) for x in pyjq.all(jq_filter, json.loads(json_input)))


@wraps(pd.read_json)
def read_json(
    path_or_buf: FilePathOrBuffer,
    encoding: str = "utf-8",
    filter: Optional[str] = None,
    *args: Any,
    **kwargs: Any,
) -> pd.DataFrame:
    if filter is not None:
        with open(path_or_buf, encoding=encoding) as f:
            path_or_buf = transform_with_jq(f.read(), filter)
    return pd.read_json(path_or_buf, encoding=encoding, *args, **kwargs)
