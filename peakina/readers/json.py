"""
Module to enhance pandas.read_json with JQ filter
"""
from functools import wraps

import pandas as pd
from jq import jq


def transform_with_jq(json_input: str, jq_filter: str) -> str:
    """Apply a jq filter on raw json input, outputs raw json (may be on several lines)"""
    return jq(jq_filter).transform(text=json_input, text_output=True)


@wraps(pd.read_json)
def read_json(
    path_or_buf=None, encoding: str = 'utf-8', filter: str = None, *args, **kwargs
) -> pd.DataFrame:
    if filter is not None:
        with open(path_or_buf, encoding=encoding) as f:
            path_or_buf = transform_with_jq(f.read(), filter)
    return pd.read_json(path_or_buf, encoding=encoding, *args, **kwargs)
