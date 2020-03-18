"""
Module to add xml support
"""
from typing import Union

import pandas as pd
import pyjq
import xmltodict


def transform_with_jq(data: Union[dict, list], jq_filter: str) -> list:
    """Apply a jq filter on data before it's passed to a pd.DataFrame"""
    data = pyjq.all(jq_filter, data)

    # If the data is already presented as a list of rows,
    # then undo the nesting caused by "multiple_output" jq option
    if len(data) == 1 and (
        isinstance(data[0], list)
        or
        # detects another valid datastructure [{col1:[value, ...], col2:[value, ...]}]
        (isinstance(data[0], dict) and isinstance(list(data[0].values())[0], list))
    ):
        return data[0]  # type: ignore
    else:
        return data  # type: ignore


def read_xml(filepath: str, encoding: str = 'utf-8', filter: str = None) -> pd.DataFrame:
    data = xmltodict.parse(open(filepath).read(), encoding=encoding)
    if filter is not None:
        data = transform_with_jq(data, filter)
    return pd.DataFrame(data)
