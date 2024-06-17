"""
Module to add xml support
"""

from typing import Any, cast

import jq
import pandas as pd
import xmltodict

PdDatalist = list[dict[str, Any]]
PdDatadict = dict[str, list[Any]]


def transform_with_jq(data: Any, jq_filter: str) -> PdDatalist | PdDatadict:
    """Apply a jq filter on data before it's passed to a pd.DataFrame"""
    all_data: list[PdDatalist] | list[PdDatadict] | PdDatalist = jq.all(jq_filter, data)

    # If the data is already presented as a list of rows,
    # then undo the nesting caused by "multiple_output" jq option
    if len(all_data) == 1 and (
        isinstance(all_data[0], list)  # list[PdDatalist]
        # detects another valid datastructure [{col1:[value, ...], col2:[value, ...]}]
        or (
            isinstance(all_data[0], dict) and isinstance(list(all_data[0].values())[0], list)
        )  # list[PdDatadict]
    ):
        return all_data[0]
    else:
        return cast(PdDatalist, all_data)


def read_xml(
    filepath: str,
    encoding: str = "utf-8",
    preview_offset: int = 0,
    preview_nrows: int | None = None,
    filter: str | None = None,
) -> pd.DataFrame:
    data = xmltodict.parse(open(filepath).read(), encoding=encoding)
    if filter is not None:
        data = transform_with_jq(data, filter)
    if isinstance(data, list) and isinstance(preview_nrows, int):
        data = data[preview_offset : preview_nrows + preview_offset]
    return pd.DataFrame(data)
