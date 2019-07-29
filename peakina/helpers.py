"""
This module provides mainly methods to validate and detect
the type (CSV, Excel...), the encoding and the separator (for CSV) of a file.
"""

import csv
import inspect
import mimetypes
from datetime import datetime
from enum import Enum
from itertools import islice
from typing import Optional, Union

import chardet
import xmltodict
import pandas as pd
from jq import jq


class TypeEnum(str, Enum):
    CSV = 'csv'
    EXCEL = 'excel'
    JSON = 'json'
    XML = 'xml'


CUSTOM_READ_TYPES = [TypeEnum.XML]


MIMETYPE_TYPE_MAPPING = {
    'text/csv': TypeEnum.CSV,
    'text/tab-separated-values': TypeEnum.CSV,
    'application/vnd.ms-excel': TypeEnum.EXCEL,
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': TypeEnum.EXCEL,
    'application/json': TypeEnum.JSON,
    'application/xml': TypeEnum.XML,
}


def detect_type(filepath: str, is_regex: bool = False) -> Optional[TypeEnum]:
    """
    Detects the type of a file, which can be a regex or not!
    Can return None in case of generic extension (filepath='...*') with is_regex=True.
    """
    if is_regex:
        filepath = filepath.rstrip('$')
    mimetype, _ = mimetypes.guess_type(filepath)
    if is_regex and mimetype is None:  # generic extension with `is_regex=True`
        return None
    try:
        return MIMETYPE_TYPE_MAPPING[mimetype]
    except KeyError:
        raise ValueError(
            f'Unsupported mimetype {mimetype!r}. '
            f'Supported types are: {", ".join(map(lambda x: repr(x.value), TypeEnum))}.'
        )


def bytes_head(filepath: str, n: int) -> bytes:
    """Returns the first `n` lines of a file as a bytes string."""
    with open(filepath, 'rb') as f:
        return b''.join(line for line in islice(f, n))


def str_head(filepath: str, n: int, encoding: str = None) -> str:
    """Returns the first `n` lines of a file as a string."""
    with open(filepath, encoding=encoding) as f:
        return ''.join(line for line in islice(f, n))


def detect_encoding(filepath: str) -> str:
    """Detects the encoding of a file based on its 100 first lines."""
    return chardet.detect(bytes_head(filepath, 100))['encoding']


def validate_encoding(filepath: str, encoding: str = None) -> bool:
    """Validates if `encoding` seems ok to read the file based on its 100 first lines."""
    try:
        str_head(filepath, 100, encoding)
        return True
    except UnicodeDecodeError:
        return False


def detect_sep(filepath: str, encoding: str = None) -> str:
    """Detect separator of a CSV file based on its 100 first lines"""
    return csv.Sniffer().sniff(str_head(filepath, 100, encoding)).delimiter


def validate_sep(filepath: str, sep: str = ',', encoding: str = None) -> bool:
    """
    Validates if the `sep` is a right separator of a CSV file
    (i.e. the dataframe has more than one column).
    """
    try:
        df = pd.read_csv(filepath, sep=sep, encoding=encoding, nrows=2)
        return len(df.columns) > 1
    except pd.errors.ParserError:
        return False


def validate_kwargs(kwargs: dict, t: Optional[str]) -> bool:
    """
    Validate that kwargs are at least in one signature of the methods
    Raises an error if it's not the case
    """
    if t == TypeEnum.XML:
        allowed_kwargs = {'filter', 'encoding'}
    else:
        types = [t] if t else [t for t in TypeEnum if t not in CUSTOM_READ_TYPES]
        methods = [getattr(pd, f'read_{t}') for t in types]
        allowed_kwargs = {kw for method in methods for kw in inspect.signature(method).parameters}

    if t is None or t == TypeEnum.EXCEL:
        # this option is missing from read_excel signature in pandas 0.23:
        allowed_kwargs.add('keep_default_na')
    bad_kwargs = set(kwargs) - allowed_kwargs
    if bad_kwargs:
        raise ValueError(f'Unsupported kwargs: {", ".join(map(repr, bad_kwargs))}')
    return True


def mdtm_to_string(mtime: int) -> str:
    """Convert the last modification date of a file as an iso string"""
    return datetime.utcfromtimestamp(mtime).isoformat() + 'Z'


def pd_read(filepath: str, t: str, kwargs: dict) -> pd.DataFrame:
    if t == TypeEnum.XML:
        return read_xml(filepath, **kwargs)
    else:
        pd_read = getattr(pd, f'read_{t}')
        return pd_read(filepath, **kwargs)


def read_xml(filepath: str, encoding: str = None, filter: str = None) -> pd.DataFrame:
    data = xmltodict.parse(open(filepath).read(), encoding=encoding or 'utf-8')
    if filter is not None:
        data = transform_with_jq(data, filter)
    return pd.DataFrame(data)


def transform_with_jq(data: Union[dict, list], jq_filter: str) -> list:
    """Our standard way to apply a jq filter on data before it's passed to a pd.DataFrame"""
    data = jq(jq_filter).transform(data, multiple_output=True)

    # If the data is already presented as a list of rows,
    # then undo the nesting caused by "multiple_output" jq option
    if len(data) == 1 and (
        isinstance(data[0], list)
        or (
            # detects another valid datastructure [{col1:[value, ...], col2:[value, ...]}]
            isinstance(data[0], dict)
            and isinstance(list(data[0].values())[0], list)
        )
    ):
        return data[0]  # type: ignore
    else:
        return data  # type: ignore
