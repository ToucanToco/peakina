"""
This module provides mainly methods to validate and detect
the type (CSV, Excel...), the encoding and the separator (for CSV) of a file.
It's also where the reference of all supported types is done.
In order to support a new type, you need to create a reader in the `readers`
package and add the MIME types and the name of the method in `SUPPORTED_TYPES`
The reader needs to take a filepath as first parameter and return a dataframe
"""

import csv
import inspect
import mimetypes
from datetime import datetime
from enum import Enum
from itertools import islice
from typing import Callable, List, NamedTuple, Optional

import chardet
import pandas as pd

from .readers import read_json, read_xml


class StrEnum(str, Enum):
    """Generic class to support string enums"""


class TypeInfos(NamedTuple):
    # All the MIME types for a given type of file
    mime_types: List[str]
    # The method to open a given type of file with the `filepath` as first parameter
    # It needs to return a dataframe
    reader: Callable[..., pd.DataFrame]
    # If the default reader has some missing declared kwargs, it's useful
    # to declare them for `validate_kwargs` method
    extra_kwargs: List[str] = []


SUPPORTED_TYPES = {
    'csv': TypeInfos(['text/csv', 'text/tab-separated-values'], pd.read_csv),
    'excel': TypeInfos(
        [
            'application/vnd.ms-excel',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        ],
        pd.read_excel,
        ['keep_default_na'],  # this option is missing from read_excel signature in pandas 0.23
    ),
    'json': TypeInfos(
        ['application/json'],
        read_json,
        ['filter'],  # this option comes from read_json, which @wraps(pd.read_json)
    ),
    'xml': TypeInfos(['application/xml'], read_xml),
}


TypeEnum = StrEnum('TypeEnum', {v.upper(): v for v in SUPPORTED_TYPES})  # type: ignore


def detect_type(filepath: str, is_regex: bool = False) -> Optional[TypeEnum]:  # type: ignore
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
        detected_type = [
            type_
            for type_, type_infos in SUPPORTED_TYPES.items()
            if mimetype in type_infos.mime_types
        ][0]
        return TypeEnum(detected_type)
    except IndexError:
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
    types = [t] if t else [t for t in TypeEnum]
    allowed_kwargs: List[str] = []
    for t in types:
        reader = SUPPORTED_TYPES[t].reader
        allowed_kwargs += [kw for kw in inspect.signature(reader).parameters]
        # Add extra allowed kwargs
        allowed_kwargs += SUPPORTED_TYPES[t].extra_kwargs
    bad_kwargs = set(kwargs) - set(allowed_kwargs)
    if bad_kwargs:
        raise ValueError(f'Unsupported kwargs: {", ".join(map(repr, bad_kwargs))}')
    return True


def mdtm_to_string(mtime: int) -> str:
    """Convert the last modification date of a file as an iso string"""
    return datetime.utcfromtimestamp(mtime).isoformat() + 'Z'


def pd_read(filepath: str, t: str, kwargs: dict) -> pd.DataFrame:
    return SUPPORTED_TYPES[t].reader(filepath, **kwargs)
