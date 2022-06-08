"""
This module provides mainly methods to validate and detect
the type (CSV, Excel...), the encoding and the separator (for CSV) of a file.
It's also where the reference of all supported types is done.
In order to support a new type, you need to create a reader in the `readers`
package and add the MIME types and the name of the method in `SUPPORTED_FILE_TYPES`
The reader needs to take a filepath as first parameter and return a dataframe
"""

import csv
import inspect
import mimetypes
import os
from datetime import datetime
from enum import Enum
from itertools import islice
from typing import Any, Callable, Dict, List, NamedTuple, Optional, cast

import chardet
import pandas as pd

from peakina.readers import (
    csv_meta,
    excel_meta,
    read_csv,
    read_excel,
    read_geo_data,
    read_json,
    read_xml,
)


class TypeInfos(NamedTuple):
    # All the MIME types for a given type of file
    mime_types: List[str]
    # The method to open a given type of file with the `filepath` as first parameter
    # It needs to return a dataframe
    reader: Callable[..., pd.DataFrame]
    # If the default reader has some missing declared kwargs, it's useful
    # to declare them for `validate_kwargs` method
    reader_kwargs: List[str] = []
    metadata_reader: Optional[Callable[..., Dict[str, Any]]] = None


# For files without MIME types, we make fake MIME types based on detected extension
CUSTOM_MIMETYPES = {".parquet": "peakina/parquet", ".geojson": "peakina/geo"}

EXTRA_PEAKINA_READER_KWARGS = ["preview_offset", "preview_nrows"]

SUPPORTED_FILE_TYPES = {
    "csv": TypeInfos(
        ["text/csv", "text/tab-separated-values"],
        read_csv,
        [],
        csv_meta,
    ),
    "excel": TypeInfos(
        [
            "application/vnd.ms-excel",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ],
        read_excel,
        ["encoding", "decimal"],
        excel_meta,
    ),
    "geodata": TypeInfos(
        ["peakina/geo"],
        read_geo_data,
    ),
    "json": TypeInfos(
        ["application/json"],
        read_json,
        ["filter"],  # this option comes from read_json, which @wraps(pd.read_json)
    ),
    "parquet": TypeInfos(["peakina/parquet"], pd.read_parquet),
    "xml": TypeInfos(["application/xml", "text/xml"], read_xml),
}


# FIXME: Right now mypy does not handle properly the Enum API to generate TypeEnum
#        We would like to have something like:
#        > TypeEnum = Enum('TypeEnum', names={v.upper(): v for v in SUPPORTED_TYPES}, type=str)
class TypeEnum(str, Enum):
    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    PARQUET = "parquet"
    XML = "xml"
    GEODATA = "geodata"


def detect_type(filepath: str, is_regex: bool = False) -> Optional[TypeEnum]:
    """
    Detects the type of a file, which can be a regex or not!
    Can return None in case of generic extension (filepath='...*') with is_regex=True.
    """
    if is_regex:
        filepath = filepath.rstrip("$")
    mimetype, _ = mimetypes.guess_type(filepath)

    if mimetype in ("application/geo+json", "application/vnd.geo+json"):
        return TypeEnum.GEODATA

    # Fallback on custom MIME types
    if mimetype is None:
        _, fileext = os.path.splitext(filepath)
        if fileext in CUSTOM_MIMETYPES:
            mimetype = CUSTOM_MIMETYPES[fileext]

    if is_regex and mimetype is None:  # generic extension with `is_regex=True`
        return None
    try:
        detected_type = [
            type_
            for type_, type_infos in SUPPORTED_FILE_TYPES.items()
            if mimetype in type_infos.mime_types
        ][0]
        return TypeEnum(detected_type)
    except IndexError:
        raise ValueError(
            f"Unsupported mimetype {mimetype!r}. "
            f'Supported types are: {", ".join(map(repr, SUPPORTED_FILE_TYPES))}.'
        )


def bytes_head(filepath: str, n: int) -> bytes:
    """Returns the first `n` lines of a file as a bytes string."""
    with open(filepath, "rb") as f:
        return b"".join(line for line in islice(f, n))


def str_head(filepath: str, n: int, encoding: Optional[str] = None) -> str:
    """Returns the first `n` lines of a file as a string."""
    with open(filepath, encoding=encoding) as f:
        return "".join(line for line in islice(f, n))


def detect_encoding(filepath: str) -> str:
    """Detects the encoding of a file based on its 100 first lines."""
    return cast(str, chardet.detect(bytes_head(filepath, 100))["encoding"])


def validate_encoding(filepath: str, encoding: Optional[str] = None) -> bool:
    """Validates if `encoding` seems ok to read the file based on its 100 first lines."""
    try:
        str_head(filepath, 100, encoding)
        return True
    except UnicodeDecodeError:
        return False


def detect_sep(filepath: str, encoding: Optional[str] = None) -> str:
    """Detect separator of a CSV file based on its 100 first lines"""
    return csv.Sniffer().sniff(str_head(filepath, 100, encoding)).delimiter


def validate_sep(filepath: str, sep: str = ",", encoding: Optional[str] = None) -> bool:
    """
    Validates if the `sep` is a right separator of a CSV file
    (i.e. the dataframe has more than one column).
    """
    try:
        # we want an error to be raised if we can't read the first two lines
        # hence the parameter `error_bad_lines` set to `True`
        df = read_csv(filepath, sep=sep, encoding=encoding, nrows=2, error_bad_lines=True)
        return len(df.columns) > 1
    except pd.errors.ParserError:
        return False


def get_reader_allowed_params(t: TypeEnum) -> List[str]:
    reader = SUPPORTED_FILE_TYPES[t].reader
    return [kw for kw in inspect.signature(reader).parameters]


def validate_kwargs(kwargs: Dict[str, Any], t: Optional[TypeEnum]) -> bool:
    """
    Validate that kwargs are at least in one signature of the methods
    Raises an error if it's not the case
    """
    types: List[TypeEnum] = [t] if t is not None else [TypeEnum(t) for t in SUPPORTED_FILE_TYPES]
    allowed_kwargs: List[str] = []
    for t in types:
        allowed_kwargs += get_reader_allowed_params(t)
        # Add extra allowed kwargs
        allowed_kwargs += SUPPORTED_FILE_TYPES[t].reader_kwargs
        allowed_kwargs += EXTRA_PEAKINA_READER_KWARGS
    bad_kwargs = set(kwargs) - set(allowed_kwargs)
    if bad_kwargs:
        raise ValueError(f'Unsupported kwargs: {", ".join(map(repr, bad_kwargs))}')
    return True


def mdtm_to_string(mtime: int) -> str:
    """Convert the last modification date of a file as an iso string"""
    return datetime.utcfromtimestamp(mtime).isoformat() + "Z"


def pd_read(filepath: str, t: str, kwargs: Dict[str, Any]) -> pd.DataFrame:
    return SUPPORTED_FILE_TYPES[t].reader(filepath, **kwargs)


def get_metadata(filepath: str, type: str, reader_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    metadata_reader = SUPPORTED_FILE_TYPES[type].metadata_reader
    return metadata_reader(filepath, reader_kwargs) if metadata_reader else {}
