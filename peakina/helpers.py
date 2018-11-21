import csv
import inspect
import mimetypes
from itertools import islice
from typing import Optional

import chardet


def bytes_head(file_path: str, n: int) -> bytes:
    """Returns bytes string of `n` first lines of `file`"""
    with open(file_path, 'rb') as f:
        return b''.join(line for line in islice(f, n))


def str_head(file_path: str, n: int, encoding: str = None) -> str:
    """Returns string of `n` first lines of `file`"""
    with open(file_path, encoding=encoding) as f:
        return ''.join(line for line in islice(f, n))


def validate_encoding(file_path: str, encoding: str = None) -> bool:
    """Detect encoding of a file based on its 100 first lines"""
    with open(file_path, encoding=encoding) as f:
        try:
            f.read()
            return True
        except UnicodeDecodeError:
            return False


def detect_encoding(file_path: str) -> str:
    """Detect encoding of a file based on its 100 first lines"""
    return chardet.detect(bytes_head(file_path, 100))['encoding']


def detect_sep(file_path: str, encoding: str = None):
    """Detect separator of a file based on its 100 first lines"""
    return csv.Sniffer().sniff(str_head(file_path, 100, encoding)).delimiter


SUPPORTED_MIMETYPES = {
    'text/csv': 'csv',
    'text/tab-separated-values': 'csv',
    'application/vnd.ms-excel': 'excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'excel',
    'application/json': 'json',
}
SUPPORTED_TYPES = sorted(set(SUPPORTED_MIMETYPES.values()))


def detect_type(file_path: str) -> Optional[str]:
    """
    Detect type of `file_path`.
    Returns None if the type couldn't be guessed
    """
    mimetype, _ = mimetypes.guess_type(file_path)
    if mimetype is None:
        return None
    try:
        return SUPPORTED_MIMETYPES[mimetype]
    except KeyError:
        raise UnknownType('Unknown detected type')


def guess_type(file_path: str, is_regex: bool) -> Optional[str]:
    """
    Try to guess the type of `file_path`
    Returns None in case of generic extension (is_regex=True and file_path=`...*')
    """
    if is_regex:
        file_path = file_path.rstrip('$')
    detected_type = detect_type(file_path)
    if is_regex and detected_type is None:
        return None
    if detected_type not in SUPPORTED_TYPES:
        raise UnknownType(f'Unknown guessed type {detected_type!r}')
    return detected_type


def validate_kwargs(kwargs: dict, methods: list) -> bool:
    """Validate that kwargs are at least in one signature of the methods"""
    allowed_kwargs = {kw for method in methods for kw in inspect.signature(method).parameters}
    bad_kwargs = set(kwargs) - allowed_kwargs
    if bad_kwargs:
        raise ValueError(f'Unsupported kwargs: {", ".join(map(repr, bad_kwargs))}')
    return True


class UnknownType(Exception):
    """raised when type is unknown"""

    def __init__(self, message):
        message = f'{message}. Supported types are: {", ".join(map(repr, SUPPORTED_TYPES))}.'
        super().__init__(message)
