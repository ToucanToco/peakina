import csv
import mimetypes
from itertools import islice

import chardet


def bytes_head(file_path: str, n: int) -> bytes:
    """Returns bytes string of `n` first lines of `file`"""
    with open(file_path, 'rb') as f:
        return b''.join(line for line in islice(f, n))


def str_head(file_path: str, n: int, encoding: str = None) -> str:
    """Returns string of `n` first lines of `file`"""
    with open(file_path, encoding=encoding) as f:
        return ''.join(line for line in islice(f, n))


def validate_encoding(file_path: str, encoding: str) -> bool:
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


def detect_type(file_path: str) -> str:
    supported_mimetypes = {
        'text/csv': 'csv',
        'application/vnd.ms-excel': 'excel',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'excel',
        'application/json': 'json',
    }
    mimetype, _ = mimetypes.guess_type(file_path)
    try:
        return supported_mimetypes[mimetype]
    except KeyError:
        supported_types = sorted(set(supported_mimetypes.values()))
        raise UnknownType(f'Unknown type. Supported types are: {", ".join(supported_types)}')


class UnknownType(Exception):
    """raised when type is unknown"""
