import logging
from enum import Enum
from typing.io import BinaryIO
from urllib.parse import urlparse, uses_netloc, uses_params, uses_relative

import pandas as pd
from pydantic.dataclasses import dataclass

from .helpers import detect_encoding, detect_sep, detect_type, validate_encoding
from .io.ftp_utils import ftp_open, uses_ftp
from .io.s3_utils import s3_open, uses_s3

PD_VALID_URLS = set(uses_relative + uses_netloc + uses_params + uses_ftp + uses_s3)

logger = logging.getLogger(__name__)


class TypeEnum(str, Enum):
    CSV = 'csv'
    EXCEL = 'excel'
    JSON = 'json'


class MatchEnum(str, Enum):
    REGEX = 'regex'
    GLOB = 'glob'


@dataclass
class DataSource:
    file: str
    type: TypeEnum = None
    encoding: str = None
    sep: str = None
    match: MatchEnum = None

    def __post_init__(self, **kwargs):
        self.scheme = urlparse(self.file).scheme
        if self.scheme not in PD_VALID_URLS:
            raise AttributeError(f'Unvalid scheme "{self.scheme}"')
        self.files = [self.file]
        self.kwargs = kwargs

    def file_stream(self, file) -> BinaryIO:
        if self.scheme in uses_ftp:
            return ftp_open(file)
        elif self.scheme in uses_s3:
            return s3_open(file)
        else:
            return open(file, 'rb')

    def _get_single_df(self, file_path: str, file_type: str, encoding: str, sep: str, **kwargs):
        if file_type is None:
            file_type = TypeEnum(detect_type(file_path))
        file_stream = self.file_stream(file_path)
        file_stream_path = file_stream.name
        if not validate_encoding(file_stream_path, encoding):
            encoding = detect_encoding(file_stream_path)
        if sep is None:
            sep = detect_sep(file_stream_path, encoding)
        pd_read = getattr(pd, f'read_{file_type}')
        return pd_read(file_stream, encoding=encoding, sep=sep, **kwargs)

    @property
    def df(self):
        return pd.concat(
            [
                self._get_single_df(file_path, self.type, self.encoding, self.sep, **self.kwargs)
                for file_path in self.files
            ]
        )
