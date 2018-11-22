import logging
from contextlib import suppress
from enum import Enum
from typing import Optional
from typing.io import BinaryIO
from urllib.parse import urlparse, uses_netloc, uses_params, uses_relative

import pandas as pd
from dataclasses import field
from pydantic.dataclasses import dataclass

from .helpers import (
    detect_encoding,
    detect_sep,
    detect_type,
    guess_type,
    validate_encoding,
    validate_kwargs,
)
from .io.ftp_utils import ftp_open, ftp_schemes
from .io.s3_utils import s3_open, s3_schemes
from .matcher import MatchEnum, Matcher

PD_VALID_URLS = set(uses_relative + uses_netloc + uses_params + ftp_schemes + s3_schemes)

logger = logging.getLogger(__name__)


class TypeEnum(str, Enum):
    CSV = 'csv'
    EXCEL = 'excel'
    JSON = 'json'


@dataclass
class DataSource:
    file: str
    type: TypeEnum = None
    match: MatchEnum = None
    extra_kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        self.scheme = urlparse(self.file).scheme
        if self.scheme not in PD_VALID_URLS:
            raise AttributeError(f'Unvalid scheme "{self.scheme}"')
        if self.type is None:
            with suppress(ValueError):  # if `guess_type` returns None
                self.type = TypeEnum(guess_type(self.file, bool(self.match)))
        if self.type:
            pandas_methods = [getattr(pd, f'read_{self.type}')]
        else:
            pandas_methods = [getattr(pd, f'read_{f_type}') for f_type in TypeEnum]
        validate_kwargs(self.extra_kwargs, pandas_methods)

    @staticmethod
    def _open(file_path) -> BinaryIO:
        scheme = urlparse(file_path).scheme
        if scheme in ftp_schemes:
            return ftp_open(file_path)
        elif scheme in s3_schemes:
            return s3_open(file_path)
        else:
            return open(file_path, 'rb')

    @staticmethod
    def _get_single_df(file_path: str, file_type: Optional[TypeEnum], **kwargs) -> pd.DataFrame:
        """Read a file_path and retrieve the data frame"""
        if file_type is None:
            file_type = TypeEnum(detect_type(file_path))

        file_stream = DataSource._open(file_path)
        file_stream_path = file_stream.name

        encoding = kwargs.get('encoding')
        if not validate_encoding(file_stream_path, encoding):
            encoding = kwargs['encoding'] = detect_encoding(file_stream_path)

        if file_type is TypeEnum.CSV and 'sep' not in kwargs:
            kwargs['sep'] = detect_sep(file_stream_path, encoding)

        pd_read = getattr(pd, f'read_{file_type}')
        try:
            return pd_read(file_stream_path, **kwargs)
        finally:
            file_stream.close()

    def get_df(self):
        all_files = Matcher.all_matches(self.file, self.scheme, self.match)
        return pd.concat(
            [
                self._get_single_df(file_path, self.type, **self.extra_kwargs)
                for file_path in all_files
            ]
        )
