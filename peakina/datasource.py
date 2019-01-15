"""
This module provides access to the DataSource class.
A datasource is defined by one or many files matching a pattern and some extra parameters like
encoding, separator...and its only method is `get_df` to retrieve the pandas DataFrame for
the given parameters.
"""
from typing import IO, Optional
from urllib.parse import urlparse, uses_netloc, uses_params, uses_relative

import pandas as pd
from dataclasses import field
from pydantic.dataclasses import dataclass

from .helpers import (
    TypeEnum,
    detect_encoding,
    detect_sep,
    detect_type,
    validate_encoding,
    validate_kwargs,
    validate_sep,
)
from .io import Fetcher, MatchEnum

PD_VALID_URLS = set(uses_relative + uses_netloc + uses_params) | set(Fetcher.registry)


@dataclass
class DataSource:
    uri: str
    type: TypeEnum = None
    match: MatchEnum = None
    extra_kwargs: dict = field(default_factory=dict)

    def __post_init__(self):
        self.scheme = urlparse(self.uri).scheme
        if self.scheme not in PD_VALID_URLS:
            raise AttributeError(f'Invalid scheme {self.scheme!r}')

        self.type = self.type or detect_type(self.uri, is_regex=bool(self.match))

        validate_kwargs(self.extra_kwargs, self.type)

    @staticmethod
    def _get_single_df(stream: IO, filetype: Optional[TypeEnum], **kwargs) -> pd.DataFrame:
        """
        Read a stream and retrieve the data frame
        It uses `stream.name`, which is the path to a local file (often temporary)
        to avoid closing it. It will be closed at the end of the method.
        """
        if filetype is None:
            filetype = TypeEnum(detect_type(stream.name))

        # Check encoding
        encoding = kwargs.get('encoding')
        if not validate_encoding(stream.name, encoding):
            encoding = detect_encoding(stream.name)
        kwargs['encoding'] = encoding

        # Check separator for CSV files if it's not set
        if filetype is TypeEnum.CSV and 'sep' not in kwargs:
            if not validate_sep(stream.name, encoding=encoding):
                kwargs['sep'] = detect_sep(stream.name, encoding)

        pd_read = getattr(pd, f'read_{filetype}')
        try:
            return pd_read(stream.name, **kwargs)
        finally:
            stream.close()

    def get_df(self) -> pd.DataFrame:
        fetcher = Fetcher.get_fetcher(self.uri, self.match)
        dfs = []
        for filename, stream in fetcher.get_files():
            df = self._get_single_df(stream, self.type, **self.extra_kwargs)
            if self.match:
                df['__filename__'] = filename
            dfs.append(df)
        return pd.concat(dfs, sort=False).reset_index(drop=True)
