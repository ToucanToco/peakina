"""
This module provides access to the DataSource class.
A datasource is defined by one or many files matching a pattern and some extra parameters like
encoding, separator...and its only method is `get_df` to retrieve the pandas DataFrame for
the given parameters.
"""
import os
from contextlib import suppress
from dataclasses import asdict, field
from datetime import timedelta
from hashlib import md5
from typing import IO, Any, Dict, Generator, Iterable, Optional, Union
from urllib.parse import urlparse, uses_netloc, uses_params, uses_relative

import pandas as pd
from pydantic.dataclasses import dataclass
from slugify import slugify

from peakina.cache import Cache
from peakina.helpers import (
    TypeEnum,
    detect_encoding,
    detect_sep,
    detect_type,
    get_metadata,
    get_reader_allowed_params,
    pd_read,
    validate_encoding,
    validate_kwargs,
    validate_sep,
)
from peakina.io import Fetcher, MatchEnum

AVAILABLE_SCHEMES = set(Fetcher.registry) - {""}  # discard the empty string scheme
PD_VALID_URLS = set(uses_relative + uses_netloc + uses_params) | AVAILABLE_SCHEMES


@dataclass
class DataSource:
    uri: str
    type: Optional[TypeEnum] = None
    match: Optional[MatchEnum] = None
    expire: Optional[timedelta] = None
    reader_kwargs: Dict[str, Any] = field(default_factory=dict)
    fetcher_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init_post_parse__(self) -> None:
        self._fetcher: Optional[Fetcher] = None
        self.scheme = urlparse(self.uri).scheme
        if self.scheme not in PD_VALID_URLS:
            raise AttributeError(f"Invalid scheme {self.scheme!r}")

        self.type = self.type or detect_type(self.uri, is_regex=bool(self.match))

        validate_kwargs(self.reader_kwargs, self.type)

    @property
    def fetcher(self) -> Fetcher:
        if self._fetcher is None:
            self._fetcher = Fetcher.get_fetcher(self.uri, **self.fetcher_kwargs)
        return self._fetcher

    @property
    def hash(self) -> str:
        identifier = asdict(self)
        del identifier["expire"]
        hash_ = md5(str(identifier).encode("utf-8")).hexdigest()
        filename = slugify(os.path.basename(self.uri), separator="_")
        return f"_{filename}_{hash_}"

    def get_metadata(self) -> Dict[str, Any]:
        """Return datasource metadata (e.g. excel sheetnames)"""
        if self.match:
            return {}  # no metadata for matched datasources
        with self.fetcher.open(self.uri) as f:
            assert self.type is not None

            allowed_params = get_reader_allowed_params(self.type)
            # Auto-detect encoding if not present
            encoding = self.reader_kwargs.get("encoding")
            if "encoding" in allowed_params:
                if not validate_encoding(f.name, encoding):
                    encoding = detect_encoding(f.name)
                self.reader_kwargs["encoding"] = encoding

            return get_metadata(f.name, self.type, self.reader_kwargs)

    @staticmethod
    def _get_single_df(
        stream: Union[IO[bytes], IO[str]], filetype: Optional[TypeEnum], **kwargs: Any
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        """
        Read a stream and retrieve the data frame or data frame generator (chunks)
        It uses `stream.name`, which is the path to a local file (often temporary)
        to avoid closing it. It will be closed at the end of the method.
        """
        if filetype is None:
            filetype = TypeEnum(detect_type(stream.name))
        allowed_params = get_reader_allowed_params(filetype)

        # Check encoding
        encoding = kwargs.get("encoding")
        if "encoding" in allowed_params:
            if not validate_encoding(stream.name, encoding):
                encoding = detect_encoding(stream.name)
            kwargs["encoding"] = encoding

        # Check separator for CSV files if it's not set
        if "sep" in allowed_params and "sep" not in kwargs:
            if not validate_sep(stream.name, encoding=encoding):
                kwargs["sep"] = detect_sep(stream.name, encoding)

        try:
            df = pd_read(stream.name, filetype, kwargs)
        finally:
            stream.close()

        return df

    def get_matched_datasources(self) -> Generator["DataSource", None, None]:
        my_args = asdict(self)
        for uri in self.fetcher.get_filepath_list(self.uri, self.match):
            overriden_args = {**my_args, "uri": uri, "match": None}
            yield DataSource(**overriden_args)  # type: ignore[arg-type]

    def get_dfs(self, cache: Optional[Cache] = None) -> Generator[pd.DataFrame, None, None]:
        """
        From the conf of the datasource, returns a generator
        with all the dataframes
        The generator can have a single dataframe (single file as input
        without options) or many (e.g. with `match` or `chunksize`)
        """
        by_chunk = self.reader_kwargs.get("chunksize") is not None
        with_cache = cache is not None and self.expire and not by_chunk

        for datasource in self.get_matched_datasources():
            if with_cache:
                cache_key = datasource.hash
                cache_mtime = None
                with suppress(NotImplementedError, KeyError, OSError):
                    cache_mtime = self.fetcher.mtime(datasource.uri)

                with suppress(KeyError):
                    assert cache is not None
                    df = cache.get(key=cache_key, mtime=cache_mtime, expire=self.expire)
                    yield df
                    continue

            stream = self.fetcher.open(datasource.uri)
            try:
                df = self._get_single_df(stream, self.type, **self.reader_kwargs)
                dfs = df if by_chunk else [df]
            except pd.errors.EmptyDataError:
                dfs = [pd.DataFrame()]

            for df in dfs:
                if self.match:
                    df["__filename__"] = os.path.basename(datasource.uri)
                if with_cache:
                    assert cache is not None
                    cache.set(key=cache_key, value=df, mtime=cache_mtime)
                yield df

    def get_df(self, cache: Optional[Cache] = None) -> pd.DataFrame:
        return pd.concat([x for x in self.get_dfs(cache=cache)], sort=False).reset_index(drop=True)


def read_pandas(
    uri: str,
    *,
    type: Optional[TypeEnum] = None,
    match: Optional[MatchEnum] = None,
    expire: Optional[timedelta] = None,
    fetcher_kwargs: Optional[Dict[str, Any]] = None,
    **reader_kwargs: Any,
) -> pd.DataFrame:
    return DataSource(
        uri=uri,
        type=type,
        match=match,
        expire=expire,
        fetcher_kwargs=fetcher_kwargs or {},
        reader_kwargs=reader_kwargs,
    ).get_df()
