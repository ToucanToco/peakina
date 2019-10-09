from abc import ABCMeta, abstractmethod
from contextlib import suppress
from datetime import timedelta
from enum import Enum
from pathlib import Path
from time import time
from typing import Optional, Union

import pandas as pd


class CacheEnum(str, Enum):
    MEMORY = 'memory'
    HDF = 'hdf'


class Cache(metaclass=ABCMeta):
    @staticmethod
    def get_cache(kind: CacheEnum, *args, **kwargs):
        return {CacheEnum.MEMORY: InMemoryCache, CacheEnum.HDF: HDFCache}[kind](*args, **kwargs)

    @staticmethod
    def should_invalidate(
        *,
        mtime: Optional[float] = None,
        cached_mtime: float,
        expire: Optional[timedelta] = None,
        cached_created_at: float,
    ):
        now = time()
        is_newer_version = False
        is_expired = False
        if mtime is not None:
            is_newer_version = mtime != cached_mtime
        if expire is not None:
            is_expired = now > cached_created_at + expire.total_seconds()
        return is_newer_version or is_expired

    @abstractmethod
    def get(
        self, key: str, mtime: Optional[float] = None, expire: Optional[timedelta] = None
    ) -> pd.DataFrame:  # pragma: no cover
        pass

    @abstractmethod
    def set(self, key: str, value: pd.DataFrame, mtime: Optional[float] = None):  # pragma: no cover
        pass

    @abstractmethod
    def delete(self, key: str):  # pragma: no cover
        pass


class InMemoryCache(Cache):
    def __init__(self):
        self._cache = {}

    def get(
        self, key: str, mtime: Optional[float] = None, expire: Optional[timedelta] = None
    ) -> pd.DataFrame:
        cached = self._cache[key]
        if self.should_invalidate(
            mtime=mtime,
            cached_mtime=cached['mtime'],
            expire=expire,
            cached_created_at=cached['created_at'],
        ):
            self.delete(key)
        return self._cache[key]['value']

    def set(self, key: str, value: pd.DataFrame, mtime: Optional[float] = None):
        mtime = mtime or time()
        self._cache[key] = {'value': value, 'mtime': mtime, 'created_at': time()}

    def delete(self, key: str):
        if key in self._cache:
            del self._cache[key]


class HDFCache(Cache):
    META_DF_KEY = '__meta__'

    def __init__(self, cache_dir: Union[str, Path]):
        self.cache_dir = Path(cache_dir).resolve()

    def get_metadata(self) -> pd.DataFrame:
        """
        metadata is a dataframe containing last mtime and created_at fields
        for each cached datasource, identified by its key (= its hash).
        If metadata file is not found or is corrupted, an empty one is recreated.
        """
        try:
            # We manually instantiate the HDFStore to be able to close it no matter what
            # See https://github.com/pandas-dev/pandas/pull/28429 for more infos
            store = pd.HDFStore(self.cache_dir / self.META_DF_KEY, mode='r')
            try:
                metadata = pd.read_hdf(store)
            finally:
                store.close()
        except Exception:  # catch all, on purpose
            metadata = pd.DataFrame(columns=['key', 'mtime', 'created_at'])
            self.set_metadata(metadata)
        return metadata

    def set_metadata(self, df: pd.DataFrame):
        df.to_hdf(self.cache_dir / self.META_DF_KEY, self.META_DF_KEY, mode='w')

    def get(
        self, key: str, mtime: Optional[float] = None, expire: Optional[timedelta] = None
    ) -> pd.DataFrame:
        metadata = self.get_metadata()
        try:
            # look for the row concerning the desired key in the metadata dataframe:
            infos = metadata[metadata.key == key].iloc[0].to_dict()
        except IndexError:
            raise KeyError(key)

        if self.should_invalidate(
            mtime=mtime,
            cached_mtime=infos['mtime'],
            expire=expire,
            cached_created_at=infos['created_at'],
        ):
            self.delete(key)

        try:
            return pd.read_hdf(self.cache_dir / key)
        except FileNotFoundError:
            raise KeyError(key)

    def set(self, key: str, value: pd.DataFrame, mtime: Optional[float] = None):
        mtime = mtime or time()
        infos = {'key': key, 'mtime': mtime, 'created_at': time()}
        metadata = self.get_metadata()
        try:
            # add new row to the metadata dataframe:
            metadata = metadata[metadata.key != key]  # drop duplicates
            metadata = metadata.append(infos, ignore_index=True)
            self.set_metadata(metadata)
            value.to_hdf(self.cache_dir / key, key, mode='w')
        except OSError:
            self.delete(key)
            raise

    def delete(self, key: str):
        metadata = self.get_metadata()
        metadata = metadata[metadata.key != key]
        self.set_metadata(metadata)
        with suppress(FileNotFoundError):
            (self.cache_dir / key).unlink()
