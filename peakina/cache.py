from abc import ABCMeta, abstractmethod
from contextlib import suppress
from datetime import timedelta
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from time import monotonic_ns, time
from typing import Any, TypedDict

import pandas as pd


class InMemoryCached(TypedDict):
    value: pd.DataFrame
    mtime: float
    created_at: float


class CacheEnum(str, Enum):
    MEMORY = "memory"
    HDF = "hdf"


class Cache(metaclass=ABCMeta):
    @staticmethod
    def get_cache(kind: CacheEnum, *args: Any, **kwargs: Any) -> "Cache":
        ALL_CACHES = {
            CacheEnum.MEMORY: InMemoryCache,
            CacheEnum.HDF: HDFCache,
        }
        return ALL_CACHES[kind](*args, **kwargs)  # type: ignore[no-any-return]

    @staticmethod
    def should_invalidate(
        *,
        mtime: float | None = None,
        cached_mtime: float,
        expire: timedelta | None = None,
        cached_created_at: float,
    ) -> bool:
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
        self, key: str, mtime: float | None = None, expire: timedelta | None = None
    ) -> pd.DataFrame:
        """get a cached value"""

    @abstractmethod
    def set(self, key: str, value: pd.DataFrame, mtime: float | None = None) -> None:
        """set a cached value"""

    @abstractmethod
    def delete(self, key: str) -> None:
        """delete a cached value"""


class InMemoryCache(Cache):
    def __init__(self) -> None:
        self._cache: dict[str, InMemoryCached] = {}

    def get(
        self, key: str, mtime: float | None = None, expire: timedelta | None = None
    ) -> pd.DataFrame:
        cached = self._cache[key]
        if self.should_invalidate(
            mtime=mtime,
            cached_mtime=cached["mtime"],
            expire=expire,
            cached_created_at=cached["created_at"],
        ):
            self.delete(key)
        return self._cache[key]["value"]

    def set(self, key: str, value: pd.DataFrame, mtime: float | None = None) -> None:
        mtime = mtime or time()
        self._cache[key] = {"value": value, "mtime": mtime, "created_at": time()}

    def delete(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]


class HDFCache(Cache):
    META_DF_KEY = "__meta__"

    def __init__(self, cache_dir: str | Path) -> None:
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
            store = pd.HDFStore(self.cache_dir / self.META_DF_KEY, mode="r")
            try:
                metadata = pd.read_hdf(store)
            finally:
                store.close()
        except Exception:  # catch all, on purpose
            metadata = pd.DataFrame(columns=["key", "mtime", "created_at"])
            self.set_metadata(metadata)
        return metadata

    def set_metadata(self, df: pd.DataFrame) -> None:
        df.to_hdf(self.cache_dir / self.META_DF_KEY, self.META_DF_KEY, mode="w")

    def get(
        self, key: str, mtime: float | None = None, expire: timedelta | None = None
    ) -> pd.DataFrame:
        metadata = self.get_metadata()
        try:
            # look for the row concerning the desired key in the metadata dataframe:
            infos = metadata[metadata.key == key].iloc[0].to_dict()
        except IndexError:
            raise KeyError(key)

        if self.should_invalidate(
            mtime=mtime,
            cached_mtime=infos["mtime"],
            expire=expire,
            cached_created_at=infos["created_at"],
        ):
            self.delete(key)

        try:
            return pd.read_hdf(self.cache_dir / key)
        except FileNotFoundError:
            raise KeyError(key)

    def set(self, key: str, value: pd.DataFrame, mtime: float | None = None) -> None:
        mtime = mtime or time()
        infos = {"key": key, "mtime": mtime, "created_at": time()}
        metadata = self.get_metadata()
        try:
            # add new row to the metadata dataframe:
            metadata = metadata[metadata.key != key]  # drop duplicates
            metadata = pd.concat([metadata, pd.Series(infos).to_frame().T], ignore_index=True)
            self.set_metadata(metadata)
            value.to_hdf(self.cache_dir / key, key, mode="w")
        except OSError:
            self.delete(key)
            raise

    def delete(self, key: str) -> None:
        metadata = self.get_metadata()
        metadata = metadata[metadata.key != key]
        self.set_metadata(metadata)
        with suppress(FileNotFoundError):
            (self.cache_dir / key).unlink()


# taken from https://gist.github.com/Morreski/c1d08a3afa4040815eafd3891e16b945
def timed_lru_cache(
    _func: Any = None, *, seconds: int = 600, maxsize: int = 128, typed: bool = False
) -> Any:
    """Extension of functools lru_cache with a timeout
    Parameters:
    seconds (int): Timeout in seconds to clear the WHOLE cache, default = 10 minutes
    maxsize (int): Maximum Size of the Cache
    typed (bool): Same value of different type will be a different entry
    """

    def wrapper_cache(f: Any) -> Any:
        f = lru_cache(maxsize=maxsize, typed=typed)(f)
        f.delta = seconds * 10**9
        f.expiration = monotonic_ns() + f.delta

        @wraps(f)
        def wrapped_f(*args: Any, **kwargs: Any) -> Any:
            if monotonic_ns() >= f.expiration:  # pragma: no cover
                f.cache_clear()
                f.expiration = monotonic_ns() + f.delta
            return f(*args, **kwargs)

        wrapped_f.cache_info = f.cache_info  # type: ignore [attr-defined]
        wrapped_f.cache_clear = f.cache_clear  # type: ignore [attr-defined]
        return wrapped_f

    # To allow decorator to be used without arguments
    if _func is None:
        return wrapper_cache
    else:  # pragma: no cover
        return wrapper_cache(_func)
