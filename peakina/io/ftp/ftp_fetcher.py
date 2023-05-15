import os
from typing import IO, Any

from peakina.cache import timed_lru_cache

from ..fetcher import Fetcher, register
from .ftp_utils import FTP_SCHEMES, dir_mtimes, ftp_mtime, ftp_open


@timed_lru_cache(maxsize=3, seconds=60)
def get_mtimes_cache(**kwargs: Any) -> dict[str, dict[str, int | None]]:
    """
    This function allows to share a common _mtime_cache object between several
    FTPFetcher objects, as long as they were instanciating with the same params.
    """
    _mtimes_cache: dict[str, dict[str, int | None]] = {}
    return _mtimes_cache


@register(schemes=FTP_SCHEMES)
class FTPFetcher(Fetcher):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._mtimes_cache: dict[str, dict[str, int | None]] = get_mtimes_cache(**kwargs)

    def get_dir_mtimes(self, dirpath: str) -> dict[str, int | None]:
        if dirpath not in self._mtimes_cache:
            self._mtimes_cache[dirpath] = dir_mtimes(dirpath)
        return self._mtimes_cache[dirpath]

    def open(self, filepath: str) -> IO[bytes]:
        return ftp_open(filepath)

    def listdir(self, dirpath: str) -> list[str]:
        """
        Make use of listdir to get all mtimes of remote files at once and
        put this information in cache.
        This will save us time by avoiding to create a new FTP connection
        for each file when we'll want to get its mtime.
        """
        return list(self.get_dir_mtimes(dirpath).keys())

    def mtime(self, filepath: str) -> int | None:
        dirpath, filename = os.path.split(filepath)
        if dirpath in self._mtimes_cache:
            return self._mtimes_cache[dirpath][filename]
        else:
            return ftp_mtime(filepath)
