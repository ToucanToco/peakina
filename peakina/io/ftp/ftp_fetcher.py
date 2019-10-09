import os
from typing import BinaryIO, List, Optional

from ..fetcher import Fetcher, register
from .ftp_utils import FTP_SCHEMES, dir_mtimes, ftp_mtime, ftp_open


@register(schemes=FTP_SCHEMES)
class FTPFetcher(Fetcher):
    def open(self, filepath, **fetcher_kwargs) -> BinaryIO:
        return ftp_open(filepath)

    def listdir(self, dirpath) -> List[str]:
        """
        Make use of listdir to get all mtimes of remote files at once and
        put this information in cache.
        This will save us time by avoiding to create a new FTP connection
        for each file when we'll want to get its mtime.
        """
        self._mtimes_cache = dir_mtimes(dirpath)
        return list(self._mtimes_cache.keys())

    def mtime(self, filepath) -> Optional[int]:
        filename = os.path.basename(filepath)
        if hasattr(self, '_mtimes_cache') and filename in self._mtimes_cache:
            return self._mtimes_cache[filename]
        else:
            return ftp_mtime(filepath)
