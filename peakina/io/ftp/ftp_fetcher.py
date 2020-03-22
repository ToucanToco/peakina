import os
from typing import BinaryIO, Dict, List, Optional

from ..fetcher import Fetcher, register
from .ftp_utils import FTP_SCHEMES, dir_mtimes, ftp_mtime, ftp_open


@register(schemes=FTP_SCHEMES)
class FTPFetcher(Fetcher):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._mtimes_cache: Dict[str, Dict[str, Optional[int]]] = {}

    def get_dir_mtimes(self, dirpath: str) -> Dict[str, Optional[int]]:
        if dirpath not in self._mtimes_cache:
            self._mtimes_cache[dirpath] = dir_mtimes(dirpath)
        return self._mtimes_cache[dirpath]

    def open(self, filepath) -> BinaryIO:
        return ftp_open(filepath)

    def listdir(self, dirpath) -> List[str]:
        """
        Make use of listdir to get all mtimes of remote files at once and
        put this information in cache.
        This will save us time by avoiding to create a new FTP connection
        for each file when we'll want to get its mtime.
        """
        return list(self.get_dir_mtimes(dirpath).keys())

    def mtime(self, filepath) -> Optional[int]:
        dirpath, filename = os.path.split(filepath)
        if dirpath in self._mtimes_cache:
            return self._mtimes_cache[dirpath][filename]
        else:
            return ftp_mtime(filepath)
