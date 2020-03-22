import os
from typing import Any, BinaryIO, Dict, List, Optional

from ..fetcher import Fetcher, register
from .s3_utils import S3_SCHEMES, dir_mtimes, s3_mtime, s3_open


@register(schemes=S3_SCHEMES)
class S3Fetcher(Fetcher):
    def __init__(self, *, client_kwargs: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(**kwargs)
        self.client_kwargs = client_kwargs
        self._mtimes_cache: Dict[str, Dict[str, int]] = {}

    def get_dir_mtimes(self, dirpath: str) -> Dict[str, int]:
        if dirpath not in self._mtimes_cache:
            self._mtimes_cache[dirpath] = dir_mtimes(dirpath, client_kwargs=self.client_kwargs)
        return self._mtimes_cache[dirpath]

    def open(self, filepath) -> BinaryIO:
        return s3_open(filepath, client_kwargs=self.client_kwargs)

    def listdir(self, dirpath) -> List[str]:
        return list(self.get_dir_mtimes(dirpath).keys())

    def mtime(self, filepath) -> Optional[int]:
        dirpath, filename = os.path.split(filepath)
        if dirpath in self._mtimes_cache:
            return self._mtimes_cache[dirpath][filename]
        else:
            return s3_mtime(filepath, client_kwargs=self.client_kwargs)
