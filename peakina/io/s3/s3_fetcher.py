import os
from typing import BinaryIO, List, Optional

from ..fetcher import Fetcher, register
from .s3_utils import S3_SCHEMES, dir_mtimes, s3_mtime, s3_open


@register(schemes=S3_SCHEMES)
class S3Fetcher(Fetcher):
    @staticmethod
    def open(filepath, **fetcher_kwargs) -> BinaryIO:
        return s3_open(filepath, **fetcher_kwargs)

    def listdir(self, dirpath, **fetcher_kwargs) -> List[str]:
        self._mtimes_cache = dir_mtimes(dirpath, **fetcher_kwargs)
        return list(self._mtimes_cache.keys())

    def mtime(self, filepath, **fetcher_kwargs) -> Optional[int]:
        filename = os.path.basename(filepath)
        if hasattr(self, '_mtimes_cache') and filename in self._mtimes_cache:
            return self._mtimes_cache[filename]
        else:
            return s3_mtime(filepath, **fetcher_kwargs)
