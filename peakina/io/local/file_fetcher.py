import os
from typing import IO

from ..fetcher import Fetcher, register


@register(schemes="")
class FileFetcher(Fetcher):
    def open(self, filepath: str) -> IO[str]:
        return open(filepath)

    def listdir(self, dirpath: str) -> list[str]:
        return os.listdir(dirpath)

    def mtime(self, filepath: str) -> int:
        return int(os.path.getmtime(filepath))
