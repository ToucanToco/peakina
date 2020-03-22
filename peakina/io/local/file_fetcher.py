import os
from typing import List, TextIO

from ..fetcher import Fetcher, register


@register(schemes='')
class FileFetcher(Fetcher):
    def open(self, filepath) -> TextIO:
        return open(filepath)

    def listdir(self, dirpath) -> List[str]:
        return os.listdir(dirpath)

    def mtime(self, filepath) -> int:
        return int(os.path.getmtime(filepath))
