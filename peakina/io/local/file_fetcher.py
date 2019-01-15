import os
from typing import List, TextIO

from ..fetcher import Fetcher, register


@register(schemes='')
class FileFetcher(Fetcher):
    @staticmethod
    def open(filepath) -> TextIO:
        return open(filepath)

    @staticmethod
    def listdir(dirpath) -> List[str]:
        return os.listdir(dirpath)

    @staticmethod
    def mtime(filepath) -> int:
        return int(os.path.getmtime(filepath))
