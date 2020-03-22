import os
from typing import List, TextIO

from ..fetcher import Fetcher, register


@register(schemes='')
class FileFetcher(Fetcher):
    def open(self, filepath, **fetcher_kwargs) -> TextIO:
        return open(filepath)

    def listdir(self, dirpath, **fetcher_kwargs) -> List[str]:
        return os.listdir(dirpath)

    def mtime(self, filepath, **fetcher_kwargs) -> int:
        return int(os.path.getmtime(filepath))
