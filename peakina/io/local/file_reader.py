import os
from typing import List, TextIO

from ..reader import Reader, register


@register(schemes='')
class FileReader(Reader):
    @staticmethod
    def open(filepath) -> TextIO:
        return open(filepath)

    @staticmethod
    def listdir(dirpath) -> List[str]:
        return os.listdir(dirpath)

    @staticmethod
    def mtime(filepath) -> int:
        return int(os.path.getmtime(filepath))
