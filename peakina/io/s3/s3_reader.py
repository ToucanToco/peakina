from typing import BinaryIO, List

from ..reader import Reader, register
from .s3_utils import S3_SCHEMES, s3_open


@register(schemes=S3_SCHEMES)
class S3Reader(Reader):
    @staticmethod
    def open(filepath) -> BinaryIO:
        return s3_open(filepath)

    @staticmethod
    def listdir(dirpath) -> List[str]:
        raise NotImplementedError

    @staticmethod
    def mtime(filepath) -> int:
        raise NotImplementedError
