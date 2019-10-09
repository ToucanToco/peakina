from typing import BinaryIO, List

from ..fetcher import Fetcher, register
from .s3_utils import S3_SCHEMES, s3_open


@register(schemes=S3_SCHEMES)
class S3Fetcher(Fetcher):
    @staticmethod
    def open(filepath, **fetcher_kwargs) -> BinaryIO:
        return s3_open(filepath, **fetcher_kwargs)

    @staticmethod
    def listdir(dirpath) -> List[str]:
        raise NotImplementedError

    @staticmethod
    def mtime(filepath) -> int:
        raise NotImplementedError
