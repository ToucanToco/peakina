from typing import BinaryIO, List

from ..fetcher import Fetcher, register
from .s3_utils import S3_SCHEMES, s3_list_dir, s3_mtime, s3_read


@register(schemes=S3_SCHEMES)
class S3Fetcher(Fetcher):
    @staticmethod
    def open(s3_url, **kwargs) -> BinaryIO:
        return s3_read(s3_url, **kwargs)

    @staticmethod
    def listdir(s3_url, **kwargs) -> List[str]:
        return s3_list_dir(s3_url, **kwargs)

    @staticmethod
    def mtime(s3_url, **kwargs) -> int:
        return s3_mtime(s3_url, **kwargs)
