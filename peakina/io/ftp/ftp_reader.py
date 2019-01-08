from typing import BinaryIO, List

from ..reader import Reader, register
from .ftp_utils import FTP_SCHEMES, ftp_listdir, ftp_mtime, ftp_open


@register(schemes=FTP_SCHEMES)
class FTPReader(Reader):
    @staticmethod
    def open(filepath) -> BinaryIO:
        return ftp_open(filepath)

    @staticmethod
    def listdir(dirpath) -> List[str]:
        return ftp_listdir(dirpath)

    @staticmethod
    def mtime(filepath) -> int:
        return ftp_mtime(filepath)
