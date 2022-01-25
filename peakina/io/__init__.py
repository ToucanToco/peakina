from .fetcher import Fetcher, MatchEnum, fetch
from .ftp.ftp_fetcher import FTPFetcher
from .http.http_fetcher import HttpFetcher
from .local.file_fetcher import FileFetcher
from .s3.s3_fetcher import S3Fetcher

__all__ = (
    "Fetcher",
    "MatchEnum",
    "FTPFetcher",
    "HttpFetcher",
    "FileFetcher",
    "S3Fetcher",
    "fetch",
)
