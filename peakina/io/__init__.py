# flake8: noqa
from .fetcher import Fetcher, MatchEnum
from .ftp.ftp_fetcher import FTPFetcher
from .http.http_fetcher import HttpFetcher
from .local.file_fetcher import FileFetcher
from .s3.s3_fetcher import S3Fetcher
