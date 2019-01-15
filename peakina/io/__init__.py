# flake8: noqa
from .local.file_fetcher import FileFetcher
from .ftp.ftp_fetcher import FTPFetcher
from .s3.s3_fetcher import S3Fetcher
from .fetcher import Fetcher, MatchEnum
