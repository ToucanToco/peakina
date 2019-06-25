import tempfile
from email.utils import parsedate_to_datetime
from typing import List, TextIO

from urllib3 import PoolManager

from ..fetcher import Fetcher, register


@register(schemes=['http', 'https'])
class HttpFetcher(Fetcher):
    @staticmethod
    def open(filepath) -> TextIO:
        r = PoolManager().request('GET', filepath, preload_content=False)
        ret = tempfile.NamedTemporaryFile(suffix='.httptmp')
        for chunk in r.stream():
            ret.write(chunk)
        ret.seek(0)
        return ret

    @staticmethod
    def listdir(dirpath) -> List[str]:
        raise NotImplementedError

    @staticmethod
    def mtime(filepath) -> int:
        r = PoolManager().request('HEAD', filepath)
        if 'last-modified' in r.headers:
            return parsedate_to_datetime(r.headers['last-modified']).timestamp()
