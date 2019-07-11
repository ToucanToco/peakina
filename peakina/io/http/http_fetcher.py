import tempfile
from email.utils import parsedate_to_datetime
from typing import IO, List

from urllib3 import PoolManager

from ..fetcher import Fetcher, register


@register(schemes=['http', 'https'])
class HttpFetcher(Fetcher):
    def open(self, filepath) -> IO:
        r = PoolManager().request('GET', filepath, preload_content=False)
        ret = tempfile.NamedTemporaryFile(suffix='.httptmp')
        for chunk in r.stream():
            ret.write(chunk)
        ret.seek(0)
        return ret

    def listdir(self, dirpath) -> List[str]:
        raise NotImplementedError

    def mtime(self, filepath) -> int:
        r = PoolManager().request('HEAD', filepath)
        if 'last-modified' not in r.headers:
            raise KeyError
        dt = parsedate_to_datetime(r.headers['last-modified'])
        return int(dt.timestamp())
