import tempfile
from email.utils import parsedate_to_datetime
from typing import IO, List, Optional

import urllib3

from ..fetcher import Fetcher, register

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@register(schemes=['http', 'https'])
class HttpFetcher(Fetcher):
    def __init__(self, *args, **kwargs):
        self.pool_manager = urllib3.PoolManager(cert_reqs='CERT_NONE', assert_hostname=False)
        super().__init__(*args, **kwargs)

    def open(self, filepath, **fetcher_kwargs) -> IO:
        r = self.pool_manager.request('GET', filepath, preload_content=False, **fetcher_kwargs)
        ret = tempfile.NamedTemporaryFile(suffix='.httptmp')
        for chunk in r.stream():
            ret.write(chunk)
        ret.seek(0)
        return ret

    def listdir(self, dirpath) -> List[str]:
        raise NotImplementedError

    def mtime(self, filepath) -> Optional[int]:
        try:
            r = self.pool_manager.request('HEAD', filepath)
        except Exception:
            return None
        else:
            dt = parsedate_to_datetime(r.headers['last-modified'])
            return int(dt.timestamp())
