import tempfile
from email.utils import parsedate_to_datetime
from typing import IO, List, Optional

import certifi
import urllib3

from ..fetcher import Fetcher, register


@register(schemes=['http', 'https'])
class HttpFetcher(Fetcher):
    def __init__(self, *, verify=True, **kwargs):
        super().__init__(**kwargs)
        if verify:
            self.pool_manager = urllib3.PoolManager(
                cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()
            )
        else:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.pool_manager = urllib3.PoolManager(cert_reqs='CERT_NONE', assert_hostname=False)

    def open(self, filepath) -> IO:
        r = self.pool_manager.request('GET', filepath, preload_content=False, **self.extra_kwargs)
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
