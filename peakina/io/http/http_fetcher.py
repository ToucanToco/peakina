import tempfile
from email.utils import parsedate_to_datetime
from typing import IO, Any

import certifi
import urllib3

from ..fetcher import Fetcher, register


@register(schemes=["http", "https"])
class HttpFetcher(Fetcher):
    def __init__(self, *, verify: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        if verify:
            self.pool_manager = urllib3.PoolManager(
                cert_reqs="CERT_REQUIRED", ca_certs=certifi.where()
            )
        else:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            self.pool_manager = urllib3.PoolManager(cert_reqs="CERT_NONE", assert_hostname=False)

    def open(self, filepath: str) -> IO[bytes]:
        r = self.pool_manager.request("GET", filepath, preload_content=False, **self.extra_kwargs)
        # the caller is responsible for closing the returned file
        ret = tempfile.NamedTemporaryFile(suffix=".httptmp")  # noqa: SIM115
        for chunk in r.stream():
            ret.write(chunk)
        ret.seek(0)
        return ret

    def listdir(self, dirpath: str) -> list[str]:
        raise NotImplementedError

    def mtime(self, filepath: str) -> int | None:
        try:
            r = self.pool_manager.request("HEAD", filepath)
        except Exception:  # noqa: BLE001 # any failure means we can't know the mtime
            return None
        else:
            dt = parsedate_to_datetime(r.headers["last-modified"])
            return int(dt.timestamp())
