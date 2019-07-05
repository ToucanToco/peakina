from datetime import timedelta
from time import time


class InMemoryCache:
    def __init__(self):
        self._cache = {}

    def get(self, key, mtime=None, expire: timedelta = None):
        cached = self._cache[key]
        now = time()
        is_newer_version = False
        is_expired = False
        if mtime is not None:
            is_newer_version = mtime > cached['mtime']
        if expire is not None:
            is_expired = now > cached['created_at'] + expire.total_seconds()
        if is_newer_version or is_expired:
            self.delete(key)
        return self._cache[key]['value']

    def set(self, key, value, mtime=None):
        mtime = mtime or time()
        self._cache[key] = {'value': value, 'mtime': mtime, 'created_at': time()}

    def delete(self, key):
        if key in self._cache:
            del self._cache[key]
