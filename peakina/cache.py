import time


class InMemoryCache:
    def __init__(self):
        self._cache = {}

    def get(self, key, mtime=-1):
        obj = self._cache[key]
        if obj['mtime'] < mtime:
            self.delete(key)
        return self._cache[key]['value']

    def set(self, key, value, mtime=None):
        mtime = mtime or time.time()
        self._cache[key] = {
            'value': value,
            'mtime': mtime
        }

    def delete(self, key):
        if key in self._cache:
            del self._cache[key]

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)
