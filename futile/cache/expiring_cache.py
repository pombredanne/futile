import sys
import time
import threading

from futile.cache import Cache, _DEFAULT_TIMEOUT


class ExpiringCache(Cache):
    def __init__(self, default_timeout=_DEFAULT_TIMEOUT):
        self._default_timeout = default_timeout
        self._cache = {}
        self._lock = threading.RLock()

    def clear(self):
        with self._lock:
            self._cache = {}

    def get(self, key, default=None):
        item = self._cache.get(key)
        if not item:
            return default
        expire_time, value = item
        if expire_time < time.time():
            return default
        return value

    def put(self, key, val, timeout=None):
        with self._lock:
            if timeout is None:
                timeout = self._default_timeout
            self._cache[key] = (time.time() + timeout, val)

    def invalidate(self, key):
        with self._lock:
            try:
                del self._cache[key]
            except KeyError:
                pass
