"""
inspired by repoze.lru, http://repoze.org/license.html
"""
import threading
from abc import ABC, abstractmethod


class Cache(ABC):

    @abstractmethod
    def clear(self):
        """Remove all entries from the cache"""

    @abstractmethod
    def get(self, key, default=None):
        """Return value for key. If not in cache, return default"""

    @abstractmethod
    def put(self, key, val):
        """Add key to the cache with value val"""

    @abstractmethod
    def invalidate(self, key):
        """Remove key from the cache"""


class UnboundedCache(Cache):
    """
    a simple unbounded cache backed by a dictionary
    """

    def __init__(self):
        self._data = dict()

    def get(self, key, default=None):
        return self._data.get(key, default)

    def clear(self):
        self._data.clear()

    def invalidate(self, key):
        try:
            del self._data[key]
        except KeyError:
            pass

    def put(self, key, val):
        self._data[key] = val


_MARKER = object()


class ClockCache(Cache):
    """
    Actually not a LRU Cache, but implements a pseudo-LRU algorithm (CLOCK)

    CLOCK 算法参见：https://blog.csdn.net/u012432778/article/details/46519551

    The Clock algorithm is not kept strictly to improve performance, e.g. to
    allow get() and invalidate() to work without acquiring the lock.
    """

    def __init__(self, size: int) -> None:
        assert size >= 1, 'size should be at least 1'
        self.size = size
        self.hand = 0
        self.clock_keys = None  # 当前位置对应的 key
        self.clock_refs = None  # 当前位置是否引用过 TODO use bitvector
        self.data: dict = {}
        self.lock = threading.Lock()

        # stats
        self.evictions = 0
        self.hits = 0
        self.misses = 0
        self.lookups = 0

        self.clear()

    def clear(self):
        with self.lock:
            self.hand = 0
            self.clock_keys = [_MARKER] * self.size
            self.clock_refs = [False] * self.size
            self.data = {}

            self.evictions = 0
            self.hits = 0
            self.misses = 0
            self.lookups = 0

    def get(self, key, default=None):
        self.lookups += 1
        try:
            pos, val = self.data[key]
            self.hits += 1
        except KeyError:
            self.misses += 1
            return default
        self.clock_refs[pos] = True  # 标记为被引用过
        return val

    def put(self, key, val):
        with self.lock:
            entry = self.data.get(key)
            # 如果已经在缓存中，直接更新即可
            if entry is not None:
                data[key] = pos, val
                self.clock_refs[key] = True
                return
            hand = self.hand
            count = 0
            max_count = self.size / 2
            # 寻找插入位置
            while True:
                ref = self.clock_refs[hand]
                if ref:
                    self.clock_refs[hand] = False
                    hand = (hand + 1) % self.size
                    count += 1
                    if count >= max_count:
                        self.clock_refs[hand] = False
                else:
                    # 如果当前位置没有使用
                    old_key = self.clock_keys[hand]
                    old_entry = self.data.pop(old_key, _MARKER)
                    if old_entry is not _MARKER:
                        self.evictions += 1
                    self.clock_keys[hand] = key
                    self.clock_refs[hand] = True
                    self.data[key] = hand, val
                    hand = (hand + 1) % self.size
                    self.hand = hand
                    break

    def invalidate(self, key):
        entry = self.data.pop(key, _MARKER)
        if entry is not _MARKER:
            self.clock_refs[entry[0]] = False


class ExpiringClockCache(Cache):

    """
    包含过期时间的 LRU Cache

    Actually not a LRU Cache, but implements a pseudo-LRU algorithm (CLOCK)

    The Clock algorithm is not kept strictly to improve performance, e.g. to
    allow get() and invalidate() to work without acquiring the lock.
    """

    def __init__(self, size: int, timeout: int = 3600) -> None:
        assert size >= 1, 'size should be at least 1'
        assert timeout > 0, 'timeout should be positive'
        self.size = size
        self.hand = 0
        self.clock_keys = None  # 当前位置对应的 key
        self.clock_refs = None  # 当前位置是否引用过 TODO use bitvector
        self.data: dict = {}
        self.timeout = timeout
        self.lock = threading.Lock()

        # stats
        self.evictions = 0
        self.hits = 0
        self.misses = 0
        self.lookups = 0

        self.clear()

    def clear(self):
        with self.lock:
            self.hand = 0
            self.clock_keys = [_MARKER] * self.size
            self.clock_refs = [False] * self.size
            self.data = {}

            self.evictions = 0
            self.hits = 0
            self.misses = 0
            self.lookups = 0

    def get(self, key, default=None):
        self.lookups += 1
        try:
            pos, val, ttl = self.data[key]
        except KeyError:
            self.misses += 1
            return default
        if ttl > time.time():
            self.hits += 1
            self.clock_refs[pos] = True  # 标记为被引用过
            return val
        else:
            self.misses += 1
            self.clock_refs[pos] = False
            return default

    def put(self, key, val):
        with self.lock:
            entry = self.data.get(key)
            # 如果已经在缓存中，直接更新即可
            if entry is not None:
                data[key] = pos, val, time.time() + self.timeout
                self.clock_refs[key] = True
                return
            hand = self.hand
            count = 0
            max_count = self.size / 2
            # 寻找插入位置
            while True:
                ref = self.clock_refs[hand]
                if ref:
                    self.clock_refs[hand] = False
                    hand = (hand + 1) % self.size
                    count += 1
                    if count >= max_count:
                        self.clock_refs[hand] = False
                else:
                    # 如果当前位置没有使用
                    old_key = self.clock_keys[hand]
                    old_entry = self.data.pop(old_key, _MARKER)
                    if old_entry is not _MARKER:
                        self.evictions += 1
                    self.clock_keys[hand] = key
                    self.clock_refs[hand] = True
                    self.data[key] = hand, val, time.time() + self.timeout
                    hand = (hand + 1) % self.size
                    self.hand = hand
                    break

    def invalidate(self, key):
        entry = self.data.pop(key, _MARKER)
        if entry is not _MARKER:
            self.clock_refs[entry[0]] = False
