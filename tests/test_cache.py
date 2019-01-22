import unittest
import time

from futile.cache.expiring_cache import ExpiringCache


class CacheTestCase(unittest.TestCase):

    def setUp(self):
        self._ecache= ExpiringCache(5)


    def test_cache(self):
        self._ecache.put("foo", "bar")
        val = self._ecache.get("foo")
        self.assertEqual(val, "bar")
        time.sleep(5)
        val = self._ecache.get("foo")
        self.assertEqual(val, None)
