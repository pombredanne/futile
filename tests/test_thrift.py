import unittest
from idl.crawl.test.ttypes import Test
from futile.thrift import thrift_dump, thrift_load


class ThrfitTestCase(unittest.TestCase):

    def setUp(self):
        """"""

    def test_load_and_dump(self):
        test_obj = Test
        test_obj.i64_test = 2**64 - 1
        test_obj.foo = 'test'
        data = thrift_dump(test_obj)
        loaded = thrift_load(data)
        self.assertEqual(loaded.i64_test, 2**64 - 1)
        self.assertEqual(loaded.foo, 'test')
