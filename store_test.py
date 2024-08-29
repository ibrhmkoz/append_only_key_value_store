import unittest

from store import Store, IndexedStore, BufferedStore, CachedStore


class TestStore(unittest.TestCase):
    def setUp(self):
        self.store = BufferedStore(IndexedStore(Store('test.db')), 10)
