import unittest

from store import Store, IndexedStore, BufferedStore, CachedStore


class TestStore(unittest.TestCase):
    def setUp(self):
        self.store = Store('test.db')
