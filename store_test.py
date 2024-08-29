import unittest

from store import Store, IndexedStore, BufferedStore, CachedStore, Pair


class TestStore(unittest.TestCase):
    def setUp(self):
        self.store = CachedStore(BufferedStore(IndexedStore(Store('test_log.txt')), 10), 5)

    def test_put_and_get(self):
        pairs = [Pair(f'key{i}', f'value{i}') for i in range(15)]

        for pair in pairs:
            self.store.put(pair.key, pair.value)

        for pair in pairs:
            self.assertEqual(self.store.get(pair.key), pair.value)

    def tearDown(self):
        import os
        os.remove('test_log.txt')
