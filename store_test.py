import unittest

from store import Store, IndexedStore, BufferedStore, CachedStore, Pair, EventBus, CacheHitEvent


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


class TestCachedStore(unittest.TestCase):
    def setUp(self):
        class CountCacheHits:
            def __init__(self):
                self.count = 0

            def __call__(self, event):
                if isinstance(event, CacheHitEvent):
                    self.count += 1

        self.cache_hits = CountCacheHits()

        event_bus = EventBus()
        event_bus.subscribe(CacheHitEvent, self.cache_hits)
        self.store = CachedStore(Store('test_log.txt'), 5, event_bus)
        pairs = [Pair(f'key{i}', f'value{i}') for i in range(15)]
        for pair in pairs:
            self.store.put(pair.key, pair.value)

    def test_put_and_get(self):
        # Cache misses
        self.store.get("key6")
        self.store.get("key7")

        # Cache hits
        self.store.get("key1")
        self.store.get("key2")

        # Assert
        self.assertEqual(self.cache_hits.count, 2)
