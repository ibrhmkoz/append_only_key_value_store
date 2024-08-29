import unittest

from store import AppendOnlyLogStore, IndexedStore, BufferedStore, CachedStore, Pair, EventBus, CacheHitEvent, \
    CacheMissEvent


class TestStore(unittest.TestCase):
    def setUp(self):
        self.store = CachedStore(BufferedStore(IndexedStore(AppendOnlyLogStore('test_log.txt')), 10), 5)

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
        class EventCounter:
            def __init__(self):
                self.count = 0

            def __call__(self, event):
                self.count += 1

        self.cache_hits = EventCounter()
        self.cache_misses = EventCounter()

        event_bus = EventBus()
        event_bus.subscribe(CacheHitEvent, self.cache_hits)
        event_bus.subscribe(CacheMissEvent, self.cache_misses)

        self.store = CachedStore(AppendOnlyLogStore('test_log.txt'), 5, event_bus)
        pairs = [Pair(f'key{i}', f'value{i}') for i in range(15)]
        for pair in pairs:
            self.store.put(pair.key, pair.value)

    def test_put_and_get(self):
        # Cache misses
        self.store.get("key6")
        self.store.get("key7")
        self.store.get("key9")

        # Cache hits
        self.store.get("key13")
        self.store.get("key14")

        # Assert
        self.assertEqual(self.cache_misses.count, 3)
        self.assertEqual(self.cache_hits.count, 2)

    def tearDown(self):
        import os
        os.remove('test_log.txt')
