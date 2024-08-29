import unittest

from store import AppendOnlyLogStore, IndexedStore, BufferedStore, CachedStore, Pair, EventBus, CacheHitEvent, \
    CacheMissEvent, BufferFlushEvent


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


class EventCounter:
    def __init__(self):
        self.count = 0

    def __call__(self, event):
        self.count += 1


class TestCachedStore(unittest.TestCase):
    def setUp(self):
        self.cache_hits = EventCounter()
        self.cache_misses = EventCounter()

        event_bus = EventBus()
        event_bus.subscribe(CacheHitEvent, self.cache_hits)
        event_bus.subscribe(CacheMissEvent, self.cache_misses)

        self.store = CachedStore(AppendOnlyLogStore('test_log.txt'), 5, event_bus)
        pairs = [Pair(f'key{i}', f'value{i}') for i in range(150)]
        for pair in pairs:
            self.store.put(pair.key, pair.value)

    def test_put_and_get(self):
        # Cache misses
        self.store.get("key6")
        self.store.get("key7")
        self.store.get("key9")
        self.store.get("key13")
        self.store.get("key14")

        # Cache hits
        self.store.get("key14")
        self.store.get("key7")

        # Assert
        self.assertEqual(self.cache_misses.count, 5)
        self.assertEqual(self.cache_hits.count, 2)

    def tearDown(self):
        import os
        os.remove('test_log.txt')


class TestBufferedStore(unittest.TestCase):
    def setUp(self):
        self.flushes = EventCounter()

        self.event_bus = EventBus()
        self.event_bus.subscribe(BufferFlushEvent, self.flushes)

        self.store = BufferedStore(AppendOnlyLogStore('test_log.txt'), 3, self.event_bus)

    def test_buffer_flush(self):
        # Add items to buffer
        self.store.put('key1', 'value1')
        self.store.put('key2', 'value2')
        self.assertEqual(self.flushes.count, 0)  # No flush yet

        # Trigger flush
        self.store.put('key3', 'value3')
        self.assertEqual(self.flushes.count, 1)  # Flush occurred

        # Add one more item
        self.store.put('key4', 'value4')
        self.assertEqual(self.flushes.count, 1)  # No new flush

    def tearDown(self):
        import os
        os.remove('test_log.txt')
