import os
from collections import OrderedDict
from dataclasses import dataclass

from event_bus import NoOpEventBus


@dataclass
class Pair:
    key: str
    value: str


class Store:
    def put(self, key, value):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError


class BulkStore(Store):
    def bulk_put(self, pairs):
        raise NotImplementedError


class AppendOnlyLogStore:
    def __init__(self, log_path):
        self.log_path = log_path
        if not os.path.exists(log_path):
            with open(log_path, 'w'):
                pass

    def put(self, key, value):
        with open(self.log_path, 'a') as file:
            offset = file.tell()
            file.write(f'{key}:{str(value)}\n')
        return offset

    def bulk_put(self, pairs):
        offsets = []
        with open(self.log_path, 'a') as file:
            for pair in pairs:
                offset = file.tell()
                file.write(f'{pair.key}:{str(pair.value)}\n')
                offsets.append((pair.key, offset))
        return offsets

    def get(self, key):
        with open(self.log_path, 'r') as file:
            for line in file:
                if line.startswith(key):
                    return line.split(':')[1].strip()
        return None

    def get_at_offset(self, offset: int):
        with open(self.log_path, 'r') as file:
            file.seek(offset)
            line = file.readline().strip()
            key, value = line.split(':', 1)
            return key, value


class IndexedStore:
    def __init__(self, store):
        self.store = store
        self._index = {}
        self.index()

    def put(self, key, value):
        offset = self.store.put(key, value)
        self._index[key] = offset

    def bulk_put(self, pairs):
        offsets = self.store.bulk_put(pairs)
        self._index.update(offsets)

    def get(self, key):
        if key in self._index:
            _, value = self.store.get_at_offset(self._index[key])
            return value
        return None

    def index(self):
        self._index.clear()
        with open(self.store.log_path, 'r') as file:
            while True:
                offset = file.tell()
                line = file.readline()
                if not line:
                    break
                key = line.split(':', 1)[0]
                self._index[key] = offset


@dataclass
class BufferFlushed:
    type = "buffer_flushed"


class BufferedStore:
    def __init__(self, bulk_store, size, bus=NoOpEventBus):
        self.bulk_store = bulk_store
        self.size = size
        self.buffer = {}
        self.bus = bus

    def put(self, key, value):
        self.buffer[key] = value
        if len(self.buffer) == self.size:
            self.flush()

    def get(self, key):
        if key in self.buffer:
            return self.buffer[key]
        return self.bulk_store.get(key)

    def flush(self):
        pairs = [Pair(k, v) for k, v in self.buffer.items()]
        self.bulk_store.bulk_put(pairs)
        self.bus.emit(BufferFlushed())
        self.buffer.clear()


class CacheHit:
    type = "cache_hit"

    def __init__(self, key):
        self.key = key


class CacheMissed:
    type = "cache_missed"

    def __init__(self, key):
        self.key = key


class CachedStore:
    def __init__(self, store, cache_size, bus=NoOpEventBus):
        self.store = store
        self.cache_size = cache_size
        self.cache = OrderedDict()
        self.bus = bus

    def get(self, key):
        if key in self.cache:
            # Move the accessed item to the end (most recently used)
            self.cache.move_to_end(key)
            self.bus.emit(CacheHit(key))
            return self.cache[key]

        self.bus.emit(CacheMissed(key))

        value = self.store.get(key)
        if value is not None:
            self._add_to_cache(key, value)
        return value

    def put(self, key, value):
        self.store.put(key, value)
        self._add_to_cache(key, value)

    def _add_to_cache(self, key, value):
        if key in self.cache:
            # If key already exists, move it to the end (most recently used)
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.cache_size:
            # If cache is full, remove the least recently used item
            self.cache.popitem(last=False)
        self.cache[key] = value
