import os
from dataclasses import dataclass


@dataclass
class Pair:
    key: str
    value: str


class Store:
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


class BufferedStore:
    def __init__(self, store, size):
        self.store = store
        self.size = size
        self.buffer = {}

    def put(self, key, value):
        self.buffer[key] = value
        if len(self.buffer) == self.size:
            self.flush()

    def get(self, key: str):
        if key in self.buffer:
            return self.buffer[key]
        return self.store.get(key)

    def flush(self):
        pairs = [Pair(k, v) for k, v in self.buffer.items()]
        self.store.bulk_put(pairs)
        self.buffer.clear()


class CacheHitEvent:
    type = "cache_hit"

    def __init__(self, key):
        self.key = key


class CacheMissEvent:
    type = "cache_miss"

    def __init__(self, key):
        self.key = key


class CacheEvictionEvent:
    type = "cache_eviction"

    def __init__(self, key):
        self.key = key


class EventBus:
    def __init__(self):
        self.listeners = {}

    def subscribe(self, event_class, listener):
        if event_class.type not in self.listeners:
            self.listeners[event_class.type] = []
        self.listeners[event_class.type].append(listener)

    def emit(self, event):
        if event.type in self.listeners:
            for listener in self.listeners[event.type]:
                listener(event)


class CachedStore:
    def __init__(self, store, cache_size, bus=None):
        self.store = store
        self.cache_size = cache_size
        self.cache = {}
        self.bus = bus

    def get(self, key):
        if key in self.cache:
            if self.bus:
                self.bus.emit(CacheHitEvent(key))
            return self.cache[key]

        if self.bus:
            self.bus.emit(CacheMissEvent(key))

        value = self.store.get(key)
        if value is not None:
            self.cache[key] = value
        return value

    def put(self, key, value):
        self.store.put(key, value)
        if len(self.cache) >= self.cache_size and key not in self.cache:
            evicted_key, _ = self.cache.popitem()
            if self.bus:
                self.bus.emit(CacheEvictionEvent(evicted_key))
        self.cache[key] = value
