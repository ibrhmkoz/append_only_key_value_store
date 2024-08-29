from dataclasses import dataclass


@dataclass
class Pair:
    key: str
    value: str


class Store:
    def __init__(self, file_name):
        self.file_name = file_name

    def put(self, key, value):
        with open(self.file_name, 'a') as file:
            offset = file.tell()
            file.write(f'{key}:{str(value)}\n')
        return offset

    def bulk_put(self, pairs):
        offsets = []
        with open(self.file_name, 'a') as file:
            for pair in pairs:
                offset = file.tell()
                file.write(f'{pair.key}:{str(pair.value)}\n')
                offsets.append((pair.key, offset))
        return offsets

    def get(self, key):
        with open(self.file_name, 'r') as file:
            for line in file:
                if line.startswith(key):
                    return line.split(':')[1].strip()
        return None

    def get_at_offset(self, offset: int):
        with open(self.file_name, 'r') as file:
            file.seek(offset)
            line = file.readline().strip()
            key, value = line.split(':', 1)
            return key, value


class IndexedStore:
    def __init__(self, store: Store):
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
        with open(self.store.file_name, 'r') as file:
            while True:
                offset = file.tell()
                line = file.readline()
                if not line:
                    break
                key = line.split(':', 1)[0]
                self._index[key] = offset


class BufferedStore:
    def __init__(self, store: Store, size: int):
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


class CachedStore:
    def __init__(self, store: Store, cache_size: int):
        self.store = store
        self.cache_size = cache_size
        self.cache = {}

    def get(self, key: str):
        if key in self.cache:
            return self.cache[key]
        value = self.store.get(key)
        if value is not None:
            self.cache[key] = value
        return value

    def put(self, key, value):
        self.store.put(key, value)
        self.cache[key] = value
        if len(self.cache) > self.cache_size:
            self.cache.popitem()
