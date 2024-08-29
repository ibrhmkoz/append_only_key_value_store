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
            file.write(f'{key}:{value}\n')

    def bulk_put(self, pairs):
        with open(self.file_name, 'a') as file:
            for pair in pairs:
                file.write(f'{pair.key}:{pair.value}\n')

    def get(self, key):
        with open(self.file_name, 'r') as file:
            for line in file:
                if line.startswith(key):
                    return line.split(':')[1].strip()
        return None


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
