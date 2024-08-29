class EventBus:
    def subscribe(self, event_class, listener):
        raise NotImplementedError

    def emit(self, event):
        raise NotImplementedError


class InMemoryEventBus(EventBus):
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


class _NoOpEventBus(EventBus):
    def subscribe(self, event_class, listener):
        pass

    def emit(self, event):
        pass


NoOpEventBus = _NoOpEventBus()
