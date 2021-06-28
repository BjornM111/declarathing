class Listener(object):

    def __init__(self, on_update):
        self.on_update = on_update

    def _update(self, item):
        self.on_update(item)

    def lock(self):
        pass

    def unlock(self):
        pass



class Reducer(object):
    def __init__(self, default):
        self.observers = []
        self.data = default
        self.locked = False

    def init(self, data):
        self.data = data

    def add_observer(self, observer):
        self.observers.append(observer)
        return observer

    def on_update(self):
        if self.locked:
            return
        if not self.observers:
            raise NotImplementedError("no observers")
        for observer in self.observers:
            observer._update(self.data)

    def lock(self):
        self.locked = True
        for observer in self.observers:
            observer.lock()

    def unlock(self):
        self.locked = False
        self.on_update()
        for observer in self.observers:
            observer.unlock()

    def min_interval(self, interval):
        from .min_interval import MinInterval
        return self.add_observer(MinInterval(interval))

    def listen(self, on_update):
        return self.add_observer(Listener(on_update))
