class Listener(object):

    def __init__(self, on_create, on_update, on_delete):
        self.on_create = on_create
        self.on_update = on_update
        self.on_delete = on_delete

    def _create(self, _, item):
        self.on_create(*item)

    def _update(self, _, item):
        self.on_update(*item)

    def _delete(self, _, item):
        self.on_delete(*item)

    def lock(self):
        pass

    def unlock(self):
        pass


class Stream(object):

    def __init__(self):
        self.observers = []

    def __enter__(self):
        self.lock()

    def __exit__(self, *args):
        self.unlock()

    def on_create(self, key, item):
        if not self.observers:
            raise NotImplementedError("no observers")
        for observer in self.observers:
            observer._create(key, item)

    def on_update(self, key, item):
        if not self.observers:
            raise NotImplementedError("no observers")
        for observer in self.observers:
            observer._update(key, item)

    def on_delete(self, key, item):
        if not self.observers:
            raise NotImplementedError("no observers")
        for observer in self.observers:
            observer._delete(key, item)

    def lock(self):
        for observer in self.observers:
            observer.lock()

    def unlock(self):
        for observer in self.observers:
            observer.unlock()

    def add_observer(self, observer):
        self.observers.append(observer)
        return observer

    def filter(self, func):
        from .filter import FilterStream
        return self.add_observer(FilterStream(func))

    def join(self, stream, left_on=None, right_on=None):
        from .join import JoinStream
        join = JoinStream(left_on, right_on)
        self.add_observer(join.left)
        stream.add_observer(join.right)
        return join

    def group_by(self, func, aggregator):
        from .group import GroupStream
        return self.add_observer(GroupStream(func, aggregator))

    def length(self):
        from .length import LengthReducer
        return self.add_observer(LengthReducer())

    def collect(self):
        from .collection import CollectionReducer
        return self.add_observer(CollectionReducer())

    def sum(self, func):
        from .sum import SumReducer
        return self.add_observer(SumReducer(func))

    def listen(self, on_create, on_update, on_delete):
        return self.add_observer(Listener(on_create, on_update, on_delete))
