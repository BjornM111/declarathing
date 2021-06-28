from . import reducer, stream


class CollectionReducer(reducer.Reducer):

    def __init__(self):
        super().__init__({})

    def _create(self, key, item):
        self.data[key] = item
        self.on_update()

    def _update(self, key, item):
        self.data[key] = item
        self.on_update()

    def _delete(self, key, item):
        del self.data[key]
        self.on_update()

    def stream(self):
        return self.add_observer(CollectionStream())


class CollectionStream(stream.Stream):

    def __init__(self, equals=None):
        super().__init__()

        if equals is None:
            equals = lambda a, b: False

        self.data = {}
        self.equals = equals

    def _update(self, items):
        self.replace(items)

    def replace(self, items):
        for new_key, new_item in items.items():
            old_item = self.data.pop(new_key, None)
            if old_item:
                if not self.equals(old_item, new_item):
                    self.on_update(new_key, new_item)
            else:
                self.on_create(new_key, new_item)
        for key, item in self.data.items():
            self.on_delete(key, item)
        self.data = items.copy()

    def add(self, key, item):
        old_item = self.data.get(key)
        self.data[key] = item
        if old_item:
            if not self.equals(old_item, item):
                self.on_update(key, item)
        else:
            self.on_create(key, item)

    def remove(self, key, _):
        old_item = self.data.pop(key, None)
        if old_item:
            self.on_delete(key, old_item)
