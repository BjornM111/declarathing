from . import stream


class FilterStream(stream.Stream):
    def __init__(self, func):
        super().__init__()
        self._func = func
        self.ids = set()

    def _create(self, key, item):
        if self._func(*item):
            self.ids.add(key)
            self.on_create(key, item)

    def _update(self, key, item):
        if self._func(*item):
            if key in self.ids:
                self.on_update(key, item)
            else:
                self.ids.add(key)
                self.on_create(key, item)
        else:
            if key in self.ids:
                self.ids.remove(key)
                self.on_delete(key, item)

    def _delete(self, key, item):
        idx = key
        if idx in self.ids:
            self.ids.remove(idx)
            self.on_delete(key, item)
