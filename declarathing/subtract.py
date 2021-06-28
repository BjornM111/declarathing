from weakref import ref

from . import stream


class Observer(object):
    def __init__(self, _create, _update, _delete, _lock, _unlock):
        super().__init__()
        self._create = _create
        self._update = _update
        self._delete = _delete
        self.lock = _lock
        self.unlock = _unlock


class SubtractStream(stream.Stream):
    def __init__(self):
        super().__init__()

        self.locked = 0
        self.items = {}
        self.a_keys = set()
        self.b_keys = set()
        self.updated_keys = set()

        self.keys = set()
        self.a = Observer(
            lambda key, item: self._create_a(key, item),
            lambda key, item: self._update_a(key, item),
            lambda key, item: self._delete_a(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )
        self.b = Observer(
            lambda key, item: self._create_b(key, item),
            lambda key, item: self._update_b(key, item),
            lambda key, item: self._delete_b(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )

    def _create_a(self, key, item):
        if not self.locked:
            self.keys.add(key)
            self.on_create(key, item)
        else:
            self.items[key] = item
            self.a_keys.add(key)

    def _update_a(self, key, item):
        if not self.locked:
            if key in self.keys:
                self.on_update(key, item)
        else:
            self.items[key] = item
            self.updated_keys.add(key)

    def _delete_a(self, key, item):
        if not self.locked:
            if key in self.keys:
                self.keys.remove(key)
                self.on_delete(key, item)
            else:
                raise ValueError("a item deleted which wasn't in the set")
        else:
            self.items[key] = item
            self.a_keys.remove(key)

    def _create_b(self, key, item):
        if not self.locked:
            if key in self.keys:
                self.keys.remove(key)
                self.on_delete(key, item)
        else:
            self.items[key] = item
            self.b_keys.add(key)

    def _update_b(self, key, item):
        if not self.locked:
            if key in self.keys:
                raise ValueError("what?")
        else:
            self.items[key] = item
            self.updated_keys.add(key)

    def _delete_b(self, key, item):
        if not self.locked:
            if key not in self.keys:
                self.keys.add(key)
                self.on_create(key, item)
            else:
                raise ValueError("b item deleted which was in the set")
        else:
            self.items[key] = item
            self.b_keys.remove(key)

    def lock(self):
        if not self.locked:
            super().lock()
        self.locked += 1

    def unlock(self):
        self.locked -= 1
        if not self.locked:
            keys = self.a_keys - self.b_keys
            for key in keys - self.keys:
                self.on_create(key, self.items[key])
            for key in self.keys - keys:
                self.on_delete(key, self.items[key])
            for key in self.updated_keys - self.keys:
                self.on_update(key, self.items[key])
            self.keys = keys
            self.items = {}
            self.a_keys = set()
            self.b_keys = set()
            self.updated_keys = set()
            super().unlock()


class SubtractStream2(stream.Stream):
    def __init__(self):
        super().__init__()

        self.locked = 0
        self.a_keys = set()
        self.b_keys = set()

        self.items = {}
        self.updated_keys = set()

        self.a = Observer(
            lambda key, item: self._create_a(key, item),
            lambda key, item: self._update_a(key, item),
            lambda key, item: self._delete_a(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )
        self.b = Observer(
            lambda key, item: self._create_b(key, item),
            lambda key, item: self._update_b(key, item),
            lambda key, item: self._delete_b(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )

    def _create_a(self, key, item):
        self.a_keys.add(key)
        if not self.locked:
            if key not in self.b_keys:
                self.on_create(key, item)
        else:
            self.items[key] = item

    def _update_a(self, key, item):
        if not self.locked:
            if key not in self.b_keys:
                self.on_update(key, item)
        else:
            self.items[key] = item
            self.updated_keys.add(key)

    def _delete_a(self, key, item):
        self.a_keys.remove(key)
        if not self.locked:
            if key not in self.b_keys:
                self.on_delete(key, item)
        else:
            self.items[key] = item

    def _create_b(self, key, item):
        self.b_keys.add(key)
        if not self.locked:
            if key in self.a_keys:
                self.on_delete(key, item)
        else:
            self.items[key] = item

    def _update_b(self, key, item):
        pass

    def _delete_b(self, key, item):
        self.b_keys.remove(key)
        if not self.locked:
            if key in self.a_keys:
                self.on_create(key, item)
        else:
            self.items[key] = item

    def lock(self):
        if not self.locked:
            super().lock()
        self.locked += 1
        self.prev_a_keys = self.a_keys.copy()
        self.prev_b_keys = self.b_keys.copy()

    def unlock(self):
        self.locked -= 1
        if not self.locked:
            created_a_keys = self.a_keys - self.prev_a_keys
            for key in created_a_keys:
                self._create_a(key, self.items[key])
            created_b_keys = self.b_keys - self.prev_b_keys
