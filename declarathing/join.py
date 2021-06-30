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


class JoinStream(stream.Stream):
    def __init__(self, left_on=None, right_on=None):
        super().__init__()

        self.locked = 0
        self.items = {}
        self.left_on = left_on
        self.right_on = right_on

        self.left = Observer(
            lambda key, item: self._create_left(key, item),
            lambda key, item: self._update_left(key, item),
            lambda key, item: self._delete_left(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )
        self.right = Observer(
            lambda key, item: self._create_right(key, item),
            lambda key, item: self._update_right(key, item),
            lambda key, item: self._delete_right(key, item),
            lambda: self.lock(),
            lambda: self.unlock(),
        )

    def _create_left(self, left_key, left):
        if self.left_on:
            key = self.left_on(*left)
        else:
            key = left_key
        collection = self.items.get(key, None)
        if collection is None:
            self.items[key] = ({left_key: left}, {})
        else:
            collection[0][left_key] = left
            for right in collection[1].values():
                self.on_create(key, left+right)

    def _update_left(self, left_key, left):
        if self.left_on:
            key = self.left_on(*left)
        else:
            key = left_key
        collection = self.items[key]
        collection[0][left_key] = left
        for right in collection[1].values():
            self.on_update(key, left+right)

    def _delete_left(self, left_key, left):
        if self.left_on:
            key = self.left_on(*left)
        else:
            key = left_key
        collection = self.items[key]
        for right in collection[1].values():
            self.on_delete(key, left+right)
        del collection[0][left_key]

    def _create_right(self, right_key, right):
        if self.right_on:
            key = self.right_on(*right)
        else:
            key = right_key
        collection = self.items.get(key, None)
        if collection is None:
            self.items[key] = ({}, {right_key: right})
        else:
            collection[1][right_key] = right
            for left in collection[0].values():
                self.on_create(key, left+right)

    def _update_right(self, right_key, right):
        if self.right_on:
            key = self.right_on(*right)
        else:
            key = right_key
        collection = self.items[key]
        collection[1][right_key] = right
        for left in collection[0].values():
            self.on_update(key, left+right)

    def _delete_right(self, right_key, right):
        if self.right_on:
            key = self.right_on(*right)
        else:
            key = right_key
        collection = self.items[key]
        for left in collection[0].values():
            self.on_delete(key, left+right)
        del collection[1][right_key]

    def lock(self):
        if not self.locked:
            super().lock()
        self.locked += 1

    def unlock(self):
        self.locked -= 1
        if not self.locked:
            super().unlock()

