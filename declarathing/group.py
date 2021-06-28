from dataclasses import dataclass
from typing import Any

from . import stream


class ItemsGroup(object):
    def __init__(self, key):
        self.key = key
        self.items = {}

    def add(self, key, item):
        self.items[key] = item

    def remove(self, key):
        del self.items[key]


class GroupStream(stream.Stream):
    def __init__(self, func, Group):
        super().__init__()
        self._func = func
        self.groups = {}
        self.item_to_group = {}
        self.Group = Group

    def _create(self, item_key, item):
        group_key = self._func(item)
        self.item_to_group[item_key] = group_key
        group = self.groups.get(group_key)
        if group:
            group.items[item_key] = item
            self.on_update(group_key, group)
        else:
            group = self.Group(group_key)
            group.add(item_key, item)
            self.groups[group_key] = group
            self.on_create(group_key, group)

    def _update(self, item_key, item):
        old_key = self.item_to_group.get(item_key)
        new_key = self._func(item)
        if old_key == new_key:
            self.on_update(new_key, self.groups[new_key])
        else:
            old_group = self.groups[old_key]
            old_group.remove(item_key)
            if not old_group.items:
                del self.groups[old_key]
                self.on_delete(old_key, old_group)
            else:
                self.on_update(old_key, old_group)

            new_group = self.groups.get(new_key)
            if new_group:
                new_group.add(item_key, item)
                self.on_update(new_key, new_group)
            else:
                new_group = self.Group(new_key)
                new_group.add(item_key, item)
                self.groups[new_key] = new_group
                self.on_create(new_key, new_group)

    def _delete(self, item_key, item):
        group_key = self.item_to_group.get(item_key)
        group = self.groups[group_key]
        group.remove(item_key)
        if not group.items:
            del self.groups[group_key]
            self.on_delete(group_key, group)
        else:
            self.on_update(group_key, group)
