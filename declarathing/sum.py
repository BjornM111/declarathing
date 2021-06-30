from . import reducer


class SumReducer(reducer.Reducer):
    def __init__(self, func):
        super().__init__(0)
        self.values = {}
        self.func = func

    def _create(self, key, item):
        value = self.func(*item)
        self.values[key] = value
        self.data += value
        self.on_update()

    def _update(self, key, item):
        old_data = self.data

        old_value = self.values[key]
        new_value = self.func(*item)
        self.values[key] = new_value
        self.data -= old_value
        self.data += new_value

        if self.data != old_data:
            self.on_update()

    def _delete(self, key, item):
        value = self.values.pop(key)
        self.data -= value
        self.on_update()
