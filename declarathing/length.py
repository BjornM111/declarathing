from . import reducer


class LengthReducer(reducer.Reducer):
    def __init__(self):
        super().__init__(0)

    def _create(self, key, item):
        self.data += 1
        self.on_update()

    def _update(self, key, item):
        pass

    def _delete(self, key, item):
        self.data -= 1
        self.on_update()
