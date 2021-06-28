import gevent
import gevent.event

from . import reducer


class MinInterval(reducer.Reducer):
    def __init__(self, interval):
        super().__init__(None)
        def task():
            emitted_data = None
            while True:
                self.updated.wait()
                gevent.sleep(0.05)
                self.updated.clear()
                if emitted_data != self.data:
                    emitted_data = self.data
                    self.on_update()
                    gevent.sleep(interval)

        self.greenlet = gevent.spawn(task)
        self.interval = interval
        self.updated = gevent.event.Event()

    def _update(self, data):
        self.data = data
        self.updated.set()

    def unlock(self):
        self.locked = False
        self.updated.set()
