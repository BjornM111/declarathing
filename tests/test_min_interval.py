import gevent

from declarathing.min_interval import MinInterval


def test_reducer():
    r = MinInterval(0.1)
    data = []
    r.listen(lambda value: data.append(value))

    r._update(1)
    gevent.sleep(0.051)
    assert data.pop() == 1

    r._update(2)
    gevent.sleep(0.051)
    assert data == []
    gevent.sleep(0.11)
    assert data.pop() == 2

    r._update(3)
    gevent.sleep(0.051)
    assert data == []
    gevent.sleep(0.1)
    assert data.pop() == 3

    assert data == []
