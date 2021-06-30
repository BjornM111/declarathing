from declarathing.sum import SumReducer


def test_subtract():
    s = SumReducer(lambda value: value)

    data = []
    s.listen(lambda item: data.append(("updated", item)))

    s._create(1, (2,))
    assert data.pop() == ("updated", 2)

    s._update(1, (3,))
    assert data.pop() == ("updated", 3)

    s._create(2, (1,))
    assert data.pop() == ("updated", 4)

    s._delete(2, (0,))
    assert data.pop() == ("updated", 3)

    s._delete(1, (0,))
    assert data.pop() == ("updated", 0)
