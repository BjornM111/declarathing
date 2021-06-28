from declarathing.length import LengthReducer


def test_reducer():
    r = LengthReducer()
    data = []
    r.listen(lambda value: data.append(value))
    r._create(1, 1)
    r._delete(2, 1)
    assert data == [1, 0]
