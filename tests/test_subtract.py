from declarathing.collection import CollectionStream
from declarathing.subtract import SubtractStream, SubtractStream2


def test_reducer():
    c1 = CollectionStream()
    c2 = CollectionStream()
    s = SubtractStream2()
    c1.add_observer(s.a)
    c2.add_observer(s.b)
    data = []
    s.listen(
        lambda item: data.append(("created", item)),
        lambda item: data.append(("updated", item)),
        lambda item: data.append(("deleted", item)),
    )

    with c1:
        with c2:
            c1.replace({1:1, 2:2})
            c2.replace({1:1})
    assert data.pop() == ("created", 2)
    assert data == []

    c2.replace({})
    assert data.pop() == ("created", 1)

    c1.remove(1, 1)
    assert data.pop() == ("deleted", 1)

    with c1:
        with c2:
            c1.replace({2:2})
            c2.replace({3:3})
    assert data == []
