from declarathing.collection import CollectionReducer, CollectionStream


def test_reducer():
    r = CollectionReducer()
    data = []
    r.listen(lambda value: data.append(value))

    r._create((1,), (1,))
    assert data.pop() == {(1,):(1,)}

    r._create((2,), (2,))
    assert data.pop() == {(1,):(1,), (2,):(2,)}

    r._update((1,), (3,))
    assert data.pop() == {(1,):(3,), (2,):(2,)}

    r._delete((2,), (2,))
    assert data.pop() == {(1,):(3,)}


def test_stream():
    s = CollectionStream(
        equals=lambda a, b: a==b,
    )
    data = []
    s.listen(
        lambda item: data.append(("created", item)),
        lambda item: data.append(("updated", item)),
        lambda item: data.append(("deleted", item)),
    )

    s.replace({(1,):(1,)})
    assert data.pop() == ("created", 1)

    s.replace({(1,):(1,)})
    s.add((1,), (1,))
    assert data == []

    s.replace({(1,):(1,), (2,):(2,)})
    assert data.pop() == ("created", 2)

    s.replace({(1,):(1,)})
    assert data.pop() == ("deleted", 2)

    s.add((2,), (2,))
    assert data.pop() == ("created", 2)

    s.remove((2,), (2,))
    assert data.pop() == ("deleted", 2)

    assert data == []
