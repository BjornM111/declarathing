from declarathing.stream import Stream


def test_observers():
    s = Stream()
    data = []
    s.listen(
        lambda item: data.append(("created", item)),
        lambda item: data.append(("updated", item)),
        lambda item: data.append(("deleted", item)),
    )
    s.on_create((1,), (1,))
    s.on_update((1,), (1,))
    s.on_update((2,), (2,))
    s.on_delete((3,), (3,))

    assert data == [
        ("created", 1),
        ("updated", 1),
        ("updated", 2),
        ("deleted", 3),
    ]
