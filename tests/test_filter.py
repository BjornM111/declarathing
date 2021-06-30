from dataclasses import dataclass

from declarathing.filter import FilterStream


@dataclass
class Task:
    progress: int


def test_filter():
    s = FilterStream(
        lambda task: task.progress > 50,
    )
    data = []
    s.listen(
        lambda task: data.append(("created", task)),
        lambda task: data.append(("updated", task)),
        lambda task: data.append(("deleted", task)),
    )

    s._create((1,), (Task(10),))
    assert data == []

    s._create((1,), (Task(70),))
    assert data.pop() == ("created", Task(70))
    assert data == []

    s._update((1,), (Task(70),))
    assert data.pop() == ("updated", Task(70))
    assert data == []

    s._delete((1,), (Task(70),))
    assert data.pop() == ("deleted", Task(70))
    assert data == []

    s._delete((2,), Task(10))
    assert data == []
