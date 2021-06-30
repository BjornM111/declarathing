from dataclasses import dataclass

from declarathing.collection import CollectionStream
from declarathing.join import JoinStream


@dataclass
class Job:
    name: str


@dataclass
class Task:
    name: str
    job: str


def test_on():
    s = JoinStream(
        left_on=lambda job: job.name,
        right_on=lambda task: task.job,
    )
    data = []
    s.listen(
        lambda job, task: data.append(("created", job, task)),
        lambda job, task: data.append(("updated", job, task)),
        lambda job, task: data.append(("deleted", job, task)),
    )

    s._create_left((1,), (Job("a"),))
    assert data == []

    s._create_right((1,), (Task("a-1", "a"),))
    assert data.pop() == ("created", Job("a"), Task("a-1", "a"))

    s._create_right((2,), (Task("a-2", "a"),))
    s._create_right((3,), (Task("a-3", "a"),))
    assert data.pop() == ("created", Job("a"), Task("a-3", "a"))
    assert data.pop() == ("created", Job("a"), Task("a-2", "a"))

    s._update_right((2,), (Task("a-2", "a"),))
    assert data.pop() == ("updated", Job("a"), Task("a-2", "a"))

    s._delete_right((2,), (Task("a-2", "a"),))
    assert data.pop() == ("deleted", Job("a"), Task("a-2", "a"))

    s._delete_left((1,), (Job("a"),))
    assert data.pop() == ("deleted", Job("a"), Task("a-3", "a"))
    assert data.pop() == ("deleted", Job("a"), Task("a-1", "a"))

    assert data == []
