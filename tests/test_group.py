from dataclasses import dataclass

from declarathing.group import GroupStream, ItemsGroup


@dataclass
class PublishedFile:
    project_name: str


def test_lock():
    s = GroupStream(
        lambda publish: publish.project_name,
        ItemsGroup,
    )
    data = []
    s.listen(
        lambda group: data.append(("created", group.key, len(group.items))),
        lambda group: data.append(("updated", group.key, len(group.items))),
        lambda group: data.append(("deleted", group.key)),
    )

    s._create((1,), (PublishedFile("first"),))
    assert data.pop() == ("created", "first", 1)

    s._create((2,), (PublishedFile("second"),))
    assert data.pop() == ("created", "second", 1)

    s._create((3,), (PublishedFile("first"),))
    assert data.pop() == ("updated", "first", 2)

    s._create((4,), (PublishedFile("third"),))
    assert data.pop() == ("created", "third", 1)

    s._delete((1,), (PublishedFile("first"),))
    assert data.pop() == ("updated", "first", 1)

    s._delete((3,), (PublishedFile("first"),))
    assert data.pop() == ("deleted", "first")
