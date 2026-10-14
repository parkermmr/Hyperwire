"""
Component groups chained nodes into one reusable, connectable unit.
"""

from typing import Any

from hyperwire.graph.node import Node
from hyperwire.graph.wire import Wire


class Component:
    """
    A named section of graph with no source or sink of its own.

    Build one with the component factory, then connect it like a
    node: with >>, inside fan out lists, or with upstream and
    downstream. Scale adjusts the workers of every member node so a
    whole section can be widened in one call.

    Components nest: one built from another becomes its parent, and
    each member node remembers the innermost component it sits in, so
    a description can draw the sections as nested groups.
    """

    def __init__(
        self,
        wire: Wire,
        members: list[Node],
        name: str = "",
        parent: "Component | None" = None,
    ) -> None:
        self.wire = wire
        self.members = members
        self.name = name or "component"
        self.parent = parent

    def __rshift__(self, other: Any):
        return self.wire >> other

    def upstream(self, *items) -> "Component":
        """
        Wire each item's tails into this component's heads.
        """
        for item in items:
            Wire.of(item) >> self
        return self

    def downstream(self, *items) -> "Component":
        """
        Wire this component's tails into each item's heads.
        """
        for item in items:
            Wire(heads=self.wire.heads, tails=self.wire.tails) >> item
        return self

    def scale(self, workers: int) -> "Component":
        """
        Set the worker count of every member node. Call before the
        pipeline starts.
        """
        for node in self.members:
            node.workers = workers
        return self

    def lineage(self) -> list["Component"]:
        """
        This component and every component enclosing it, outermost
        first.
        """
        chain: list[Component] = []
        current: Component | None = self
        while current is not None:
            chain.insert(0, current)
            current = current.parent
        return chain

    def adopt(self, node: Node) -> None:
        """
        Claim a member node. A node already inside a component keeps
        it, and that component's outermost ancestor nests under this
        one, so building a component from components nests them.
        """
        if node.component is None:
            node.component = self
            return
        root = node.component.lineage()[0]
        if root is not self:
            root.parent = self

    def __repr__(self) -> str:
        return f"Component({self.name!r})"


def component(*items: Any, name: str = "") -> Component:
    """
    Chain items left to right into a component.
    """
    if not items:
        raise ValueError("component() needs at least one item")
    wire = Wire.of(items[0])
    for item in items[1:]:
        wire = wire >> item
    built = Component(wire=wire, members=span(wire), name=name)
    for node in built.members:
        built.adopt(node)
    return built


def span(wire: Wire) -> list[Node]:
    """
    Every node from the wire's heads through to its tails, fan out
    arms included, in the order they are reached.
    """
    ends = {id(node) for node in wire.tails}
    pending = list(wire.heads)
    found: list[Node] = []
    seen: set[int] = set()
    while pending:
        node = pending.pop(0)
        if id(node) in seen:
            continue
        seen.add(id(node))
        found.append(node)
        if id(node) not in ends:
            pending.extend(target for predicate, target, fallback in node.subscriptions)
    return found


def sections(nodes: list[Node]) -> list[Component]:
    """
    The components the given nodes sit in, each once, every parent
    listed before its children.
    """
    found: list[Component] = []
    for node in nodes:
        if node.component is None:
            continue
        for item in node.component.lineage():
            if item not in found:
                found.append(item)
    return found
