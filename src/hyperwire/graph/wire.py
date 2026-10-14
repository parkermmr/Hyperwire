"""
Wire tracks the heads and tails of a partially built graph so the
>> operator can extend chains, fan out over lists, and converge.
"""

from typing import Any

from hyperwire.graph.node import Node
from hyperwire.graph.terminate import Terminate


class Wire:
    """
    A build time handle on a section of graph.

    Heads are the entry nodes of the section. Tails are the nodes the
    next >> target will be attached to.
    """

    def __init__(self, heads: list[Node], tails: list[Node]) -> None:
        self.heads = heads
        self.tails = tails

    @classmethod
    def of(cls, item: Any) -> "Wire":
        """
        Coerce a node, wire, or component into a wire.
        """
        from hyperwire.graph.component import Component

        if isinstance(item, Wire):
            return item
        if isinstance(item, Node):
            return cls(heads=[item], tails=[item])
        if isinstance(item, Component):
            return item.wire
        raise TypeError(f"Cannot wire {type(item).__name__} into a pipeline")

    def __rshift__(self, other: Any):
        from hyperwire.graph.branch import Branch

        if isinstance(other, Terminate):
            return Branch(wire=self, terminated=True)
        if isinstance(other, list):
            return self.fan(other)
        target = Wire.of(other)
        for tail in self.tails:
            for head in target.heads:
                tail.subscribe(head)
        return Wire(heads=self.heads, tails=target.tails)

    def fan(self, items: list) -> "Wire":
        """
        Subscribe every tail to each branch in the list and return a
        wire whose tails are the non terminated branch tails.
        """
        from hyperwire.graph.branch import Branch

        branches = [Branch.of(item) for item in items]
        for tail in self.tails:
            for branch in branches:
                if branch.wire is None:
                    continue
                for head in branch.wire.heads:
                    tail.subscribe(head, branch.predicate, branch.fallback)
        tails = [
            tail
            for branch in branches
            if not branch.terminated and branch.wire is not None
            for tail in branch.wire.tails
        ]
        return Wire(heads=self.heads, tails=tails)
