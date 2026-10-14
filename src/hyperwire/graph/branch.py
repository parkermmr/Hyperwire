"""
Branch is one arm of a fan out list, optionally guarded by a
predicate and optionally terminated.
"""

from typing import Any

from hyperwire.graph.node import Predicate
from hyperwire.graph.terminate import Terminate
from hyperwire.graph.wire import Wire


class Branch:
    """
    A predicate plus the wire it routes into.

    A predicate of None makes the branch unconditional. A fallback
    branch receives only when no predicated branch matched. A
    terminated branch is excluded from convergence.
    """

    def __init__(
        self,
        predicate: Predicate | None = None,
        wire: Wire | None = None,
        terminated: bool = False,
        fallback: bool = False,
    ) -> None:
        self.predicate = predicate
        self.wire = wire
        self.terminated = terminated
        self.fallback = fallback

    @classmethod
    def of(cls, item: Any) -> "Branch":
        """
        Coerce a fan out list element into a branch.
        """
        if isinstance(item, Branch):
            if item.wire is None and not item.terminated:
                raise ValueError("Branch has no destination")
            return item
        return cls(wire=Wire.of(item))

    def __rshift__(self, other: Any) -> "Branch":
        if isinstance(other, Terminate):
            self.terminated = True
            return self
        if self.wire is None:
            self.wire = Wire.of(other)
        else:
            self.wire = self.wire >> other
        return self
