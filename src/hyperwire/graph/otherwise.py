"""
otherwise is the catch all branch for fan out lists: it fires only
when no when branch matched.
"""

from typing import Any

from hyperwire.graph.branch import Branch


class Otherwise:
    """
    Sentinel used as: otherwise >> node.
    """

    def __rshift__(self, other: Any) -> Branch:
        return Branch(fallback=True) >> other


otherwise = Otherwise()
