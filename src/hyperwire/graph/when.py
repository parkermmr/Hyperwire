"""
when builds a conditional branch for fan out lists.
"""

from typing import Any

from hyperwire.graph.branch import Branch
from hyperwire.graph.node import Predicate
from hyperwire.models.message import Message


def when(predicate: Predicate | None = None, **checks: Any) -> Branch:
    """
    Route messages whose predicate returns True, or whose metadata
    matches every keyword check.
    """
    if predicate is None and checks:
        fields = dict(checks)

        def matcher(message: Message) -> bool:
            return all(message.metadata.get(key) == value for key, value in fields.items())

        matcher.label = ", ".join(f"{key}={value!r}" for key, value in fields.items())
        predicate = matcher
    if predicate is None:
        raise ValueError("when() needs a predicate or keyword checks")
    return Branch(predicate=predicate)
