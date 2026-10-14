"""
Message is the unit of data that flows through a pipeline.
"""

from copy import copy, deepcopy
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4


class Message:
    """
    Envelope carrying content and metadata between nodes.
    """

    __slots__ = ("id", "content", "metadata", "timestamp")

    def __init__(
        self,
        content: Any = None,
        metadata: dict | None = None,
        id: str | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        self.id = id or uuid4().hex
        self.content = content
        self.metadata = metadata if metadata is not None else {}
        self.timestamp = timestamp or datetime.now(timezone.utc)

    def copy(self, deep: bool = True) -> "Message":
        """
        Return an independent copy so parallel branches cannot
        interfere. A shallow copy is faster but shares nested
        containers, which is the tradeoff performance mode makes.
        """
        duplicate = deepcopy if deep else copy
        return Message(
            content=duplicate(self.content),
            metadata=duplicate(self.metadata),
            id=self.id,
            timestamp=self.timestamp,
        )

    def __repr__(self) -> str:
        return f"Message(id={self.id!r}, content={self.content!r}, metadata={self.metadata!r})"
