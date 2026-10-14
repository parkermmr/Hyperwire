"""
Reservoir holds the messages routed into it until they are released.

It is a queue you can stand in front of: park the output of a stage
while something downstream is being fixed, look at what piled up,
then let it through, a few at a time or all at once, or throw it away.
Held messages live in memory only, bounded by a limit.
"""

import asyncio
from collections import deque

from hyperwire.graph.node import Node
from hyperwire.models.message import Message

BLOCK = "block"
DROP = "drop"


class Reservoir(Node):
    """
    A node that keeps what it receives instead of passing it on.

    While holding, every message is kept in arrival order. When the
    reservoir is full, block makes the sender wait (the pipeline's
    usual backpressure, all the way to the source) and drop discards
    the oldest held message to make room. Opening the reservoir
    releases everything held and lets new messages flow straight
    through; closing it holds again.

    A reservoir with messages in it is never idle, so a run does not
    finish, and its sources do not commit, while anything is held.
    Stopping the run is how held messages are abandoned on purpose.
    """

    def __init__(
        self,
        name: str = "",
        limit: int = 10_000,
        overflow: str = BLOCK,
        holding: bool = True,
    ) -> None:
        if overflow not in (BLOCK, DROP):
            raise ValueError(f"overflow must be {BLOCK!r} or {DROP!r}, not {overflow!r}")
        super().__init__(name=name)
        self.limit = limit
        self.overflow = overflow
        self.holding = holding
        self.held: deque[Message] = deque()
        self.released = 0
        self.dropped = 0
        self.room = asyncio.Event()
        self.room.set()

    async def handle(self, message: Message) -> Message | None:
        """
        Keep the message while holding, pass it on otherwise.
        """
        if not self.holding:
            return message
        while len(self.held) >= self.limit:
            if self.overflow == DROP:
                self.held.popleft()
                self.dropped += 1
                continue
            self.room.clear()
            await self.room.wait()
            if not self.holding:
                return message
        self.held.append(message)
        return None

    def vacate(self) -> None:
        """Wake a sender waiting for room once there is some."""
        if len(self.held) < self.limit:
            self.room.set()

    async def release(self, count: int | None = None) -> int:
        """
        Send up to count held messages on, oldest first, or all of
        them. Returns how many went.
        """
        sent = 0
        while self.held and (count is None or sent < count):
            await self.emit(self.held.popleft())
            sent += 1
        self.released += sent
        self.vacate()
        return sent

    def drop(self, count: int | None = None) -> int:
        """
        Discard up to count held messages, oldest first, or all of
        them. Returns how many went.
        """
        gone = 0
        while self.held and (count is None or gone < count):
            self.held.popleft()
            gone += 1
        self.dropped += gone
        self.vacate()
        return gone

    async def open(self) -> int:
        """
        Stop holding: release everything held and pass new messages
        straight through. Returns how many were released.
        """
        self.holding = False
        self.room.set()
        return await self.release()

    def close(self) -> None:
        """Hold again from the next message."""
        self.holding = True

    def peek(self, limit: int = 50) -> list[Message]:
        """The oldest held messages, the next to be released first."""
        return list(self.held)[:limit]

    def idle(self) -> bool:
        return super().idle() and not self.held

    def describe(self) -> dict:
        snapshot = super().describe()
        snapshot["reservoir"] = {
            "held": len(self.held),
            "holding": self.holding,
            "limit": self.limit,
            "overflow": self.overflow,
            "released": self.released,
            "dropped": self.dropped,
        }
        return snapshot
