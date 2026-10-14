"""
Source is the entry node of a pipeline.
"""

import abc
import asyncio
from typing import AsyncIterator

from hyperwire.graph.node import Node
from hyperwire.models.message import Message


class Source(Node, abc.ABC):
    """
    Emits messages from an external system into the graph.

    Pumping respects the pause gate, so a paused source truly stops
    pulling events instead of letting them pile into the graph. The
    delivered and committed counters give sources offset semantics:
    the pipeline commits only after a clean drain, so an errored run
    never commits and the backlog stays claimable.
    """

    def __init__(self, name: str = "", workers: int = 1, capacity: int = 1024) -> None:
        super().__init__(name=name, workers=workers, capacity=capacity)
        self.stopping = False
        self.delivered = 0
        self.committed = 0
        self.iterator: AsyncIterator[Message] | None = None

    @abc.abstractmethod
    def events(self) -> AsyncIterator[Message]:
        """
        Yield every incoming message until the source is exhausted.
        """

    def stream(self) -> AsyncIterator[Message]:
        """
        The cached events iterator, shared by run and poll so the two
        never consume the same message twice.
        """
        if self.iterator is None:
            self.iterator = self.events().__aiter__()
        return self.iterator

    async def poll(self, count: int) -> list[Message]:
        """
        Pull up to count messages without emitting them, waiting for
        each one and returning early when the source closes.
        """
        stream = self.stream()
        collected: list[Message] = []
        for _ in range(count):
            try:
                message = await anext(stream)
            except StopAsyncIteration:
                break
            collected.append(message)
            self.delivered += 1
        return collected

    async def run(self) -> None:
        """
        Pump every event into the graph. The gate is awaited before
        each pull so pausing halts consumption, and a stop request
        ends pumping at the next message boundary.
        """
        stream = self.stream()
        while not self.stopping:
            await self.gate.wait()
            try:
                message = await anext(stream)
            except StopAsyncIteration:
                break
            await self.emit(message)
            self.delivered += 1
            await asyncio.sleep(0)

    def stop_pumping(self) -> None:
        """
        Ask run to end at the next message boundary.
        """
        self.stopping = True

    def commit(self) -> int:
        """
        Mark every delivered message consumed and return how many were
        newly committed. Sources backed by a broker override this to
        commit real offsets upstream.
        """
        fresh = self.delivered - self.committed
        self.committed = self.delivered
        return fresh

    def lag(self) -> int | None:
        """
        How many records are waiting upstream that this source has not
        yet pulled, or None when it cannot tell. Sources backed by a
        broker override this; a platform scales replicas on it.
        """
        return None

    def partitions(self) -> int | None:
        """
        How many partitions the upstream splits into, or None when it
        is not partitioned or cannot tell. More consumers than
        partitions leaves the extra ones idle.
        """
        return None

    def describe(self) -> dict:
        """
        The node snapshot, plus the backlog where the source knows it.
        """
        found = super().describe()
        for key, value in (("lag", self.lag()), ("partitions", self.partitions())):
            if value is not None:
                found[key] = value
        return found
