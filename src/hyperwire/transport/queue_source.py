"""
QueueSource is an in process source fed by push.
"""

import asyncio
from typing import AsyncIterator

from hyperwire.models.message import Message
from hyperwire.transport.source import Source


class QueueSource(Source):
    """
    Buffers pushed messages and replays them as events. Close the
    source to let the pipeline finish.

    Delivered messages are retained until commit, so a run that
    errors out can replay its uncommitted backlog into the queue and
    try again.
    """

    def __init__(self, name: str = "source") -> None:
        super().__init__(name=name)
        self.queue: asyncio.Queue = asyncio.Queue()
        self.retained: list[Message] = []

    async def push(self, message: Message) -> None:
        await self.queue.put(message)

    async def close(self) -> None:
        await self.queue.put(None)

    def stop_pumping(self) -> None:
        """
        Stop requests also close the queue, so a run blocked waiting
        for the next message wakes up and ends.
        """
        super().stop_pumping()
        self.queue.put_nowait(None)

    async def events(self) -> AsyncIterator[Message]:
        while True:
            message = await self.queue.get()
            if message is None:
                return
            self.retained.append(message)
            yield message

    def commit(self) -> int:
        self.retained.clear()
        return super().commit()

    def replay(self) -> int:
        """
        Queue every uncommitted message again after a failed run and
        return how many were requeued.
        """
        count = len(self.retained)
        for message in self.retained:
            self.queue.put_nowait(message)
        self.retained.clear()
        self.delivered -= count
        return count
