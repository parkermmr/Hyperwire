"""
MemorySink collects delivered messages in memory.
"""

from hyperwire.models.message import Message
from hyperwire.transport.sink import Sink


class MemorySink(Sink):
    """
    Stores every delivered message on a list, useful for local runs
    and tests.
    """

    def __init__(self, name: str = "sink") -> None:
        super().__init__(name=name)
        self.messages: list[Message] = []

    async def deliver(self, message: Message) -> None:
        self.messages.append(message)
