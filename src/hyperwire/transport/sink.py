"""
Sink is the exit node of a pipeline.
"""

import abc

from hyperwire.graph.node import Node
from hyperwire.models.message import Message


class Sink(Node, abc.ABC):
    """
    Delivers messages to an external system.
    """

    @abc.abstractmethod
    async def deliver(self, message: Message) -> None:
        """
        Send one message to the destination.
        """

    async def handle(self, message: Message) -> Message | None:
        await self.deliver(message)
        return message
