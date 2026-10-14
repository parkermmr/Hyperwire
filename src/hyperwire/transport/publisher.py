"""
Publisher delivers messages to every listener on its topics.
"""

from hyperwire.graph.node import Node
from hyperwire.models.message import Message
from hyperwire.transport.broker import Broker


class Publisher(Node):
    """
    Sits at the end of a stream and hands each message to every
    listener subscribed to its topics. Messages also pass through to
    direct subscribers, so a publisher can sit mid chain.
    """

    def __init__(
        self,
        topics: list[str],
        name: str = "publisher",
        workers: int = 1,
        broker: Broker | None = None,
    ) -> None:
        super().__init__(name=name, workers=workers)
        self.topics = list(topics)
        self.broker = broker if broker is not None else Broker.default()

    def linked(self) -> list[Node]:
        return self.broker.listeners(self.topics)

    async def handle(self, message: Message) -> Message | None:
        recipients = self.broker.listeners(self.topics)
        passthrough = bool(self.subscriptions)
        total = len(recipients) + (1 if passthrough else 0)
        for node in recipients:
            outgoing = message.copy(deep=not self.turbo) if total > 1 else message
            await node.inbox.put(outgoing)
        return message if passthrough else None
