"""
Listener receives messages published to its topics.
"""

from hyperwire.graph.node import Node
from hyperwire.transport.broker import Broker


class Listener(Node):
    """
    The head of a detached graph section fed by publishers:

        source >> process >> Publisher(topics=["done"])
        Listener(topics=["done"]) >> enrich >> sink

    Both lines can live in the same pipeline or in different ones
    sharing a broker.
    """

    def __init__(
        self,
        topics: list[str],
        name: str = "listener",
        workers: int = 1,
        broker: Broker | None = None,
    ) -> None:
        super().__init__(name=name, workers=workers)
        self.topics = list(topics)
        self.broker = broker if broker is not None else Broker.default()
        for topic in self.topics:
            self.broker.subscribe(topic, self)
