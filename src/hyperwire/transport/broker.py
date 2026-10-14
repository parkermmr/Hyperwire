"""
Broker routes published messages to subscribed listeners in process.
"""

from hyperwire.graph.node import Node


class Broker:
    """
    Topic registry connecting publishers to listeners.

    Publishers and listeners share the default broker unless handed
    their own, so two pipeline lines in the same process meet without
    any extra plumbing.
    """

    shared: "Broker | None" = None

    def __init__(self) -> None:
        self.topics: dict[str, list[Node]] = {}

    @classmethod
    def default(cls) -> "Broker":
        """
        The process wide broker, created on first use.
        """
        if cls.shared is None:
            cls.shared = cls()
        return cls.shared

    def subscribe(self, topic: str, node: Node) -> None:
        """
        Register a listener node on a topic.
        """
        nodes = self.topics.setdefault(topic, [])
        if node not in nodes:
            nodes.append(node)

    def listeners(self, topics: list[str]) -> list[Node]:
        """
        Every listener subscribed to any of the topics, deduplicated
        in subscription order.
        """
        found: list[Node] = []
        for topic in topics:
            for node in self.topics.get(topic, []):
                if node not in found:
                    found.append(node)
        return found
