"""
KafkaSource: a kafka topic as the entry node of a flow.
"""

from typing import AsyncIterator

from aiokafka import AIOKafkaConsumer
from plyght.connectors.kafka import KafkaSettings

from hyperwire.codecs.codec import Codec
from hyperwire.codecs.registry import codec as resolve
from hyperwire.models.message import Message
from hyperwire.transport.dataset import topic
from hyperwire.transport.source import Source


def coordinates(record) -> dict:
    """Where a record came from, for message metadata."""
    found = {
        "kafka_topic": record.topic,
        "kafka_partition": record.partition,
        "kafka_offset": record.offset,
    }
    if record.key is not None:
        found["kafka_key"] = record.key.decode(errors="replace")
    return found


class KafkaSource(Source):
    """
    Consume one topic into the graph. This is the partitioned source:
    every replica joining with the same group becomes one consumer
    group, so kafka splits partitions across replicas.

    Values arrive as bytes by default - Kafka Connect and most
    producers are not json - and codec="json", "text", or "envelope"
    (another flow's KafkaSink with the envelope codec) opts into
    decoding. Broker settings come from PLYGHT_KAFKA_* unless given.

        source = KafkaSource("events", group="my-flow", codec="json")
    """

    def __init__(
        self,
        topic: str,
        group: str = "hyperwire",
        name: str = "kafka-in",
        from_beginning: bool = True,
        codec: "str | Codec" = "bytes",
        settings: KafkaSettings | None = None,
        **overrides,
    ) -> None:
        super().__init__(name=name)
        base = settings or KafkaSettings()
        self.settings = base.merge(**overrides)
        self.topic = topic
        self.group = group
        self.from_beginning = from_beginning
        self.codec = resolve(codec)
        self.positions: dict[int, int] = {}
        self._consumer: AIOKafkaConsumer | None = None

    def datasets(self) -> list[str]:
        """The topic, addressed by the brokers it lives on."""
        return [topic(self.settings.bootstrap, self.topic)]

    def decode(self, record) -> Message:
        """One consumer record as a message."""
        return self.codec.decode(record.value or b"", coordinates(record))

    async def events(self) -> AsyncIterator[Message]:
        """Join the consumer group and yield until close()."""
        self._consumer = AIOKafkaConsumer(
            self.topic,
            group_id=self.group,
            auto_offset_reset="earliest" if self.from_beginning else "latest",
            **self.settings.common(),
        )
        await self._consumer.start()
        try:
            async for record in self._consumer:
                self.positions[record.partition] = record.offset + 1
                yield self.decode(record)
        finally:
            consumer, self._consumer = self._consumer, None
            if consumer is not None:
                await consumer.stop()

    def lag(self) -> int | None:
        """
        Records waiting on the partitions this member holds: each
        partition's last known high water mark less the next offset
        this source will read. Partitions it has read nothing from yet
        are left out rather than guessed, so None until there is data.
        """
        consumer = self._consumer
        if consumer is None:
            return None
        waiting = None
        for assigned in consumer.assignment():
            mark = consumer.highwater(assigned)
            position = self.positions.get(assigned.partition)
            if mark is None or position is None:
                continue
            waiting = (waiting or 0) + max(0, mark - position)
        return waiting

    def partitions(self) -> int | None:
        """The topic's partition count, from the consumer's metadata."""
        if self._consumer is None:
            return None
        found = self._consumer.partitions_for_topic(self.topic)
        return len(found) if found else None

    async def close(self) -> None:
        """Leave the group; the events iterator ends and the flow drains."""
        if self._consumer is not None:
            await self._consumer.stop()
