"""
KafkaSink: a kafka topic as the exit node of a flow.
"""

from aiokafka import AIOKafkaProducer
from plyght.connectors.kafka import KafkaSettings

from hyperwire.codecs.codec import Codec
from hyperwire.codecs.registry import codec as resolve
from hyperwire.models.message import Message
from hyperwire.transport.dataset import topic
from hyperwire.transport.sink import Sink


def acknowledgement(acks: str):
    """The producer acks setting: a number when numeric, else the word."""
    return int(acks) if acks.lstrip("-").isdigit() else acks


class KafkaSink(Sink):
    """
    Produce every delivered message to one topic, keyed by message id so
    a partitioned topic keeps per-message ordering. The value is the
    content as bytes by default; codec="envelope" sends the whole
    message so a downstream flow's KafkaSource(codec="envelope") gets
    its id and metadata back.
    """

    def __init__(
        self,
        topic: str,
        name: str = "kafka-out",
        codec: "str | Codec" = "bytes",
        settings: KafkaSettings | None = None,
        **overrides,
    ) -> None:
        super().__init__(name=name)
        base = settings or KafkaSettings()
        self.settings = base.merge(**overrides)
        self.topic = topic
        self.codec = resolve(codec)
        self._producer: AIOKafkaProducer | None = None

    def datasets(self) -> list[str]:
        """The topic, addressed by the brokers it lives on."""
        return [topic(self.settings.bootstrap, self.topic)]

    async def deliver(self, message: Message) -> None:
        """Send one message, waiting for the broker's ack."""
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                acks=acknowledgement(self.settings.acks), **self.settings.common()
            )
            await self._producer.start()
        await self._producer.send_and_wait(
            self.topic, self.codec.encode(message), key=message.id.encode()
        )

    async def stop(self) -> None:
        """Stop the node's workers, then flush and release the producer."""
        await super().stop()
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
