"""
ClaimCheckSink: park the payload in a store, queue a claim naming it.
"""

import json
from pathlib import Path

from plyght.connectors.kafka import KafkaSettings

from hyperwire.codecs.codec import Codec
from hyperwire.codecs.registry import codec as resolve
from hyperwire.kafka.kafka_sink import acknowledgement
from hyperwire.models.message import Message
from hyperwire.transport.dataset import topic
from hyperwire.transport.sink import Sink


class ClaimCheckSink(Sink):
    """
    The claim-check pattern for payloads too big or too binary for the
    broker. Each message's payload is written to s3 (give bucket) or a
    directory (give directory) through its codec - bytes by default, so
    a blob lands exactly as it is - and only a small json claim goes to
    the topic. The claim is the one place json is assumed, because the
    claim is ours:

        {"id", "metadata", "codec", "store": "s3", "bucket", "key"}
        {"id", "metadata", "codec", "store": "file", "path"}

    Broker and store settings come from PLYGHT_KAFKA_* and PLYGHT_S3_*
    unless given. s3 needs hyperwire[s3].
    """

    def __init__(
        self,
        topic: str,
        bucket: str | None = None,
        prefix: str = "claims",
        directory: str | None = None,
        name: str = "claim-out",
        codec: "str | Codec" = "bytes",
        kafka: KafkaSettings | None = None,
        s3=None,
        **overrides,
    ) -> None:
        super().__init__(name=name)
        if (bucket is None) == (directory is None):
            raise ValueError("give exactly one of bucket (s3) or directory (file)")
        base = kafka or KafkaSettings()
        self.kafka = base.merge(**overrides)
        self.topic = topic
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.directory = Path(directory) if directory else None
        self.codec = resolve(codec)
        self.s3_settings = s3
        self._producer = None
        self._store = None

    async def park(self, message: Message) -> dict:
        """Persist the payload; return the claim coordinates naming where."""
        payload = self.codec.encode(message)
        filename = f"{message.id}{self.codec.suffix}"
        if self.bucket is not None:
            if self._store is None:
                from plyght.connectors.s3 import S3Connector

                self._store = S3Connector(settings=self.s3_settings)
                await self._store.connect()
                await self._store.ensure_bucket(self.bucket)
            key = f"{self.prefix}/{filename}"
            await self._store.put(self.bucket, key, payload)
            return {"store": "s3", "bucket": self.bucket, "key": key}
        self.directory.mkdir(parents=True, exist_ok=True)
        location = self.directory / filename
        location.write_bytes(payload)
        return {"store": "file", "path": str(location.resolve())}

    def datasets(self) -> list[str]:
        """
        The claims topic. The payload store is where the bytes park,
        not what downstream flows read by, so it is not a dataset here.
        """
        return [topic(self.kafka.bootstrap, self.topic)]

    def claim(self, message: Message, coordinates: dict) -> bytes:
        """The json claim event for one parked payload."""
        body = {
            "id": message.id,
            "metadata": message.metadata,
            "codec": self.codec.name,
            **coordinates,
        }
        return json.dumps(body, default=str).encode()

    async def deliver(self, message: Message) -> None:
        """Park the payload, then queue the claim."""
        coordinates = await self.park(message)
        if self._producer is None:
            from aiokafka import AIOKafkaProducer

            self._producer = AIOKafkaProducer(
                acks=acknowledgement(self.kafka.acks), **self.kafka.common()
            )
            await self._producer.start()
        await self._producer.send_and_wait(
            self.topic, self.claim(message, coordinates), key=message.id.encode()
        )

    async def stop(self) -> None:
        """Stop the node's workers, then release producer and store."""
        await super().stop()
        producer, self._producer = self._producer, None
        if producer is not None:
            await producer.stop()
        store, self._store = self._store, None
        if store is not None:
            await store.aclose()
