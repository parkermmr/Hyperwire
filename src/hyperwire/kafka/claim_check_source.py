"""
ClaimCheckSource: redeem queued claims back into messages.
"""

import json
from pathlib import Path
from typing import AsyncIterator

from plyght.connectors.kafka import KafkaSettings

from hyperwire.codecs.codec import Codec
from hyperwire.codecs.registry import CODECS
from hyperwire.codecs.registry import codec as resolve
from hyperwire.models.message import Message
from hyperwire.transport.dataset import topic
from hyperwire.transport.source import Source

LOCATIONS = ("bucket", "key", "path")


class ClaimCheckSource(Source):
    """
    Consume json claims from a topic (a consumer group, like
    KafkaSource), fetch each named payload from s3 or the filesystem,
    and emit it. The payload decodes with the codec the claim names, so
    it round trips whatever the sink parked; a hand-rolled claim that
    names none gets this source's codec, bytes by default. codec forces
    one codec for every claim. delete=True removes a payload once it has
    been redeemed.
    """

    def __init__(
        self,
        topic: str,
        group: str = "hyperwire",
        name: str = "claim-in",
        from_beginning: bool = True,
        delete: bool = False,
        codec: "str | Codec | None" = None,
        kafka: KafkaSettings | None = None,
        s3=None,
        **overrides,
    ) -> None:
        super().__init__(name=name)
        base = kafka or KafkaSettings()
        self.kafka = base.merge(**overrides)
        self.topic = topic
        self.group = group
        self.from_beginning = from_beginning
        self.delete = delete
        self.forced = resolve(codec) if codec is not None else None
        self.s3_settings = s3
        self._consumer = None
        self._store = None

    def datasets(self) -> list[str]:
        """The claims topic; the payload store is named in each claim."""
        return [topic(self.kafka.bootstrap, self.topic)]

    def codec_for(self, claim: dict) -> Codec:
        """The codec a claim's payload decodes with."""
        if self.forced is not None:
            return self.forced
        return CODECS.get(str(claim.get("codec", "")), CODECS["bytes"])

    async def redeem(self, claim: dict) -> bytes:
        """Fetch the payload a claim points at."""
        if "path" in claim:
            location = Path(claim["path"])
            payload = location.read_bytes()
            if self.delete:
                location.unlink(missing_ok=True)
            return payload
        if self._store is None:
            from plyght.connectors.s3 import S3Connector

            self._store = S3Connector(settings=self.s3_settings)
            await self._store.connect()
        payload = await self._store.get(claim["bucket"], claim["key"])
        if self.delete:
            await self._store.delete(claim["bucket"], claim["key"])
        return payload

    def rebuild(self, payload: bytes, claim: dict) -> Message:
        """
        A redeemed payload as a message: the claim's metadata and its
        store coordinates ride along, and the claim's id is kept unless
        the payload is an envelope that carries its own.
        """
        chosen = self.codec_for(claim)
        found = {key: claim[key] for key in LOCATIONS if key in claim}
        metadata = {**(claim.get("metadata") or {}), **found}
        message = chosen.decode(payload, metadata)
        if claim.get("id") and chosen.name != "envelope":
            message.id = str(claim["id"])
        return message

    async def events(self) -> AsyncIterator[Message]:
        """Join the group, redeem every claim, yield until close()."""
        from aiokafka import AIOKafkaConsumer

        self._consumer = AIOKafkaConsumer(
            self.topic,
            group_id=self.group,
            auto_offset_reset="earliest" if self.from_beginning else "latest",
            **self.kafka.common(),
        )
        await self._consumer.start()
        try:
            async for record in self._consumer:
                try:
                    claim = json.loads(record.value)
                except (ValueError, TypeError):
                    continue
                if not isinstance(claim, dict):
                    continue
                payload = await self.redeem(claim)
                yield self.rebuild(payload, claim)
        finally:
            consumer, self._consumer = self._consumer, None
            if consumer is not None:
                await consumer.stop()
            store, self._store = self._store, None
            if store is not None:
                await store.aclose()

    async def close(self) -> None:
        """Leave the group; the events iterator ends and the flow drains."""
        if self._consumer is not None:
            await self._consumer.stop()
