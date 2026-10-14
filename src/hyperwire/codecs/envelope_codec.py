"""
EnvelopeCodec carries a whole message: id, content, metadata, timestamp.
"""

import json
from base64 import b64decode, b64encode
from datetime import datetime

from hyperwire.models.message import Message


class EnvelopeCodec:
    """
    Flow to flow over a broker: the message crosses whole, so its id and
    metadata survive the hop. Bytes content rides base64 and is marked,
    so it comes back as bytes. A payload that is not an envelope becomes
    a bytes message rather than a guess.
    """

    name = "envelope"
    suffix = ".json"

    def encode(self, message: Message) -> bytes:
        """The message as one json envelope."""
        content = message.content
        body = {
            "id": message.id,
            "content": content,
            "metadata": message.metadata,
            "timestamp": message.timestamp.isoformat(),
        }
        if isinstance(content, (bytes, bytearray)):
            body["content"] = b64encode(bytes(content)).decode()
            body["encoding"] = "base64"
        return json.dumps(body, default=str).encode()

    def decode(self, payload: bytes, metadata: dict) -> Message:
        """The envelope restored, or the raw payload when it is not one."""
        try:
            body = json.loads(payload)
        except (ValueError, TypeError):
            return Message(content=payload, metadata=metadata)
        if not isinstance(body, dict) or not {"id", "content", "metadata"} <= body.keys():
            return Message(content=payload, metadata=metadata)
        content = body["content"]
        if body.get("encoding") == "base64":
            content = b64decode(content)
        return Message(
            content=content,
            metadata={**body["metadata"], **metadata},
            id=body["id"],
            timestamp=stamp(body.get("timestamp")),
        )


def stamp(value) -> datetime | None:
    """An iso timestamp, or None when absent or unreadable."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (ValueError, TypeError):
        return None
