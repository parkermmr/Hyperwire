"""
JsonCodec carries content as a json document.
"""

import json

from hyperwire.models.message import Message


class JsonCodec:
    """
    Content is a json document on the wire. Chosen, never assumed: a
    payload that is not json raises, because a flow that asked for json
    and silently got text would fail somewhere far less obvious.
    """

    name = "json"
    suffix = ".json"

    def encode(self, message: Message) -> bytes:
        """The content as json."""
        return json.dumps(message.content, default=str).encode()

    def decode(self, payload: bytes, metadata: dict) -> Message:
        """The payload parsed as json."""
        return Message(content=json.loads(payload), metadata=metadata)
