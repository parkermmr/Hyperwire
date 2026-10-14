"""
BytesCodec passes payloads through untouched.
"""

import json

from hyperwire.models.message import Message


class BytesCodec:
    """
    The default everywhere bytes cross a boundary: a kafka value, an
    object in a bucket, a file on disk arrive as the bytes they are, and
    nothing guesses at json. On the way out bytes go as is, text as
    utf-8, and anything else as json, since a dict has no other honest
    byte form.
    """

    name = "bytes"
    suffix = ".bin"

    def encode(self, message: Message) -> bytes:
        """The content as bytes."""
        content = message.content
        if isinstance(content, (bytes, bytearray, memoryview)):
            return bytes(content)
        if isinstance(content, str):
            return content.encode()
        return json.dumps(content, default=str).encode()

    def decode(self, payload: bytes, metadata: dict) -> Message:
        """The payload as content, unread."""
        return Message(content=payload, metadata=metadata)
