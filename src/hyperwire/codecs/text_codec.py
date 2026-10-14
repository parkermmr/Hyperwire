"""
TextCodec carries content as utf-8 text.
"""

from hyperwire.models.message import Message


class TextCodec:
    """
    Text in, text out. Undecodable bytes are replaced rather than
    raised, so one bad record never stops a stream.
    """

    name = "text"
    suffix = ".txt"

    def encode(self, message: Message) -> bytes:
        """The content as utf-8."""
        content = message.content
        if isinstance(content, (bytes, bytearray)):
            return bytes(content)
        return str(content).encode()

    def decode(self, payload: bytes, metadata: dict) -> Message:
        """The payload decoded as utf-8."""
        return Message(content=payload.decode(errors="replace"), metadata=metadata)
