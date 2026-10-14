"""
Codec: how a message's content becomes bytes on a wire and back.
"""

from typing import Protocol

from hyperwire.models.message import Message


class Codec(Protocol):
    """
    A named way of turning messages into payload bytes and payload bytes
    into messages. suffix names the file a parked payload is written to.
    decode receives the transport's own coordinates as metadata so a
    message always says where it came from.
    """

    name: str
    suffix: str

    def encode(self, message: Message) -> bytes:
        """The payload bytes for a message."""

    def decode(self, payload: bytes, metadata: dict) -> Message:
        """The message a payload becomes."""
