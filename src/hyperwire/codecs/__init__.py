"""
Codecs: how message content becomes bytes and back. Transports that
cross a byte boundary take a codec by name - bytes (the default, no
guessing), text, json, or envelope (the whole message, for flow to
flow hops) - and register() adds more.
"""

from hyperwire.codecs.bytes_codec import BytesCodec
from hyperwire.codecs.codec import Codec
from hyperwire.codecs.envelope_codec import EnvelopeCodec
from hyperwire.codecs.json_codec import JsonCodec
from hyperwire.codecs.registry import CODECS, codec, register
from hyperwire.codecs.text_codec import TextCodec

__all__ = [
    "CODECS",
    "BytesCodec",
    "Codec",
    "EnvelopeCodec",
    "JsonCodec",
    "TextCodec",
    "codec",
    "register",
]
