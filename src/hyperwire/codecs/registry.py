"""
The codecs a transport can name, and how a name becomes one.
"""

from hyperwire.codecs.bytes_codec import BytesCodec
from hyperwire.codecs.codec import Codec
from hyperwire.codecs.envelope_codec import EnvelopeCodec
from hyperwire.codecs.json_codec import JsonCodec
from hyperwire.codecs.text_codec import TextCodec

CODECS: dict[str, Codec] = {
    codec.name: codec
    for codec in (BytesCodec(), TextCodec(), JsonCodec(), EnvelopeCodec())
}


def register(codec: Codec) -> Codec:
    """Make a codec nameable by transports; returns it for chaining."""
    CODECS[codec.name] = codec
    return codec


def codec(value: "str | Codec") -> Codec:
    """A codec by name, or the codec itself when one is given."""
    if not isinstance(value, str):
        return value
    if value not in CODECS:
        raise ValueError(f"unknown codec {value!r}, have {sorted(CODECS)}")
    return CODECS[value]
