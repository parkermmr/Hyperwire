"""
Codecs: bytes by default with no guessing, json only when asked, and an
envelope that carries a whole message - bytes content included.
"""

import pytest

from hyperwire import Message
from hyperwire.codecs import CODECS, BytesCodec, codec, register


def test_bytes_passes_payloads_through():
    message = CODECS["bytes"].decode(b'{"looks": "like json"}', {"from": "x"})
    assert message.content == b'{"looks": "like json"}'
    assert message.metadata == {"from": "x"}


def test_bytes_encodes_by_content_type():
    bytes_codec = CODECS["bytes"]
    assert bytes_codec.encode(Message(content=b"\x00\x01")) == b"\x00\x01"
    assert bytes_codec.encode(Message(content="text")) == b"text"
    assert bytes_codec.encode(Message(content={"a": 1})) == b'{"a": 1}'


def test_json_decodes_and_refuses_non_json():
    assert CODECS["json"].decode(b'{"a": 1}', {}).content == {"a": 1}
    with pytest.raises(ValueError):
        CODECS["json"].decode(b"\xff not json", {})


def test_text_replaces_bad_bytes():
    assert CODECS["text"].decode(b"ok\xff", {}).content == "ok�"


def test_envelope_round_trips_bytes_and_identity():
    original = Message(content=b"\x00raw", metadata={"trace": "t1"})
    envelope = CODECS["envelope"]
    back = envelope.decode(envelope.encode(original), {"kafka_offset": 4})
    assert back.content == b"\x00raw"
    assert back.id == original.id
    assert back.metadata == {"trace": "t1", "kafka_offset": 4}


def test_envelope_leaves_foreign_payloads_as_bytes():
    back = CODECS["envelope"].decode(b'{"not": "an envelope"}', {})
    assert back.content == b'{"not": "an envelope"}'


def test_names_resolve_and_register():
    class Upper(BytesCodec):
        name = "upper"

    register(Upper())
    try:
        assert codec("upper").name == "upper"
    finally:
        CODECS.pop("upper")
    with pytest.raises(ValueError, match="unknown codec"):
        codec("yaml")
