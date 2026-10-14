"""
Kafka transports without a broker: values decode through the chosen
codec (bytes by default), and claim checks park payloads exactly and
redeem them with the codec the claim names.
"""

import asyncio
import json
from types import SimpleNamespace

import pytest

pytest.importorskip("aiokafka")
pytest.importorskip("plyght.connectors.kafka")

from hyperwire import Message
from hyperwire.kafka import ClaimCheckSink, ClaimCheckSource, KafkaSink, KafkaSource


def record(value: bytes, key: bytes | None = b"k1"):
    return SimpleNamespace(topic="events", partition=0, offset=7, key=key, value=value)


def test_source_values_are_bytes_by_default():
    message = KafkaSource("events").decode(record(b'{"a": 1}'))
    assert message.content == b'{"a": 1}'
    assert message.metadata == {
        "kafka_topic": "events", "kafka_partition": 0, "kafka_offset": 7, "kafka_key": "k1",
    }


def test_source_decodes_json_when_asked():
    assert KafkaSource("events", codec="json").decode(record(b'{"a": 1}')).content == {"a": 1}


def test_envelope_chains_flows():
    original = Message(content={"a": 1}, metadata={"trace": "t"})
    value = KafkaSink("out", codec="envelope").codec.encode(original)
    back = KafkaSource("out", codec="envelope").decode(record(value))
    assert back.id == original.id
    assert back.metadata["trace"] == "t"


def park(tmp_path, content, codec="bytes") -> tuple[dict, Message]:
    sink = ClaimCheckSink("claims", directory=str(tmp_path), codec=codec)
    message = Message(content=content, metadata={"origin": "s3-drop"})
    coordinates = asyncio.run(sink.park(message))
    return json.loads(sink.claim(message, coordinates)), message


def test_claims_are_json_and_payloads_exact(tmp_path):
    claim, message = park(tmp_path, b"\x89PNG raw bytes")
    assert claim["codec"] == "bytes" and claim["store"] == "file"
    assert open(claim["path"], "rb").read() == b"\x89PNG raw bytes"
    back = ClaimCheckSource("claims").rebuild(open(claim["path"], "rb").read(), claim)
    assert back.content == b"\x89PNG raw bytes"
    assert back.id == message.id
    assert back.metadata["origin"] == "s3-drop"


def test_redeem_uses_the_codec_the_claim_names(tmp_path):
    claim, _ = park(tmp_path, {"a": 1}, codec="json")
    payload = open(claim["path"], "rb").read()
    assert ClaimCheckSource("claims").rebuild(payload, claim).content == {"a": 1}


def test_hand_rolled_claims_default_to_bytes(tmp_path):
    path = tmp_path / "blob"
    path.write_bytes(b'{"looks": "json"}')
    claim = {"path": str(path)}
    back = ClaimCheckSource("claims").rebuild(path.read_bytes(), claim)
    assert back.content == b'{"looks": "json"}'


def test_kafka_nodes_name_their_topic_whatever_the_broker_order(tmp_path):
    read = KafkaSource("orders", bootstrap="b:9092, a:9092")
    write = KafkaSink("orders", bootstrap="a:9092,b:9092")
    assert read.datasets() == write.datasets() == ["kafka://a:9092,b:9092/orders"]
    claims = ClaimCheckSink("claims", directory=str(tmp_path), bootstrap="a:9092")
    assert claims.datasets() == ClaimCheckSource("claims", bootstrap="a:9092").datasets()


def test_a_node_with_no_outside_data_names_none():
    from hyperwire.graph.node import Node

    assert Node("middle").describe()["datasets"] == []


class Consumer:
    """The slice of AIOKafkaConsumer a source reads its backlog from."""

    def __init__(self, marks: dict, count: int = 3) -> None:
        self.marks = marks
        self.count = count

    def assignment(self):
        return [SimpleNamespace(topic="events", partition=key) for key in self.marks]

    def highwater(self, assigned):
        return self.marks[assigned.partition]

    def partitions_for_topic(self, topic):
        return set(range(self.count)) if topic == "events" else None


def test_lag_is_the_high_water_mark_less_what_was_read():
    source = KafkaSource("events")
    assert source.lag() is None and source.partitions() is None
    source._consumer = Consumer({0: 100, 1: 50, 2: None})
    assert source.lag() is None
    source.positions = {0: 90, 1: 50}
    assert source.lag() == 10
    assert source.partitions() == 3
    snapshot = source.describe()
    assert (snapshot["lag"], snapshot["partitions"]) == (10, 3)


def test_a_source_that_cannot_tell_reports_no_backlog():
    from hyperwire import QueueSource

    snapshot = QueueSource("in").describe()
    assert "lag" not in snapshot and "partitions" not in snapshot
