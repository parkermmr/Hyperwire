"""
Sampled provenance: bounded rings, media types rendered on read, and
live toggling.
"""

import asyncio
from dataclasses import dataclass

from hyperwire import (
    MemorySink,
    Message,
    Pipeline,
    Processor,
    Provenance,
    QueueSource,
)
from hyperwire.runtime.provenance import render, sniff


def build(name="provenance"):
    return Pipeline(name=name), QueueSource(name="In"), MemorySink(name="Out")


async def pump(pipeline, source, contents):
    for content in contents:
        await source.push(Message(content=content))
    await source.close()
    await pipeline.run(source)


def test_records_what_crossed_each_node():
    pipeline, source, sink = build()
    recorder = Provenance()
    pipeline.provenance(recorder)
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, [1, 2, 3]))
    assert [record["content"] for record in recorder.read("Step")] == ["3", "2", "1"]
    assert recorder.read("Step")[0]["edge"] == "Out"


def test_ring_is_bounded_by_depth():
    pipeline, source, sink = build()
    recorder = Provenance(depth=2)
    pipeline.provenance(recorder)
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, list(range(10))))
    kept = recorder.read("Step")
    assert len(kept) == 2
    assert [record["content"] for record in kept] == ["9", "8"]


def test_content_is_cut_to_width_but_metadata_is_kept_whole():
    pipeline, source, sink = build()
    recorder = Provenance(width=10)
    pipeline.provenance(recorder)
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink

    async def go():
        await source.push(
            Message(content="x" * 100, metadata={"tenant": "acme", "kind": "big"})
        )
        await source.close()
        await pipeline.run(source)

    asyncio.run(go())
    record = recorder.read("Step")[0]
    assert record["truncated"] is True
    assert len(record["content"]) == 10
    assert record["metadata"] == {"tenant": "acme", "kind": "big"}


def test_payloads_sniff_as_media_types():
    @dataclass
    class Order:
        sku: str

    assert sniff(b'{"a": 1}', {}) == "application/json"
    assert sniff(b"hello", {}) == "text/plain"
    assert sniff(b"\xff\x00\x10", {}) == "application/octet-stream"
    assert sniff({"a": 1}, {}) == "application/json"
    assert sniff(Order("x"), {}) == "application/json"
    assert sniff(object(), {}) == "text/x-python"
    assert sniff(b"a,b", {"content-type": "application/json; charset=utf-8"}) == (
        "application/json"
    )


def test_a_record_reads_in_any_view_after_the_fact():
    recorder = Provenance()
    recorder.observe("Step", Message(content=b'{"a": 1}'))
    auto = recorder.read("Step")[0]
    assert auto["mime"] == "application/json"
    assert auto["content"] == '{\n  "a": 1\n}'
    assert recorder.read("Step", view="text/plain")[0]["content"] == '{"a": 1}'
    binary = recorder.read("Step", view="application/octet-stream")[0]["content"]
    assert binary.startswith("00000000  7b 22 61 22")
    assert "payload" not in auto


def test_structures_are_captured_as_json():
    @dataclass
    class Order:
        sku: str
        count: int

    recorder = Provenance()
    recorder.observe("Step", Message(content=Order("x", 2)))
    assert recorder.read("Step", view="text/plain")[0]["content"] == (
        '{"sku": "x", "count": 2}'
    )


def test_unreadable_payload_still_reports_its_type():
    class Opaque:
        def __repr__(self):
            raise RuntimeError("no repr for you")

    recorder = Provenance()
    recorder.observe("Step", Message(content=Opaque()))
    record = recorder.read("Step")[0]
    assert "Opaque" in record["content"]
    assert record["type"] == "Opaque"
    assert render(b"\xff", "application/json") == "\ufffd"


def test_depth_changes_apply_to_existing_rings():
    recorder = Provenance(depth=5)
    for index in range(5):
        recorder.observe("Step", Message(content=index))
    recorder.depth = 2
    recorder.observe("Step", Message(content=5))
    assert [record["content"] for record in recorder.read("Step")] == ["5", "4"]


def test_disabled_recorder_keeps_nothing():
    pipeline, source, sink = build()
    recorder = Provenance(enabled=False)
    pipeline.provenance(recorder)
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, [1, 2]))
    assert recorder.read() == []
    assert recorder.seen == 0


def test_recording_toggles_on_a_running_graph():
    pipeline, source, sink = build()
    recorder = Provenance(enabled=False)
    pipeline.provenance(recorder)

    def flip(message):
        if message.content == 2:
            recorder.enabled = True
        return message

    step = Processor(flip, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, [1, 2, 3]))
    assert [record["content"] for record in recorder.read("Step")] == ["3", "2"]


def test_reading_the_whole_graph_orders_newest_first():
    pipeline, source, sink = build()
    recorder = Provenance()
    pipeline.provenance(recorder)
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, [1, 2]))
    everything = recorder.read()
    # the sink emits with nothing subscribed, so its edge is empty
    assert {record["edge"] for record in everything} == {"Step", "Out", ""}
    assert everything == sorted(everything, key=lambda r: r["at"], reverse=True)


def test_no_recorder_leaves_nodes_untouched():
    pipeline, source, sink = build()
    step = Processor(lambda message: message, name="Step")
    source >> step >> sink
    asyncio.run(pump(pipeline, source, [1]))
    assert step.recorder is None
    assert [message.content for message in sink.messages] == [1]
