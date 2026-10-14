"""
Retry policies, error routes, and cyclic wiring.
"""

import asyncio

from hyperwire import (
    MemorySink,
    Message,
    Pipeline,
    Processor,
    QueueSource,
    Retry,
    otherwise,
    terminate,
    when,
)


def build(name="resilience"):
    return Pipeline(name=name), QueueSource(), MemorySink()


async def pump(pipeline, source, contents):
    for content in contents:
        await source.push(Message(content=content))
    await source.close()
    await pipeline.run(source)


def test_retry_recovers_before_the_attempts_run_out():
    pipeline, source, sink = build()
    calls = []

    def flaky(message):
        calls.append(message.content)
        if len(calls) < 3:
            raise RuntimeError("not yet")
        return message

    node = Processor(flaky, name="flaky", retry=Retry(times=2))
    source >> node >> sink
    asyncio.run(pump(pipeline, source, [1]))
    assert calls == [1, 1, 1]
    assert node.retried == 2
    assert node.failed == 0
    assert [message.content for message in sink.messages] == [1]


def test_exhausted_retries_take_the_error_route():
    pipeline, source, sink = build()
    dead = MemorySink(name="dead")

    def always(message):
        raise RuntimeError("no")

    node = Processor(always, name="always", retry=Retry(times=1))
    node.failing(dead)
    source >> node >> sink
    pipeline.include(dead)
    asyncio.run(pump(pipeline, source, [1, 2]))
    assert node.retried == 2
    assert node.failed == 2
    assert sorted(message.content for message in dead.messages) == [1, 2]
    assert sink.messages == []


def test_retry_only_catches_its_listed_errors():
    pipeline, source, sink = build()
    calls = []

    def picky(message):
        calls.append(message.content)
        raise TypeError("wrong kind")

    node = Processor(picky, name="picky", retry=Retry(times=3, catching=(ValueError,)))
    source >> node >> sink
    asyncio.run(pump(pipeline, source, [1]))
    assert calls == [1]
    assert node.failed == 1


def test_backoff_grows_and_is_capped():
    policy = Retry(times=5, backoff=1.0, factor=2.0, ceiling=4.0)
    assert [policy.wait(attempt) for attempt in range(1, 5)] == [1.0, 2.0, 4.0, 4.0]


def test_pipeline_decorator_carries_retries():
    pipeline, source, sink = build()
    calls = []

    @pipeline.processor(name="wobbly", retries=2)
    def wobbly(message):
        calls.append(message.content)
        if len(calls) < 2:
            raise RuntimeError("blip")
        return message

    source >> wobbly >> sink
    asyncio.run(pump(pipeline, source, [7]))
    assert calls == [7, 7]
    assert [message.content for message in sink.messages] == [7]


def test_cycle_routes_back_upstream_until_the_guard_clears():
    pipeline, source, sink = build()

    def bump(message):
        message.content += 1
        return message

    step = Processor(bump, name="step")
    source >> step >> [
        when(lambda message: message.content < 3) >> step,
        otherwise >> sink,
    ] >> terminate
    asyncio.run(pump(pipeline, source, [0]))
    assert [message.content for message in sink.messages] == [3]


def test_discovery_reaches_the_error_node_through_the_route():
    pipeline, source, sink = build()
    dead = MemorySink(name="dead")
    node = Processor(lambda message: message, name="step").failing(dead)
    source >> node >> sink
    found = {found.name for found in pipeline.discover(source)}
    assert "dead" in found


def test_describe_reports_retry_and_edge_kinds():
    pipeline, source, sink = build()
    dead = MemorySink(name="dead")
    node = Processor(lambda message: message, name="step")
    node.attempts(times=2, backoff=0.5).failing(dead)
    node >> sink
    snapshot = node.describe()
    assert snapshot["retry"] == {
        "times": 2,
        "backoff": 0.5,
        "factor": 2.0,
        "ceiling": 30.0,
    }
    kinds = {edge["target"]: edge["kind"] for edge in snapshot["edges"]}
    assert kinds == {"sink": "flow", "dead": "error"}
