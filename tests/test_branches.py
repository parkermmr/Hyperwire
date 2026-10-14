"""
Fan out branch semantics: every matching when fires, otherwise is a
true fallback, and terminated branches still receive messages.
"""

import asyncio

from hyperwire import (
    MemorySink,
    Message,
    Pipeline,
    Processor,
    QueueSource,
    otherwise,
    terminate,
    when,
)


def build(name="branches"):
    return Pipeline(name=name), QueueSource(), MemorySink()


def tap(label, seen):
    def inner(message):
        seen.setdefault(label, []).append(message.content)
        return message

    return inner


async def pump(pipeline, source, contents):
    for content in contents:
        await source.push(Message(content=content))
    await source.close()
    await pipeline.run(source)


def test_every_matching_when_fires():
    pipeline, source, sink = build()
    seen = {}
    low = Processor(tap("low", seen), name="low")
    high = Processor(tap("high", seen), name="high")
    source >> [
        when(lambda message: message.content >= 0) >> low,
        when(lambda message: message.content >= 1) >> high,
    ] >> sink
    asyncio.run(pump(pipeline, source, [0, 1, 2]))
    assert seen["low"] == [0, 1, 2]
    assert seen["high"] == [1, 2]


def test_otherwise_fires_only_when_nothing_matched():
    pipeline, source, sink = build()
    seen = {}
    even = Processor(tap("even", seen), name="even")
    rest = Processor(tap("rest", seen), name="rest")
    source >> [
        when(lambda message: message.content % 2 == 0) >> even,
        otherwise >> rest,
    ] >> sink
    asyncio.run(pump(pipeline, source, [0, 1, 2, 3]))
    assert seen["even"] == [0, 2]
    assert seen["rest"] == [1, 3]


def test_terminated_branch_receives_but_never_converges():
    pipeline, source, sink = build()
    seen = {}
    side = Processor(tap("side", seen), name="side")
    main = Processor(tap("main", seen), name="main")
    source >> [side >> terminate, main] >> sink
    asyncio.run(pump(pipeline, source, [1, 2]))
    assert seen["side"] == [1, 2]
    assert seen["main"] == [1, 2]
    assert sorted(message.content for message in sink.messages) == [1, 2]


def test_unconditional_branch_rides_along_with_whens():
    pipeline, source, sink = build()
    seen = {}
    audit = Processor(tap("audit", seen), name="audit")
    matched = Processor(tap("matched", seen), name="matched")
    source >> [
        audit >> terminate,
        when(lambda message: message.content > 1) >> matched,
    ] >> sink
    asyncio.run(pump(pipeline, source, [1, 2]))
    assert seen["audit"] == [1, 2]
    assert seen["matched"] == [2]
    assert [message.content for message in sink.messages] == [2]


def test_conditional_edges_describe_their_predicates():
    def oversized(message):
        return True

    source = QueueSource(name="In")
    step = Processor(lambda message: message, name="Step")
    orders = MemorySink(name="Orders")
    rest = MemorySink(name="Rest")
    source >> step >> [
        when(lambda message: message.metadata.get("retry")) >> step,
        when(kind="order", region="eu") >> orders,
        when(oversized) >> rest,
        otherwise >> rest,
    ]
    labels = [(edge["kind"], edge["label"]) for edge in step.describe()["edges"]]
    assert labels == [
        ("conditional", "message.metadata.get('retry')"),
        ("conditional", "kind='order', region='eu'"),
        ("conditional", "oversized"),
        ("fallback", ""),
    ]
