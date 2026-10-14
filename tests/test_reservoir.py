"""
Reservoir: holds what reaches it, releases on demand, bounds itself,
and keeps a run from finishing while anything is held.
"""

import asyncio

import pytest

from hyperwire import MemorySink, Message, Pipeline, QueueSource, Reservoir


def build(**options):
    pipeline = Pipeline(name="reservoir")
    source = QueueSource(name="In")
    reservoir = Reservoir(name="Hold", **options)
    sink = MemorySink(name="Out")
    source >> reservoir >> sink
    return pipeline, source, reservoir, sink


async def settle(condition, timeout=2.0):
    for _ in range(int(timeout / 0.01)):
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition never held")


def test_holds_until_released_in_arrival_order():
    pipeline, source, reservoir, sink = build()

    async def go():
        run = asyncio.create_task(pipeline.run(source))
        for content in (1, 2, 3):
            await source.push(Message(content=content))
        await source.close()
        await settle(lambda: len(reservoir.held) == 3)
        assert sink.messages == []
        assert [message.content for message in reservoir.peek()] == [1, 2, 3]
        assert await reservoir.release(2) == 2
        await settle(lambda: len(sink.messages) == 2)
        assert not run.done()
        assert await reservoir.release() == 1
        await asyncio.wait_for(run, 2)

    asyncio.run(go())
    assert [message.content for message in sink.messages] == [1, 2, 3]
    assert source.committed == 3


def test_a_held_run_does_not_finish_or_commit():
    pipeline, source, reservoir, sink = build()

    async def go():
        run = asyncio.create_task(pipeline.run(source))
        await source.push(Message(content="x"))
        await source.close()
        await settle(lambda: len(reservoir.held) == 1)
        await asyncio.sleep(0.1)
        assert not run.done()
        assert source.committed == 0
        assert reservoir.drop() == 1
        await asyncio.wait_for(run, 2)

    asyncio.run(go())
    assert sink.messages == []
    assert reservoir.describe()["reservoir"]["dropped"] == 1


def test_open_releases_and_passes_through():
    pipeline, source, reservoir, sink = build()

    async def go():
        run = asyncio.create_task(pipeline.run(source))
        await source.push(Message(content="held"))
        await settle(lambda: len(reservoir.held) == 1)
        assert await reservoir.open() == 1
        await source.push(Message(content="through"))
        await source.close()
        await asyncio.wait_for(run, 2)

    asyncio.run(go())
    assert [message.content for message in sink.messages] == ["held", "through"]


def test_drop_overflow_keeps_the_newest():
    pipeline, source, reservoir, sink = build(limit=2, overflow="drop")

    async def go():
        run = asyncio.create_task(pipeline.run(source))
        for content in range(5):
            await source.push(Message(content=content))
        await source.close()
        await settle(lambda: reservoir.processed == 5)
        assert [message.content for message in reservoir.peek()] == [3, 4]
        await reservoir.release()
        await asyncio.wait_for(run, 2)

    asyncio.run(go())
    assert reservoir.dropped == 3


def test_block_overflow_backpressures_until_room():
    pipeline, source, reservoir, sink = build(limit=1)

    async def go():
        run = asyncio.create_task(pipeline.run(source))
        await source.push(Message(content="a"))
        await source.push(Message(content="b"))
        await source.close()
        await settle(lambda: len(reservoir.held) == 1 and reservoir.working == 1)
        await reservoir.release(1)
        await settle(lambda: len(reservoir.held) == 1 and reservoir.working == 0)
        await reservoir.release()
        await asyncio.wait_for(run, 2)

    asyncio.run(go())
    assert [message.content for message in sink.messages] == ["a", "b"]


def test_an_unknown_overflow_is_refused():
    with pytest.raises(ValueError):
        Reservoir(overflow="spill")
