"""
Transport behavior: publisher to listener delivery, source offsets
with commit and replay, manual poll, pause, and graceful stop.
"""

import asyncio

import pytest

from hyperwire import (
    Broker,
    Listener,
    MemorySink,
    Message,
    Pipeline,
    Publisher,
    QueueSource,
)


def test_publisher_reaches_listener_chain():
    broker = Broker()
    pipeline = Pipeline(name="pubsub")
    source = QueueSource()
    sink = MemorySink()

    @pipeline.processor()
    def stamp(message):
        message.metadata["stamped"] = True
        return message

    source >> stamp >> Publisher(topics=["done"], broker=broker)
    Listener(topics=["done"], broker=broker) >> sink

    async def run():
        await source.push(Message(content="x"))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert len(sink.messages) == 1
    assert sink.messages[0].metadata["stamped"] is True


def test_commit_happens_only_on_clean_runs():
    pipeline = Pipeline(name="offsets")
    source = QueueSource()
    sink = MemorySink()

    @pipeline.processor()
    def fine(message):
        return message

    source >> fine >> sink

    async def run():
        await source.push(Message(content=1))
        await source.push(Message(content=2))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert source.delivered == 2
    assert source.committed == 2
    assert source.retained == []


def test_replay_requeues_uncommitted_messages():
    source = QueueSource()

    async def run():
        await source.push(Message(content="a"))
        polled = await source.poll(1)
        assert polled[0].content == "a"
        assert source.delivered == 1
        assert source.committed == 0
        assert source.replay() == 1
        again = await source.poll(1)
        assert again[0].content == "a"
        source.commit()
        assert source.committed == source.delivered

    asyncio.run(run())


def test_poll_returns_early_when_source_closes():
    source = QueueSource()

    async def run():
        await source.push(Message(content=1))
        await source.close()
        return await source.poll(5)

    polled = asyncio.run(run())
    assert len(polled) == 1


def test_pause_stops_source_pumping():
    source = QueueSource()
    sink = MemorySink()
    source >> sink

    async def run():
        for index in range(5):
            await source.push(Message(content=index))
        source.pause()
        sink.start()
        pump = asyncio.create_task(source.run())
        await asyncio.sleep(0.05)
        assert sink.inbox.qsize() == 0 and sink.messages == []
        source.resume()
        await source.close()
        await pump
        await sink.inbox.join()
        await sink.stop()

    asyncio.run(run())
    assert len(sink.messages) == 5


def test_pipeline_stop_is_graceful():
    pipeline = Pipeline(name="stopper")
    source = QueueSource()
    sink = MemorySink()

    @pipeline.processor()
    def slow(message):
        return message

    source >> slow >> sink

    async def run():
        for index in range(3):
            await source.push(Message(content=index))
        task = asyncio.create_task(pipeline.run(source))
        await asyncio.sleep(0.05)
        pipeline.stop()
        await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())
    assert len(sink.messages) == 3


def test_run_without_sources_raises():
    pipeline = Pipeline(name="empty")
    with pytest.raises(ValueError):
        asyncio.run(pipeline.run())
