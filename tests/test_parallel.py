"""
Parallel processing grains: process mode, performance mode, and
fire and forget remote processing that adjoins results as they land.
"""

import asyncio

import pytest

from hyperwire import MemorySink, Message, Pipeline, QueueSource, remote
from hyperwire.runtime.processor import Processor


def square(message):
    message.content = message.content**2
    return message


def test_process_mode_runs_in_processes():
    pipeline = Pipeline(name="procs")
    source = QueueSource()
    sink = MemorySink()
    worker = pipeline.processor(name="square", workers=2, mode="process")(square)
    source >> worker >> sink

    async def run():
        for value in range(4):
            await source.push(Message(content=value))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert sorted(message.content for message in sink.messages) == [0, 1, 4, 9]


def test_process_mode_rejects_state_and_async():
    def stateful(message, state):
        return message

    async def coroutine(message):
        return message

    with pytest.raises(ValueError):
        Processor(stateful, mode="process")
    with pytest.raises(ValueError):
        Processor(coroutine, mode="process")


def test_performance_mode_still_correct():
    pipeline = Pipeline(name="turbo").performance()
    source = QueueSource()
    sink = MemorySink()

    @pipeline.processor(workers=4)
    def bump(message):
        message.content += 1
        return message

    source >> bump >> sink

    async def run():
        for value in range(10):
            await source.push(Message(content=value))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert sorted(message.content for message in sink.messages) == list(range(1, 11))


class FakeResult:
    def __init__(self, value=None, error=None):
        self.value = value
        self.error = error

    def successful(self):
        return self.error is None


class FakeCompute:
    """
    Completes jobs after a per job delay, out of submission order.
    """

    def __init__(self, delays):
        self.delays = delays
        self.landing: dict[str, float] = {}
        self.payloads: dict[str, Message] = {}

    async def submit(self, function, name, arguments=(), cpus=1.0, ttl=3600.0, keywords=None):
        message = arguments[0]
        delay = self.delays.get(message.content, 0.01)
        self.landing[name] = asyncio.get_running_loop().time() + delay
        self.payloads[name] = message
        return True

    async def poll(self, name):
        if name not in self.landing:
            return None
        if asyncio.get_running_loop().time() < self.landing[name]:
            return None
        message = self.payloads.pop(name)
        del self.landing[name]
        message.metadata["done"] = True
        return FakeResult(value=message)


def test_remote_results_adjoin_out_of_order():
    compute = FakeCompute(delays={"slow": 0.3, "fast": 0.01})
    pipeline = Pipeline(name="remote", compute=compute)
    source = QueueSource()
    sink = MemorySink()

    def work(message):
        return message

    worker = pipeline.remote(name="work", workers=2)(work)
    source >> worker >> sink

    async def run():
        await source.push(Message(content="slow"))
        await source.push(Message(content="fast"))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    contents = [message.content for message in sink.messages]
    assert contents == ["fast", "slow"]
    assert all(message.metadata.get("done") for message in sink.messages)


def test_remote_without_compute_runs_locally():
    pipeline = Pipeline(name="nocompute")
    source = QueueSource()
    sink = MemorySink()

    @remote
    def work(message):
        message.metadata["local"] = True
        return message

    source >> work >> sink

    async def run():
        await source.push(Message(content="x"))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert [message.metadata.get("local") for message in sink.messages] == [True]
    assert work.target is None


def test_a_remotes_own_compute_wins_over_the_pipelines():
    mine = FakeCompute(delays={})
    theirs = FakeCompute(delays={})
    pipeline = Pipeline(name="remote", compute=theirs)
    source = QueueSource()
    sink = MemorySink()

    @remote(workers=2)
    def work(message):
        return message

    @remote
    def other(message):
        return message

    work.compute(mine)
    source >> work >> other >> sink

    async def run():
        await source.push(Message(content="x"))
        await source.close()
        await pipeline.run(source)

    asyncio.run(run())
    assert work.target is mine
    assert other.target is theirs
    assert [message.metadata.get("done") for message in sink.messages] == [True]
    assert work(Message(content="plain")).content == "plain"
