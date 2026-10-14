"""
The free processor decorator: callable as a plain function, joined to
a pipeline by wiring, and bound to its state and error route at run.
"""

import asyncio

from hyperwire import MemorySink, MemoryState, Message, Pipeline, QueueSource, processor


async def pump(pipeline, source, contents):
    for content in contents:
        await source.push(Message(content=content))
    await source.close()
    await pipeline.run(source)


def test_decorated_function_stays_callable():
    @processor
    def double(message):
        return message * 2

    @processor(workers=2)
    async def triple(message):
        return message * 3

    assert double(4) == 8
    assert asyncio.run(triple(2)) == 6
    assert double.__name__ == "double"
    assert double.name == "double"
    assert double.workers == 1 and triple.workers == 2


def test_wiring_joins_the_pipeline():
    pipeline, source, sink = Pipeline(name="free"), QueueSource(), MemorySink()

    @processor
    def double(message):
        message.content *= 2
        return message

    source >> double >> sink
    asyncio.run(pump(pipeline, source, [1, 2, 3]))
    assert sorted(message.content for message in sink.messages) == [2, 4, 6]


def test_state_binds_at_run():
    state = MemoryState()
    pipeline, source, sink = Pipeline(name="stateful", state=state), QueueSource(), MemorySink()

    @processor
    async def count(message, state):
        await state.set("seen", (await state.get("seen") or 0) + 1)
        return message

    source >> count >> sink
    asyncio.run(pump(pipeline, source, [1, 2]))
    assert count.state is state


def test_pipeline_error_route_is_adopted():
    errors = MemorySink()
    pipeline, source, sink = Pipeline(name="errors", errors=errors), QueueSource(), MemorySink()

    @processor
    def fail(message):
        raise ValueError("boom")

    source >> fail >> sink
    asyncio.run(pump(pipeline, source, [1]))
    assert len(errors.messages) == 1
    assert sink.messages == []


def test_error_chain_never_routes_into_itself():
    errors_out = MemorySink()

    @processor
    def handle(message):
        raise ValueError("handler failed too")

    handle >> errors_out
    pipeline, source = Pipeline(name="loop", errors=handle), QueueSource()

    @processor
    def fail(message):
        raise ValueError("boom")

    source >> fail
    asyncio.run(asyncio.wait_for(pump(pipeline, source, [1]), timeout=5))
    assert handle.errors is None
