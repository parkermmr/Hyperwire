"""
Examples for hyperwire, standard library only.

Example 1: conditional routing with parallel workers.
Example 2: shared state, a context resource, and an error route.
Example 3: jsonl files in and out.

    python example.py
"""

import asyncio
import json
from pathlib import Path
from tempfile import TemporaryDirectory

from hyperwire import (
    Context,
    MemorySink,
    Message,
    Pipeline,
    QueueSource,
    otherwise,
    processor,
    when,
)
from hyperwire.files import JsonlSink, JsonlSource


@processor
def classify(message: Message) -> Message:
    """Tag each message with the lane it belongs in."""
    message.metadata["lane"] = "fast" if message.content.get("urgent") else "slow"
    return message


@processor(workers=4)
async def fast(message: Message) -> Message:
    """Four of these run at once."""
    print(f"  fast lane: {message.content}")
    return message


@processor
def slow(message: Message) -> Message:
    """Sync functions run in a thread so they never block the loop."""
    print(f"  slow lane: {message.content}")
    return message


async def routing() -> None:
    """
    Route urgent messages to a fast lane and the rest to a slow lane,
    then converge both lanes on one sink.
    """
    pipeline = Pipeline(name="routing")
    source = QueueSource()
    sink = MemorySink()

    source >> classify >> [
        when(lane="fast") >> fast,
        otherwise >> slow,
    ] >> sink

    for urgent in (True, False, True):
        await source.push(Message(content={"urgent": urgent}))
    await source.close()
    await pipeline.run(source)
    print(f"  delivered: {len(sink.messages)}")


@processor(workers=2, retries=2, backoff=0.01)
async def price(message: Message, state, context: Context) -> Message:
    """
    Declaring state and context parameters is all it takes to receive
    them. A failure retries twice, then takes the error route.
    """
    if message.content["item"] not in context.prices:
        raise KeyError(message.content["item"])
    message.content["price"] = context.prices[message.content["item"]]
    message.content["seen"] = await state.increment("seen")
    return message


async def stateful() -> None:
    """Shared state, a context resource, and an error route."""
    failures = MemorySink()
    pipeline = Pipeline(
        name="pricing",
        errors=failures,
        context=Context(prices={"apple": 3, "pear": 5}),
    )
    source = QueueSource()
    sink = MemorySink()

    source >> price >> sink

    for item in ("apple", "pear", "plum"):
        await source.push(Message(content={"item": item}))
    await source.close()
    await pipeline.run(source)
    print(f"  priced: {sorted(message.content['item'] for message in sink.messages)}")
    print(f"  failed: {[message.content['item'] for message in failures.messages]}")
    print(f"  price node: processed={price.processed} retried={price.retried}")


@processor
def shout(message: Message) -> Message:
    """Upper case the text field."""
    message.content["text"] = message.content["text"].upper()
    return message


async def files(directory: Path) -> None:
    """Read a jsonl file, transform each line, write another."""
    inbound = directory / "in.jsonl"
    outbound = directory / "out.jsonl"
    inbound.write_text("\n".join(json.dumps({"text": word}) for word in ("one", "two")))

    source = JsonlSource(str(inbound))
    source >> shout >> JsonlSink(str(outbound))
    await Pipeline(name="files").run(source)
    print(f"  wrote: {outbound.read_text().splitlines()}")


async def main() -> None:
    print("routing")
    await routing()
    print("stateful")
    await stateful()
    print("files")
    with TemporaryDirectory() as scratch:
        await files(Path(scratch))


if __name__ == "__main__":
    asyncio.run(main())
