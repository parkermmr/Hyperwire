"""
A remote stage: each message runs as a named job on a compute backend.

Any object with async submit and poll methods is a compute. This uses
asteroidbelt, so install it first:

    pip install asteroidbelt
    python remote.py

Swap LocalCompute for RayCompute(address="ray://head:10001") and the
same graph runs its remote stage on a Ray cluster. Without any compute
attached, the remote stage simply runs in process.
"""

import asyncio

from asteroidbelt import LocalCompute

from hyperwire import MemorySink, Message, Pipeline, QueueSource, remote


@remote(cpus=1.0, ttl=60.0, workers=4)
def score(message: Message) -> Message:
    """
    Runs in a separate process. A remote function receives only the
    message, so it may not touch pipeline state or live objects.
    """
    message.content["score"] = sum(ord(character) for character in message.content["text"])
    return message


async def main() -> None:
    compute = LocalCompute(cpus=2, sweep=0.1)
    await compute.start()
    try:
        pipeline = Pipeline(name="scored", compute=compute)
        source = QueueSource()
        sink = MemorySink()

        source >> score >> sink

        for text in ("alpha", "beta", "gamma"):
            await source.push(Message(content={"text": text}))
        await source.close()
        await pipeline.run(source)
        for message in sink.messages:
            print(message.content)
        print(compute.progress())
    finally:
        await compute.stop()


if __name__ == "__main__":
    asyncio.run(main())
