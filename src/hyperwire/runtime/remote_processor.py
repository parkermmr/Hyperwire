"""
RemoteProcessor farms each message out to a compute backend, and remote
is the decorator that builds one without a pipeline.
"""

import asyncio
import inspect
from functools import update_wrapper
from typing import Callable

from hyperwire.compute.compute import Compute
from hyperwire.graph.node import Node
from hyperwire.graph.retry import Retry
from hyperwire.models.message import Message


class RemoteProcessor(Node):
    """
    Submits the function as a named job per message and emits results
    as jobs land, in completion order. Submission is fire and forget:
    a worker never waits on its own job, so a short task queued after
    a long one flows through the moment it finishes instead of
    sitting behind it.

    Job names derive from the message id, so broker redelivery never
    runs the same work twice. The limit bounds jobs in flight, which
    is this node's backpressure. Remote functions receive only the
    message; they cannot touch pipeline state.

    Where the jobs go is decided when the graph starts: this node's
    own compute when one was given (compute(...)), else the running
    pipeline's. With neither, remote is ignored and the function runs
    here like any processor, so the same flow works with and without
    a cluster behind it.
    """

    def __init__(
        self,
        function: Callable,
        compute: Compute | None = None,
        name: str = "",
        cpus: float = 1.0,
        ttl: float = 3600.0,
        workers: int = 4,
        limit: int = 32,
        interval: float = 0.05,
        retry: Retry | None = None,
    ) -> None:
        super().__init__(name=name or function.__name__, workers=workers, retry=retry)
        self.function = function
        self.backend = compute
        self.target: Compute | None = None
        self.cpus = cpus
        self.ttl = ttl
        self.limit = limit
        self.interval = interval
        self.inflight: dict[str, Message] = {}
        self.collector: asyncio.Task | None = None
        update_wrapper(self, function, updated=())

    def __call__(self, *args, **kwargs):
        """
        Call the wrapped function directly, outside any graph.
        """
        return self.function(*args, **kwargs)

    def compute(self, value: Compute | None = None) -> Compute | None:
        """
        This node's own compute; attaches value first when given. It
        wins over the pipeline's for this node alone.
        """
        if value is not None:
            self.backend = value
        return self.backend

    def resolve(self, fallback: Compute | None) -> Compute | None:
        """
        Settle where jobs go for this run: this node's compute, else
        the fallback (the pipeline's), else nowhere, which runs locally.
        """
        self.target = self.backend if self.backend is not None else fallback
        return self.target

    async def local(self, message: Message) -> Message | None:
        """Run the function here, as a plain processor would."""
        if inspect.iscoroutinefunction(self.function):
            return await self.function(message)
        return await asyncio.to_thread(self.function, message)

    async def handle(self, message: Message) -> Message | None:
        if self.target is None:
            return await self.local(message)
        while len(self.inflight) >= self.limit:
            await asyncio.sleep(self.interval)
        job = f"{self.name}:{message.id}"
        await self.target.submit(
            self.function,
            name=job,
            arguments=(message,),
            cpus=self.cpus,
            ttl=self.ttl,
        )
        self.inflight[job] = message
        return None

    def start(self) -> None:
        super().start()
        if self.target is not None:
            self.collector = asyncio.create_task(self.collect())

    async def stop(self) -> None:
        if self.collector is not None:
            self.collector.cancel()
            await asyncio.gather(self.collector, return_exceptions=True)
            self.collector = None
        await super().stop()

    async def collect(self) -> None:
        """
        Poll the backend for landed jobs and adjoin their results
        into the graph as they come back.
        """
        while True:
            await asyncio.sleep(self.interval)
            for job, message in list(self.inflight.items()):
                result = await self.target.poll(job)
                if result is None:
                    continue
                del self.inflight[job]
                if result.successful():
                    if result.value is not None:
                        await self.emit(result.value)
                else:
                    self.failed += 1
                    self.last_error = f"remote job {job} failed: {result.error}"
                    if self.errors is not None:
                        await self.errors.inbox.put(message)

    def idle(self) -> bool:
        return super().idle() and not self.inflight


def remote(
    function: Callable | None = None,
    name: str = "",
    cpus: float = 1.0,
    ttl: float = 3600.0,
    workers: int = 4,
    limit: int = 32,
    retries: int = 0,
    backoff: float = 0.0,
    errors: Node | None = None,
    compute: Compute | None = None,
):
    """
    Build a remote processor from a function, bare or with options:

        @remote
        def heavy(message): ...

        @remote(cpus=2.0, workers=8)
        def heavier(message): ...

    No pipeline is needed. Wired into a graph, it runs on its own
    compute (compute= here, or heavy.compute(...) later), else on the
    pipeline's; with neither it runs locally. It stays callable as a
    plain function.
    """

    def build(target: Callable) -> RemoteProcessor:
        built = RemoteProcessor(
            target,
            compute,
            name=name,
            cpus=cpus,
            ttl=ttl,
            workers=workers,
            limit=limit,
            retry=Retry(times=retries, backoff=backoff),
        )
        built.errors = errors
        return built

    if function is not None:
        return build(function)
    return build
