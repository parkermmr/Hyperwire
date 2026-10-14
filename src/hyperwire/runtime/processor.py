"""
Processor wraps a user function as a graph node, and processor is the
decorator that builds one without a pipeline.
"""

import asyncio
import inspect
from concurrent.futures import ProcessPoolExecutor
from contextvars import copy_context
from functools import partial, update_wrapper
from typing import Callable

from hyperwire.graph.node import Node
from hyperwire.graph.retry import Retry
from hyperwire.models.message import Message
from hyperwire.runtime.context import Context
from hyperwire.state.state import State


class Processor(Node):
    """
    Runs a sync or async function for every message.

    A function with a state parameter receives the pipeline state; a
    function with a context parameter receives the pipeline context.
    Sync functions run in a thread so they never block the event
    loop; in process mode each worker runs in its own process for
    true multiprocessing, which requires a plain picklable function
    of the message alone.

    A processor stays callable as the function it wraps, so decorated
    code can be tested or reused outside any graph.
    """

    def __init__(
        self,
        function: Callable,
        name: str = "",
        state: State | None = None,
        workers: int = 1,
        mode: str = "auto",
        context: Context | None = None,
        retry: Retry | None = None,
    ) -> None:
        super().__init__(name=name or function.__name__, workers=workers, retry=retry)
        self.function = function
        self.state = state
        self.context = context
        self.mode = mode
        self.pool: ProcessPoolExecutor | None = None
        self.executor = None
        parameters = inspect.signature(function).parameters
        self.stateful = "state" in parameters
        self.contextual = "context" in parameters
        self.asynchronous = inspect.iscoroutinefunction(function)
        update_wrapper(self, function, updated=())
        if mode not in ("auto", "process"):
            raise ValueError(f"unknown processor mode {mode!r}")
        if mode == "process" and self.asynchronous:
            raise ValueError("process mode requires a sync function")
        if mode == "process" and (self.stateful or self.contextual):
            raise ValueError("process mode cannot inject state or context")

    def __call__(self, *args, **kwargs):
        """
        Call the wrapped function directly, outside any graph.
        """
        return self.function(*args, **kwargs)

    def start(self) -> None:
        if self.mode == "process":
            self.pool = ProcessPoolExecutor(max_workers=self.workers)
        super().start()

    async def stop(self) -> None:
        await super().stop()
        if self.pool is not None:
            self.pool.shutdown(wait=False, cancel_futures=True)
            self.pool = None

    def arguments(self, message: Message) -> tuple:
        """
        Build the call arguments in message, state, context order.
        """
        arguments = [message]
        if self.stateful:
            arguments.append(self.state)
        if self.contextual:
            arguments.append(self.context)
        return tuple(arguments)

    async def handle(self, message: Message) -> Message | None:
        arguments = self.arguments(message)
        if self.asynchronous:
            return await self.function(*arguments)
        loop = asyncio.get_running_loop()
        if self.mode == "process":
            return await loop.run_in_executor(self.pool, self.function, message)
        if self.executor is not None:
            return await loop.run_in_executor(
                self.executor, partial(copy_context().run, self.function, *arguments)
            )
        return await asyncio.to_thread(self.function, *arguments)


def processor(
    function: Callable | None = None,
    name: str = "",
    workers: int = 1,
    mode: str = "auto",
    retries: int = 0,
    backoff: float = 0.0,
    errors: Node | None = None,
):
    """
    Build a processor from a function, bare or with options:

        @processor
        def clean(message): ...

        @processor(workers=4, retries=2)
        async def enrich(message, state): ...

    No pipeline is needed. Wiring it with >> makes it part of whatever
    pipeline runs that graph, which attaches state, context, and its
    error route at run time. It stays callable as a plain function.
    """

    def build(target: Callable) -> Processor:
        built = Processor(
            target,
            name=name,
            workers=workers,
            mode=mode,
            retry=Retry(times=retries, backoff=backoff),
        )
        built.errors = errors
        return built

    if function is not None:
        return build(function)
    return build
