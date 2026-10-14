"""
Node is the base unit of a pipeline graph.

Every node owns an inbox queue and runs one or more workers that
consume from it, so parallel processing is native to every node.
Graphs are wired at definition time with the >> operator, or
programmatically with upstream and downstream.

The running context variable names the node whose worker is active,
for anything that wants to say which node a line of output came from
(TaggedStream).
"""

import asyncio
import logging
from contextvars import ContextVar
from traceback import format_exception
from typing import Callable

from hyperwire.graph.caption import caption
from hyperwire.graph.retry import Retry
from hyperwire.models.message import Message

logger = logging.getLogger(__name__)

running: ContextVar[str | None] = ContextVar("hyperwire_node", default=None)

Predicate = Callable[[Message], bool]


class Node:
    """
    A runnable graph node with an inbox, workers, and subscribers.

    Subclasses override handle to process messages. Returning None
    from handle stops propagation for that message.

    The inbox is bounded by capacity so a fast upstream cannot flood
    memory; a full inbox blocks the sender, which is the pipeline's
    backpressure. Pausing closes the gate workers wait on, so it
    takes effect between messages, never mid handle.

    A failing handle is retried according to the node's retry policy
    and only then routed to the error node, so the error route sees
    messages that exhausted their attempts, never transient blips.

    Component is the innermost component the node was built into, or
    None; it is description only and never changes how messages move.
    """

    def __init__(
        self,
        name: str = "",
        workers: int = 1,
        capacity: int = 1024,
        retry: Retry | None = None,
    ) -> None:
        self.name = name or type(self).__name__
        self.workers = workers
        self.inbox: asyncio.Queue = asyncio.Queue(maxsize=capacity)
        self.subscriptions: list[tuple[Predicate | None, "Node", bool]] = []
        self.errors: "Node | None" = None
        self.retry = retry if retry is not None else Retry()
        self.recorder = None
        self.component = None
        self.working = 0
        self.processed = 0
        self.failed = 0
        self.retried = 0
        self.executed = 0
        self.turbo = False
        self.last_error: str | None = None
        self.fault: BaseException | None = None
        self.tasks: list[asyncio.Task] = []
        self.gate = asyncio.Event()
        self.gate.set()

    def __rshift__(self, other):
        from hyperwire.graph.wire import Wire

        return Wire(heads=[self], tails=[self]) >> other

    def upstream(self, *items) -> "Node":
        """
        Wire each item's tails into this node and return self.
        """
        from hyperwire.graph.wire import Wire

        for item in items:
            Wire.of(item) >> self
        return self

    def downstream(self, *items) -> "Node":
        """
        Wire this node into each item's heads and return self.
        """
        from hyperwire.graph.branch import Branch
        from hyperwire.graph.wire import Wire

        for item in items:
            wire = Wire(heads=[self], tails=[self])
            if isinstance(item, Branch):
                wire >> [item]
            else:
                wire >> item
        return self

    def subscribe(
        self,
        node: "Node",
        predicate: Predicate | None = None,
        fallback: bool = False,
    ) -> None:
        """
        Register a downstream node. A predicate of None means the node
        always receives; each predicated node receives when its own
        predicate matches; a fallback node receives only when no
        predicate matched.
        """
        self.subscriptions.append((predicate, node, fallback))

    def failing(self, target) -> "Node":
        """
        Route messages this node could not handle into target, and
        return self so the call chains inside a graph expression.
        """
        from hyperwire.graph.wire import Wire

        self.errors = Wire.of(target).heads[0]
        return self

    def attempts(self, times: int = 0, backoff: float = 0.0, **options) -> "Node":
        """
        Set this node's retry policy and return self.
        """
        self.retry = Retry(times=times, backoff=backoff, **options)
        return self

    def linked(self) -> list["Node"]:
        """
        Nodes reachable from here outside graph edges, such as
        listeners a publisher delivers to. Discovery follows these.
        """
        return [self.errors] if self.errors is not None else []

    async def emit(self, message: Message) -> None:
        """
        Deliver a message to every unconditional subscriber, to every
        conditional subscriber whose predicate matches, and to the
        fallback subscribers only when no predicate matched.
        """
        recipients = [
            node
            for predicate, node, fallback in self.subscriptions
            if predicate is None and not fallback
        ]
        matched = [
            node
            for predicate, node, fallback in self.subscriptions
            if predicate is not None and predicate(message)
        ]
        if not matched:
            matched = [
                node for predicate, node, fallback in self.subscriptions if fallback
            ]
        recipients += matched
        if self.recorder is not None:
            self.recorder.observe(
                self.name,
                message,
                edge=",".join(node.name for node in recipients),
            )
        for node in recipients:
            outgoing = message.copy(deep=not self.turbo) if len(recipients) > 1 else message
            await node.inbox.put(outgoing)

    async def handle(self, message: Message) -> Message | None:
        """
        Process one message.
        """
        return message

    def start(self) -> None:
        """
        Launch this node's worker tasks.
        """
        self.tasks = [asyncio.create_task(self.work()) for _ in range(self.workers)]

    async def stop(self) -> None:
        """
        Cancel and await this node's worker tasks.
        """
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks = []

    @property
    def paused(self) -> bool:
        return not self.gate.is_set()

    def pause(self) -> None:
        """
        Stop consuming the inbox. Queued and upstream messages wait,
        so pausing a node backpressures everything above it.
        """
        self.gate.clear()

    def resume(self) -> None:
        self.gate.set()

    async def attempt(self, message: Message) -> Message | None:
        """
        Handle one message, retrying per the retry policy. Raises the
        last error once the attempts are spent.
        """
        for attempt in range(self.retry.times + 1):
            try:
                self.executed += 1
                return await self.handle(message)
            except Exception as error:
                spent = attempt >= self.retry.times
                if spent or not self.retry.catches(error):
                    raise
                self.retried += 1
                self.last_error = repr(error)
                logger.warning(
                    "Node %s retrying message %s after %r",
                    self.name,
                    message.id,
                    error,
                )
                pause = self.retry.wait(attempt + 1)
                if pause > 0:
                    await asyncio.sleep(pause)
        return None

    async def work(self) -> None:
        """
        Consume the inbox forever, handling one message at a time.
        The sleep keeps the event loop responsive even when neither
        get nor handle ever truly suspends, so a big backlog cannot
        starve everything else sharing the loop. The worker marks its
        task as this node's, so output it causes can be told apart.
        """
        running.set(self.name)
        while True:
            await asyncio.sleep(0)
            await self.gate.wait()
            message = await self.inbox.get()
            self.working += 1
            try:
                result = await self.attempt(message)
                if result is not None:
                    await self.emit(result)
                self.processed += 1
            except Exception as error:
                self.failed += 1
                self.last_error = repr(error)
                self.fault = error
                logger.exception("Node %s failed on message %s", self.name, message.id)
                if self.errors is not None:
                    await self.errors.inbox.put(message)
            finally:
                self.working -= 1
                self.inbox.task_done()

    def idle(self) -> bool:
        """
        True when this node has nothing queued or in flight.
        """
        return self.inbox.empty() and self.working == 0

    def datasets(self) -> list[str]:
        """
        The outside data this node reads or writes, as uris: a source
        names what it reads, a sink what it writes, and anything in
        between usually nothing. A platform joins flows on these, so
        two flows naming the same topic are drawn as feeding each other
        without anyone declaring it.
        """
        return []

    def describe(self) -> dict:
        """
        Serializable snapshot of this node and its outgoing edges.
        """
        return {
            "name": self.name,
            "type": type(self).__name__,
            "datasets": self.datasets(),
            "workers": self.workers,
            "queued": self.inbox.qsize(),
            "processed": self.processed,
            "failed": self.failed,
            "retried": self.retried,
            "executed": self.executed,
            "paused": self.paused,
            "last_error": self.last_error,
            "last_trace": self.trace(),
            "retry": self.retry.describe(),
            "edges": [
                {
                    "target": node.name,
                    "conditional": predicate is not None or fallback,
                    "fallback": fallback,
                    "kind": "conditional"
                    if predicate is not None
                    else "fallback"
                    if fallback
                    else "flow",
                    "label": caption(predicate),
                }
                for predicate, node, fallback in self.subscriptions
            ]
            + (
                [
                    {
                        "target": self.errors.name,
                        "conditional": False,
                        "fallback": False,
                        "kind": "error",
                        "label": "",
                    }
                ]
                if self.errors is not None
                else []
            ),
        }

    def trace(self) -> str | None:
        """
        The full traceback of the last failure, formatted when asked
        rather than on every failure, so a failure storm costs one
        reference instead of one formatting per message.
        """
        if self.fault is None:
            return None
        return "".join(format_exception(self.fault))

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.name!r})"
