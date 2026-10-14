"""
Pipeline owns processors, shared services, and the run loop.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count
from typing import Callable

from hyperwire.compute.compute import Compute
from hyperwire.graph.component import sections
from hyperwire.graph.node import Node
from hyperwire.graph.retry import Retry
from hyperwire.graph.wire import Wire
from hyperwire.runtime.context import Context
from hyperwire.runtime.experimental import free_threaded
from hyperwire.runtime.processor import Processor
from hyperwire.runtime.provenance import Provenance
from hyperwire.runtime.remote_processor import RemoteProcessor
from hyperwire.state.memory_state import MemoryState
from hyperwire.state.state import State
from hyperwire.transport.source import Source

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Builds processors and runs a wired graph to completion.

    Graphs are wired with >> or assembled programmatically:

        pipeline = Pipeline(name="demo")

        @pipeline.processor(workers=4)
        async def step(message, state):
            return message

        pipeline.sources(source)
        pipeline.assemble(source, step, sink)
        await pipeline.run()

    State, context, and compute attach through their accessors and
    reach every processor at run time. Sources commit their offsets
    only after a clean drain, so an errored run never commits.
    """

    def __init__(
        self,
        name: str,
        state: State | None = None,
        errors: Node | None = None,
        context: Context | None = None,
        compute: Compute | None = None,
    ) -> None:
        self.name = name
        self.shared = state if state is not None else MemoryState()
        self.pile = context if context is not None else Context()
        self.backend = compute
        self.errors = errors
        self.recorder: Provenance | None = None
        self.processors: dict[str, Processor] = {}
        self.inputs: list[Source] = []
        self.outputs: list[Node] = []
        self.included: list[Node] = []
        self.turbo = False
        self.experimental = False
        self.executor: ThreadPoolExecutor | None = None
        self.nodes: list[Node] = []
        self.targets: list[Source] = []

    def state(self, value: State | None = None) -> State:
        """
        The shared state; attaches value first when given.
        """
        if value is not None:
            self.shared = value
        return self.shared

    def context(self, value: Context | None = None) -> Context:
        """
        The shared context; attaches value first when given.
        """
        if value is not None:
            self.pile = value
        return self.pile

    def provenance(self, value: "Provenance | None" = None) -> "Provenance | None":
        """
        The attached provenance recorder; attaches value first when
        given. Every node observes through it from the next emit, so
        recording can start and stop on a running graph.
        """
        if value is not None:
            self.recorder = value
        for node in self.nodes:
            node.recorder = self.recorder
        return self.recorder

    def compute(self, value: Compute | None = None) -> Compute | None:
        """
        The attached compute backend; attaches value first when
        given. Remote processors built without a compute use it.
        """
        if value is not None:
            self.backend = value
        return self.backend

    def processor(
        self,
        name: str = "",
        workers: int = 1,
        mode: str = "auto",
        retries: int = 0,
        backoff: float = 0.0,
        errors: Node | None = None,
    ) -> Callable:
        """
        Register a function as a processor node. Retries re-run the
        function on failure before the message takes the error route,
        which is this node's errors argument or the pipeline's.
        """

        def register(function: Callable) -> Processor:
            built = Processor(
                function,
                name=name,
                state=self.shared,
                workers=workers,
                mode=mode,
                retry=Retry(times=retries, backoff=backoff),
            )
            built.errors = errors if errors is not None else self.errors
            self.processors[built.name] = built
            return built

        return register

    def remote(
        self,
        compute: Compute | None = None,
        name: str = "",
        cpus: float = 1.0,
        ttl: float = 3600.0,
        workers: int = 4,
        limit: int = 32,
        retries: int = 0,
        backoff: float = 0.0,
        errors: Node | None = None,
    ) -> Callable:
        """
        Register a function that runs each message as a named job on
        a compute backend: this one when given, else the pipeline's,
        else nowhere, in which case it runs locally.
        """

        def register(function: Callable) -> RemoteProcessor:
            built = RemoteProcessor(
                function,
                compute,
                name=name,
                cpus=cpus,
                ttl=ttl,
                workers=workers,
                limit=limit,
                retry=Retry(times=retries, backoff=backoff),
            )
            built.errors = errors if errors is not None else self.errors
            self.processors[built.name] = built
            return built

        return register

    def assemble(self, *items) -> Wire:
        """
        Chain items left to right, exactly as >> would, and include
        the result in the pipeline. Lists fan out as branches.
        """
        if not items:
            raise ValueError("assemble() needs at least one item")
        wire = Wire.of(items[0])
        for item in items[1:]:
            wire = wire >> item
        self.included.extend(wire.heads)
        return wire

    def include(self, *items) -> "Pipeline":
        """
        Register graph sections discovery cannot reach from a source,
        such as listener chains.
        """
        for item in items:
            self.included.extend(Wire.of(item).heads)
        return self

    def sources(self, *sources: Source) -> "Pipeline":
        """
        Register sources; run() without an argument pumps them all.
        """
        self.inputs.extend(sources)
        return self

    def sinks(self, *sinks: Node) -> "Pipeline":
        """
        Register sinks so discovery and description always see them.
        """
        self.outputs.extend(sinks)
        return self

    def scale(self, workers: int) -> "Pipeline":
        """
        Set the worker count of every processor in the graph, whether
        registered here or joined by wiring.
        """
        for node in self.graph():
            if isinstance(node, (Processor, RemoteProcessor)):
                node.workers = workers
        return self

    def performance(self, enabled: bool = True, experimental: bool = False) -> "Pipeline":
        """
        Trade resources and copy safety for speed: fan outs copy
        shallowly and sync processors share a wide thread pool. The
        experimental flag targets free threaded builds, where those
        threads run truly parallel; it can be less stable.
        """
        self.turbo = enabled
        self.experimental = enabled and experimental
        return self

    def pause(self) -> None:
        """
        Pause every node in the graph, sources included.
        """
        for node in self.graph():
            node.pause()

    def resume(self) -> None:
        for node in self.graph():
            node.resume()

    def stop(self) -> None:
        """
        Ask the running sources to stop pumping; run() then drains
        what is in flight and shuts down cleanly.
        """
        for source in self.targets or self.inputs:
            source.stop_pumping()

    def graph(self) -> list[Node]:
        """
        The running node set, or a fresh discovery when not running.
        """
        return self.nodes if self.nodes else self.discover()

    def describe(self, nodes: list[Node] | None = None) -> dict:
        """
        Serializable snapshot of the graph, or of the given nodes.

        Each node carries the component it sits in and the path of
        components down to it, outermost first, both None and empty
        outside any. Components are listed parents first. Their names
        are their labels joined down the nesting with a slash and
        suffixed when two would collide, so one build always
        describes the same way.
        """
        nodes = nodes if nodes is not None else self.graph()
        keys: dict[int, str] = {}
        components = []
        for item in sections(nodes):
            parent = keys[id(item.parent)] if item.parent is not None else None
            base = f"{parent}/{item.name}" if parent is not None else item.name
            key, number = base, 1
            while key in keys.values():
                number += 1
                key = f"{base}-{number}"
            keys[id(item)] = key
            components.append({"name": key, "label": item.name, "parent": parent})
        described = []
        for node in nodes:
            lineage = node.component.lineage() if node.component is not None else []
            path = [keys[id(item)] for item in lineage]
            snapshot = node.describe()
            snapshot["component"] = path[-1] if path else None
            snapshot["path"] = path
            described.append(snapshot)
        return {"name": self.name, "nodes": described, "components": components}

    async def run(self, source: Source | None = None) -> None:
        """
        Start every node, pump the sources dry, drain in flight
        messages, commit the sources, then stop everything.
        """
        targets = [source] if source is not None else list(self.inputs)
        if not targets:
            raise ValueError("run() needs a source argument or registered sources")
        self.targets = targets
        nodes = self.discover(*targets)
        self.bind(nodes)
        await self.shared.start()
        for node in nodes:
            node.start()
        try:
            await asyncio.gather(*(target.run() for target in targets))
            await self.drain(nodes)
            for target in targets:
                target.commit()
        finally:
            for node in nodes:
                await node.stop()
            await self.shared.stop()
            if self.executor is not None:
                self.executor.shutdown(wait=False)
                self.executor = None
            self.nodes = []
            self.targets = []

    def bind(self, nodes: list[Node]) -> None:
        """
        Attach shared services and performance settings to the graph
        just before it starts, so late attachments still land. Every
        processor reached counts, so a free processor wired in with >>
        joins the pipeline here.
        """
        if self.experimental and not free_threaded():
            logger.warning(
                "Pipeline %s: experimental mode without a free threaded "
                "interpreter; thread parallelism stays GIL bound",
                self.name,
            )
        if self.turbo and self.executor is None:
            self.executor = ThreadPoolExecutor(max_workers=max(32, (cpu_count() or 1) * 4))
        for node in nodes:
            node.turbo = self.turbo
            node.recorder = self.recorder
        handling = self.reach(self.errors)
        for processor in nodes:
            joined = isinstance(processor, (Processor, RemoteProcessor))
            if joined and processor.errors is None and id(processor) not in handling:
                processor.errors = self.errors
            if isinstance(processor, RemoteProcessor):
                processor.resolve(self.backend)
            elif isinstance(processor, Processor):
                processor.state = self.shared
                processor.context = self.pile
                processor.executor = self.executor
        self.nodes = nodes

    def reach(self, start: Node | None) -> set[int]:
        """
        The ids of start and every node downstream of it. The error
        handling chain is this set for the pipeline's error node, and
        must never route its own failures back into itself.
        """
        pending = [start] if start is not None else []
        seen: set[int] = set()
        while pending:
            node = pending.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            pending.extend(target for predicate, target, fallback in node.subscriptions)
        return seen

    def discover(self, *sources: Source) -> list[Node]:
        """
        Collect every node reachable from the sources, the registered
        processors, sinks, inclusions, and the error node. Broker
        links count as reachability, so listeners are found too.
        """
        pending: list[Node] = [
            *sources,
            *self.inputs,
            *self.processors.values(),
            *self.included,
            *self.outputs,
        ]
        if self.errors is not None:
            pending.append(self.errors)
        found: list[Node] = []
        seen: set[int] = set()
        while pending:
            node = pending.pop()
            if id(node) in seen:
                continue
            seen.add(id(node))
            found.append(node)
            pending.extend(target for predicate, target, fallback in node.subscriptions)
            pending.extend(node.linked())
        return found

    async def drain(self, nodes: list[Node]) -> None:
        """
        Wait until every inbox is empty and every worker is idle.
        """
        while True:
            for node in nodes:
                await node.inbox.join()
            if all(node.idle() for node in nodes):
                return
            await asyncio.sleep(0.01)
