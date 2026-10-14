"""
hyperwire - an event based pipeline framework.

Graphs are wired with the >> operator or assembled programmatically,
and run by a Pipeline. Every node consumes messages from its own
queue with configurable parallel workers. Processors are stateless
by default and become stateful by declaring a state parameter, or
resourceful by declaring a context parameter.

Usage:

    pipeline = Pipeline(name="demo")

    @processor(workers=4)
    async def step(message, state):
        return message

    source >> step >> [when(kind="a") >> fast, otherwise >> slow] >> sink
    await pipeline.run(source)

or without operators:

    pipeline.assemble(source, step, sink)
    pipeline.sources(source)
    await pipeline.run()
"""

from hyperwire.compute.compute import Compute
from hyperwire.compute.compute_result import ComputeResult
from hyperwire.graph.branch import Branch
from hyperwire.graph.component import Component, component
from hyperwire.graph.node import Node
from hyperwire.graph.otherwise import otherwise
from hyperwire.graph.retry import Retry
from hyperwire.graph.terminate import terminate
from hyperwire.graph.when import when
from hyperwire.graph.wire import Wire
from hyperwire.models.message import Message
from hyperwire.runtime.context import Context
from hyperwire.runtime.experimental import free_threaded
from hyperwire.runtime.pipeline import Pipeline
from hyperwire.runtime.processor import Processor, processor
from hyperwire.runtime.provenance import Provenance
from hyperwire.runtime.remote_processor import RemoteProcessor, remote
from hyperwire.runtime.reservoir import Reservoir
from hyperwire.state.file_state import FileState
from hyperwire.state.memory_state import MemoryState
from hyperwire.state.state import State
from hyperwire.transport.broker import Broker
from hyperwire.transport.listener import Listener
from hyperwire.transport.memory_sink import MemorySink
from hyperwire.transport.publisher import Publisher
from hyperwire.transport.queue_source import QueueSource
from hyperwire.transport.sink import Sink
from hyperwire.transport.source import Source

__all__ = [
    "Branch",
    "Broker",
    "Component",
    "Compute",
    "ComputeResult",
    "Context",
    "FileState",
    "Listener",
    "MemorySink",
    "MemoryState",
    "Message",
    "Node",
    "Pipeline",
    "Processor",
    "Provenance",
    "Publisher",
    "QueueSource",
    "RemoteProcessor",
    "Reservoir",
    "Retry",
    "Sink",
    "Source",
    "State",
    "Wire",
    "component",
    "free_threaded",
    "otherwise",
    "processor",
    "remote",
    "terminate",
    "when",
]
