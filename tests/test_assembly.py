"""
Programmatic graph assembly: assemble, include, upstream and
downstream, components, and the pipeline accessors.
"""

import asyncio

from hyperwire import (
    Context,
    MemorySink,
    MemoryState,
    Message,
    Pipeline,
    QueueSource,
    component,
    otherwise,
    when,
)


def build(name="assembly"):
    return Pipeline(name=name), QueueSource(), MemorySink()


async def pump(pipeline, source, contents):
    for content in contents:
        await source.push(Message(content=content))
    await source.close()
    await pipeline.run(source)


def test_assemble_runs_a_chain():
    pipeline, source, sink = build()

    @pipeline.processor()
    def double(message):
        message.content *= 2
        return message

    pipeline.assemble(source, double, sink)
    asyncio.run(pump(pipeline, source, [1, 2, 3]))
    assert sorted(message.content for message in sink.messages) == [2, 4, 6]


def test_upstream_and_downstream_wire_nodes():
    pipeline, source, sink = build()

    @pipeline.processor()
    def tag(message):
        message.metadata["seen"] = True
        return message

    tag.upstream(source)
    tag.downstream(sink)
    asyncio.run(pump(pipeline, source, ["a"]))
    assert sink.messages[0].metadata["seen"] is True


def test_component_chains_and_scales():
    pipeline, source, sink = build()

    @pipeline.processor()
    def first(message):
        message.content += "-first"
        return message

    @pipeline.processor()
    def second(message):
        message.content += "-second"
        return message

    section = component(first, second, name="enrich")
    section.scale(3)
    assert first.workers == 3 and second.workers == 3
    section.upstream(source)
    section.downstream(sink)
    asyncio.run(pump(pipeline, source, ["go"]))
    assert sink.messages[0].content == "go-first-second"



def test_component_middle_and_fan_out_nodes_are_members():
    pipeline, source, sink = build()

    @pipeline.processor()
    def head(message):
        return message

    @pipeline.processor()
    def middle(message):
        return message

    @pipeline.processor()
    def fast(message):
        return message

    @pipeline.processor()
    def slow(message):
        return message

    section = component(head >> middle, [when(kind="a") >> fast, otherwise >> slow])
    assert set(section.members) == {head, middle, fast, slow}
    section.scale(2)
    assert middle.workers == 2 and slow.workers == 2


def test_describe_reports_nested_component_membership():
    pipeline, source, sink = build()

    @pipeline.processor()
    def parse(message):
        return message

    @pipeline.processor()
    def check(message):
        return message

    @pipeline.processor()
    def enrich(message):
        return message

    inner = component(parse, check, name="validate")
    outer = component(inner, enrich, name="ingest")
    assert inner.parent is outer and outer.parent is None
    pipeline.assemble(source, outer, sink)
    description = pipeline.describe(pipeline.discover(source))
    assert description["components"] == [
        {"name": "ingest", "label": "ingest", "parent": None},
        {"name": "ingest/validate", "label": "validate", "parent": "ingest"},
    ]
    nodes = {entry["name"]: entry for entry in description["nodes"]}
    assert nodes["parse"]["component"] == "ingest/validate"
    assert nodes["parse"]["path"] == ["ingest", "ingest/validate"]
    assert nodes["enrich"]["component"] == "ingest"
    assert nodes["enrich"]["path"] == ["ingest"]
    assert nodes["source"]["component"] is None
    assert nodes["source"]["path"] == []
    assert nodes["parse"]["edges"][0]["target"] == "check"


def test_describe_keeps_colliding_component_names_apart():
    pipeline, source, sink = build()

    @pipeline.processor()
    def first(message):
        return message

    @pipeline.processor()
    def second(message):
        return message

    pipeline.assemble(source, component(first), component(second), sink)
    description = pipeline.describe()
    names = sorted(entry["name"] for entry in description["components"])
    assert names == ["component", "component-2"]
    owners = {entry["name"]: entry["component"] for entry in description["nodes"]}
    assert owners["first"] != owners["second"]
    assert description["name"] == "assembly"


def test_sources_and_sinks_registration_runs_without_argument():
    pipeline, source, sink = build()

    @pipeline.processor()
    def identity(message):
        return message

    pipeline.sources(source)
    pipeline.sinks(sink)
    pipeline.assemble(source, identity, sink)

    async def run():
        await source.push(Message(content=1))
        await source.close()
        await pipeline.run()

    asyncio.run(run())
    assert len(sink.messages) == 1


def test_state_context_compute_accessors():
    pipeline = Pipeline(name="accessors")
    state = MemoryState()
    context = Context(flavor="salt")
    assert pipeline.state(state) is state
    assert pipeline.state() is state
    assert pipeline.context(context) is context
    assert pipeline.context().flavor == "salt"
    marker = object()
    assert pipeline.compute(marker) is marker
    assert pipeline.compute() is marker


def test_context_reaches_processors():
    pipeline, source, sink = build()
    pipeline.context().set("suffix", "!")

    @pipeline.processor()
    def shout(message, context):
        message.content += context.suffix
        return message

    pipeline.assemble(source, shout, sink)
    asyncio.run(pump(pipeline, source, ["hey"]))
    assert sink.messages[0].content == "hey!"


def test_context_resolve_by_type():
    context = Context(number=7, text="seven")
    assert context.resolve(int) == 7
    assert context.resolve(dict) is None
