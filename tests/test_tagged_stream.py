"""
TaggedStream: output a node causes carries the node's name.
"""

import asyncio
import io

from hyperwire.graph.node import running
from hyperwire.models.message import Message
from hyperwire.runtime.processor import Processor
from hyperwire.runtime.tagged_stream import TaggedStream


def test_lines_written_inside_a_node_are_tagged():
    target = io.StringIO()
    stream = TaggedStream(target)
    stream.write("boot\n")
    token = running.set("clean")
    stream.write("one\ntwo")
    stream.write(" more\n")
    running.reset(token)
    stream.write("after\n")
    assert target.getvalue() == "boot\n[node:clean] one\n[node:clean] two more\nafter\n"


def test_a_worker_marks_its_own_task():
    seen = []

    def remember(message):
        seen.append(running.get())
        return message

    node = Processor(remember, name="remember")

    async def go():
        node.start()
        await node.inbox.put(Message(content=b"x"))
        await node.inbox.join()
        await node.stop()

    asyncio.run(go())
    assert seen == ["remember"]
    assert running.get() is None
