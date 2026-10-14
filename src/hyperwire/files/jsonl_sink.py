"""
JsonlSink appends one line per message to a jsonl file.
"""

import json
from pathlib import Path

from hyperwire.models.message import Message
from hyperwire.transport.dataset import location
from hyperwire.transport.sink import Sink


class JsonlSink(Sink):
    """
    File sink for outputs and error rows. The file stays open across
    messages and every line is flushed, so readers see progress
    without paying an open and close per message.
    """

    def __init__(self, path: str, name: str = "output") -> None:
        super().__init__(name=name)
        self.path = Path(path)
        self.stream = None

    def datasets(self) -> list[str]:
        """The file written, by where it resolves."""
        return [location(self.path)]

    def start(self) -> None:
        """
        Truncate at deploy, not at construction, so building a flow
        for a check never touches a previous run's output.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text("")
        super().start()

    async def deliver(self, message: Message) -> None:
        if self.stream is None:
            self.stream = self.path.open("a")
        self.stream.write(json.dumps(message.content, default=str) + "\n")
        self.stream.flush()

    async def stop(self) -> None:
        await super().stop()
        if self.stream is not None:
            self.stream.close()
            self.stream = None
