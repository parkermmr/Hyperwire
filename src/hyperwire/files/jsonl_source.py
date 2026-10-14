"""
JsonlSource reads one message per line from a jsonl file.
"""

import json
from pathlib import Path
from typing import AsyncIterator

from hyperwire.models.message import Message
from hyperwire.transport.dataset import location
from hyperwire.transport.source import Source


class JsonlSource(Source):
    """
    File source for local runs and replay.
    """

    def __init__(self, path: str, name: str = "input") -> None:
        super().__init__(name=name)
        self.path = Path(path)

    def datasets(self) -> list[str]:
        """The file read, by where it resolves."""
        return [location(self.path)]

    async def events(self) -> AsyncIterator[Message]:
        with self.path.open() as handle:
            for line in handle:
                line = line.strip()
                if line:
                    yield Message(content=json.loads(line))
