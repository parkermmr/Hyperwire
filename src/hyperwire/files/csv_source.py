"""
CsvSource reads one message per row from a csv file.
"""

import csv
from pathlib import Path
from typing import AsyncIterator

from hyperwire.models.message import Message
from hyperwire.transport.dataset import location
from hyperwire.transport.source import Source


class CsvSource(Source):
    """
    Single-run file source: each row becomes a message whose content
    is the row as a dict keyed by the header. Values arrive as
    strings; a processor downstream shapes them.
    """

    def __init__(
        self,
        path: str,
        delimiter: str = ",",
        encoding: str = "utf-8",
        name: str = "input",
    ) -> None:
        super().__init__(name=name)
        self.path = Path(path)
        self.delimiter = delimiter
        self.encoding = encoding

    def datasets(self) -> list[str]:
        """The file read, by where it resolves."""
        return [location(self.path)]

    async def events(self) -> AsyncIterator[Message]:
        with self.path.open(newline="", encoding=self.encoding) as handle:
            for row in csv.DictReader(handle, delimiter=self.delimiter):
                yield Message(
                    content=dict(row),
                    metadata={"source": "csv", "path": str(self.path)},
                )
