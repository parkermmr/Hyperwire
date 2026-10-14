"""
DirectorySource watches a directory and emits one message per file.
"""

import asyncio
from pathlib import Path
from typing import AsyncIterator

from hyperwire.models.message import Message
from hyperwire.transport.dataset import location as uri
from hyperwire.transport.source import Source


class DirectorySource(Source):
    """
    A drop-directory as a queue: every new file matching the
    extension filter becomes one message. By default its content is
    the file's coordinates ({path, name, size, modified}) and
    downstream stages open the file themselves; with read=True the
    content is the file's bytes, exactly as on disk, and the
    coordinates move to metadata. Nothing assumes a format - decode
    in a processor.

    A file is emitted only once its size and mtime survive one whole
    poll interval unchanged, so half-written files never enter the
    flow. Files present at start are emitted too unless existing is
    False.
    """

    def __init__(
        self,
        path: str,
        extensions: list[str] | None = None,
        interval: float = 2.0,
        recursive: bool = False,
        existing: bool = True,
        read: bool = False,
        name: str = "watch",
    ) -> None:
        super().__init__(name=name)
        self.path = Path(path)
        self.extensions = {
            suffix.lower() if suffix.startswith(".") else f".{suffix.lower()}"
            for suffix in (extensions or [])
        }
        self.interval = float(interval)
        self.recursive = bool(recursive)
        self.existing = bool(existing)
        self.read = bool(read)
        self.closed = False

    def datasets(self) -> list[str]:
        """The folder watched, by where it resolves."""
        return [uri(self.path)]

    def matches(self, location: Path) -> bool:
        """Whether one file passes the extension filter."""
        return not self.extensions or location.suffix.lower() in self.extensions

    def scan(self) -> dict[str, tuple[int, float]]:
        """Every matching file as {path: (size, mtime)}."""
        pattern = "**/*" if self.recursive else "*"
        found: dict[str, tuple[int, float]] = {}
        for location in self.path.glob(pattern):
            if location.is_file() and self.matches(location):
                stat = location.stat()
                found[str(location)] = (stat.st_size, stat.st_mtime)
        return found

    def message(self, location: str, size: int, modified: float) -> Message:
        """The event one settled file becomes."""
        coordinates = {
            "path": str(Path(location).resolve()),
            "name": Path(location).name,
            "size": size,
            "modified": modified,
        }
        metadata = {"source": "directory", "directory": str(self.path)}
        if self.read:
            return Message(
                content=Path(location).read_bytes(),
                metadata={**metadata, **coordinates},
            )
        return Message(content=coordinates, metadata=metadata)

    async def events(self) -> AsyncIterator[Message]:
        """Poll until close(), yielding each file exactly once."""
        emitted: set[str] = set()
        if not self.existing:
            emitted.update(self.scan())
        pending: dict[str, tuple[int, float]] = {}
        while not self.closed:
            for location, stamp in self.scan().items():
                if location in emitted:
                    continue
                if pending.get(location) == stamp:
                    del pending[location]
                    emitted.add(location)
                    yield self.message(location, *stamp)
                else:
                    pending[location] = stamp
            await asyncio.sleep(self.interval)

    async def close(self) -> None:
        """Stop watching; the pipeline drains what was emitted."""
        self.closed = True
