"""
FileState keeps state as files on disk, one file per key.

Each file holds a serialized envelope of the value and its optional
expiry, so ttls survive restarts.
"""

import json
from fnmatch import fnmatch
from pathlib import Path
from time import time
from typing import Any, Callable
from urllib.parse import quote, unquote

from hyperwire.state.state import State


def encode(value: Any) -> bytes:
    return json.dumps(value).encode()


def decode(data: bytes) -> Any:
    return json.loads(data)


class FileState(State):
    """
    Durable backend that survives restarts.

    Values are stored as JSON by default. For values JSON cannot
    represent, pass another codec, for example dumps=pickle.dumps
    and loads=pickle.loads.
    """

    def __init__(
        self,
        directory: str,
        dumps: Callable[[Any], bytes] = encode,
        loads: Callable[[bytes], Any] = decode,
        sweep: float | None = None,
    ) -> None:
        super().__init__(sweep=sweep)
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.dumps = dumps
        self.loads = loads

    def path(self, key: str) -> Path:
        return self.directory / f"{quote(key, safe='')}.state"

    async def get(self, key: str, default: Any = None) -> Any:
        location = self.path(key)
        if not location.exists():
            return default
        envelope = self.loads(location.read_bytes())
        if envelope["expires"] is not None and envelope["expires"] <= time():
            location.unlink()
            return default
        return envelope["value"]

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        expires = time() + ttl if ttl is not None else None
        self.path(key).write_bytes(self.dumps({"value": value, "expires": expires}))

    async def delete(self, key: str) -> bool:
        location = self.path(key)
        if location.exists():
            location.unlink()
            return True
        return False

    async def keys(self, pattern: str = "*") -> list[str]:
        names = []
        for location in self.directory.glob("*.state"):
            envelope = self.loads(location.read_bytes())
            if envelope["expires"] is not None and envelope["expires"] <= time():
                location.unlink()
                continue
            names.append(unquote(location.stem))
        return [name for name in names if fnmatch(name, pattern)]
