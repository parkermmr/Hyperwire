"""
MemoryState keeps state in a process local dictionary.
"""

from fnmatch import fnmatch
from time import time
from typing import Any

from hyperwire.state.state import State


class MemoryState(State):
    """
    Fast in process backend for single process pipelines and tests.
    Expired keys are dropped lazily when read or listed.
    """

    def __init__(self, sweep: float | None = None) -> None:
        super().__init__(sweep=sweep)
        self.store: dict[str, Any] = {}
        self.expiries: dict[str, float] = {}

    async def get(self, key: str, default: Any = None) -> Any:
        if self.expired(key):
            await self.delete(key)
            return default
        return self.store.get(key, default)

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        self.store[key] = value
        if ttl is not None:
            self.expiries[key] = time() + ttl
        else:
            self.expiries.pop(key, None)

    async def delete(self, key: str) -> bool:
        self.expiries.pop(key, None)
        if key in self.store:
            del self.store[key]
            return True
        return False

    async def keys(self, pattern: str = "*") -> list[str]:
        for key in [key for key in self.expiries if self.expired(key)]:
            await self.delete(key)
        return [key for key in self.store if fnmatch(key, pattern)]

    def expired(self, key: str) -> bool:
        expiry = self.expiries.get(key)
        return expiry is not None and expiry <= time()
