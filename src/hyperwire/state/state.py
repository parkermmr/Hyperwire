"""
State is the shared key value store available to stateful
processors.
"""

import abc
import asyncio
from collections import defaultdict
from typing import Any


class State(abc.ABC):
    """
    Async key value interface shared by all backends.

    Expired keys are always dropped lazily when read or listed. Pass
    sweep in seconds to also run a watchdog that actively purges them
    while the pipeline runs. Backends may override increment and lock
    with native implementations when they have them.
    """

    def __init__(self, sweep: float | None = None) -> None:
        self.locks: defaultdict = defaultdict(asyncio.Lock)
        self.sweep = sweep
        self.watchdog: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Open the backend and launch the watchdog when configured.
        """
        if self.sweep is not None:
            self.watchdog = asyncio.create_task(self.watch())

    async def stop(self) -> None:
        """
        Stop the watchdog and close the backend.
        """
        if self.watchdog is not None:
            self.watchdog.cancel()
            await asyncio.gather(self.watchdog, return_exceptions=True)
            self.watchdog = None

    async def watch(self) -> None:
        """
        Periodically purge expired keys. Listing keys drops them.
        """
        while True:
            await asyncio.sleep(self.sweep)
            await self.keys()

    @abc.abstractmethod
    async def get(self, key: str, default: Any = None) -> Any:
        """
        Return the value for key, or default when absent.
        """

    @abc.abstractmethod
    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        """
        Store a value under key. A ttl in seconds makes the key
        expire; expired keys are dropped when read or listed.
        """

    @abc.abstractmethod
    async def delete(self, key: str) -> bool:
        """
        Remove key. Returns True when it existed.
        """

    @abc.abstractmethod
    async def keys(self, pattern: str = "*") -> list[str]:
        """
        Return every key matching the glob pattern.
        """

    def lock(self, key: str) -> asyncio.Lock:
        """
        Cooperative per key lock for read modify write sections.
        """
        return self.locks[key]

    async def increment(self, key: str, amount: int = 1) -> int:
        """
        Atomically add to a counter and return the new value.
        """
        async with self.lock(key):
            value = await self.get(key, 0) + amount
            await self.set(key, value)
            return value
