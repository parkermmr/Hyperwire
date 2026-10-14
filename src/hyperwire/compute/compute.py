"""
Compute is the structural contract for remote job execution.

Any object with these methods can back a RemoteProcessor. The
asteroidbelt Compute satisfies this protocol without importing
hyperwire, keeping the two projects fully separable.
"""

from typing import Any, Callable, Protocol

from hyperwire.compute.compute_result import ComputeResult


class Compute(Protocol):
    """
    Named job submission with poll back.
    """

    async def submit(
        self,
        function: Callable,
        name: str,
        arguments: tuple = (),
        keywords: dict[str, Any] | None = None,
        cpus: float = 1.0,
        ttl: float = 3600.0,
    ) -> bool: ...

    async def poll(self, name: str) -> ComputeResult | None: ...
