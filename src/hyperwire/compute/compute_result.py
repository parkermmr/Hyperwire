"""
ComputeResult is the shape a compute backend returns for a job.
"""

from typing import Any, Protocol


class ComputeResult(Protocol):
    """
    Structural contract for a terminal job outcome.
    """

    value: Any
    error: str | None

    def successful(self) -> bool: ...
