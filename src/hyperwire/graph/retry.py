"""
Retry is the policy a node applies before a failure becomes an error.
"""

from typing import Iterable


class Retry:
    """
    How many extra attempts a failing handle gets, and how long to
    wait between them.

    Attempts counts retries after the first try, so times=2 means up
    to three calls. Backoff is the first delay in seconds and each
    further wait multiplies it by factor, capped at ceiling. Only
    the listed exception types retry; anything else fails straight
    to the error route.
    """

    def __init__(
        self,
        times: int = 0,
        backoff: float = 0.0,
        factor: float = 2.0,
        ceiling: float = 30.0,
        catching: Iterable[type[BaseException]] = (Exception,),
    ) -> None:
        if times < 0:
            raise ValueError("retry times cannot be negative")
        if backoff < 0:
            raise ValueError("retry backoff cannot be negative")
        if factor < 1:
            raise ValueError("retry factor cannot shrink the wait")
        self.times = times
        self.backoff = backoff
        self.factor = factor
        self.ceiling = ceiling
        self.catching = tuple(catching)

    def catches(self, error: BaseException) -> bool:
        """
        True when this policy is willing to retry the error.
        """
        return isinstance(error, self.catching)

    def wait(self, attempt: int) -> float:
        """
        Seconds to sleep before the given attempt, counting the first
        retry as attempt one.
        """
        if self.backoff <= 0:
            return 0.0
        return min(self.backoff * self.factor ** (attempt - 1), self.ceiling)

    def describe(self) -> dict:
        """
        Serializable snapshot for the graph description.
        """
        return {
            "times": self.times,
            "backoff": self.backoff,
            "factor": self.factor,
            "ceiling": self.ceiling,
        }

    def __repr__(self) -> str:
        return f"Retry(times={self.times}, backoff={self.backoff})"
