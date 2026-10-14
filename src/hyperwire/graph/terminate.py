"""
Terminate marks a branch as a dead end for convergence.

A terminated branch still receives messages, it just is not wired
to whatever follows the fan out list. Wire its own downstream
separately if needed.
"""


class Terminate:
    """
    Sentinel used as: branch >> terminate.
    """

    def __repr__(self) -> str:
        return "terminate"


terminate = Terminate()
