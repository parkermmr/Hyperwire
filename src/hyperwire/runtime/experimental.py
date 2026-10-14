"""
Experimental mode helpers for free threaded Python builds.
"""

import sys


def free_threaded() -> bool:
    """
    True when this interpreter runs without the GIL, so threads give
    real parallelism and experimental mode can use them fully.
    """
    probe = getattr(sys, "_is_gil_enabled", None)
    if probe is None:
        return False
    return not probe()
