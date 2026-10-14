"""
dataset spells the uris nodes name their outside data by.

Lineage is only as good as its join key: two flows touching the same
topic must spell it the same way, whatever order their brokers were
listed in, and two flows touching the same file must agree on its
path. These functions are that spelling, written once.
"""

from pathlib import Path


def topic(bootstrap: str, name: str) -> str:
    """A kafka topic on a cluster, brokers sorted so any order agrees."""
    brokers = ",".join(sorted(part.strip() for part in bootstrap.split(",") if part.strip()))
    return f"kafka://{brokers}/{name}"


def location(path) -> str:
    """A file or folder, as the absolute file uri of where it resolves."""
    return Path(path).resolve().as_uri()
