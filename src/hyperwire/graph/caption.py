"""
caption names a routing predicate for anyone drawing the graph.

A conditional edge that only says "when" makes a reader open the code
to learn which messages take it. The predicate usually says it well
enough itself: keyword checks name their fields, a named function its
name, and a lambda its own body.
"""

from ast import Lambda, parse, unparse, walk
from functools import lru_cache
from inspect import getsource
from textwrap import dedent
from typing import Callable


@lru_cache(maxsize=1024)
def spoken(function: Callable) -> str:
    """
    The body of a lambda as written, or empty when the source cannot
    be read or the line holds more than one lambda to choose from.
    Read once per lambda, since graphs are described on every poll.
    """
    try:
        source = dedent(getsource(function)).strip()
    except (OSError, TypeError):
        return ""
    for attempt in (source, source.rstrip(","), f"({source.rstrip(',')})"):
        try:
            tree = parse(attempt)
        except SyntaxError:
            continue
        found = [node for node in walk(tree) if isinstance(node, Lambda)]
        return unparse(found[0].body) if len(found) == 1 else ""
    return ""


def caption(predicate: Callable | None) -> str:
    """
    A short label for a predicate: its own label attribute when it has
    one, a lambda's body, or a function's name.
    """
    if predicate is None:
        return ""
    label = getattr(predicate, "label", "")
    if label:
        return str(label)
    name = getattr(predicate, "__name__", "")
    if name == "<lambda>":
        return spoken(predicate)
    return name
