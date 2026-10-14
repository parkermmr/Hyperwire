"""
TaggedStream says which node a line of output came from.

Wrapped around stdout and stderr, it prefixes every line written while
a node's worker is running with [node:<name>], so a log viewer can
filter one node's prints, warnings, and tracebacks out of a whole
flow's output. Lines written outside any node pass through untouched.
Output from separate processes (process mode, remote compute) is not
tagged: the marker lives in the worker's context, not the process.
"""

import sys

from hyperwire.graph.node import running

PREFIX = "[node:{}] "


class TaggedStream:
    """A text stream that prefixes node output, line by line."""

    def __init__(self, stream) -> None:
        self.stream = stream
        self.fresh = True

    def write(self, text: str) -> int:
        """Write text, tagging each line that starts inside a node."""
        name = running.get()
        if not text:
            return 0
        if name is None:
            self.fresh = text.endswith("\n")
            return self.stream.write(text)
        out = []
        for piece in text.splitlines(keepends=True):
            if self.fresh:
                out.append(PREFIX.format(name))
            out.append(piece)
            self.fresh = piece.endswith("\n")
        self.stream.write("".join(out))
        return len(text)

    def __getattr__(self, attribute: str):
        return getattr(self.stream, attribute)


def tag_output() -> None:
    """Wrap stdout and stderr once; calling it again changes nothing."""
    if not isinstance(sys.stdout, TaggedStream):
        sys.stdout = TaggedStream(sys.stdout)
    if not isinstance(sys.stderr, TaggedStream):
        sys.stderr = TaggedStream(sys.stderr)
