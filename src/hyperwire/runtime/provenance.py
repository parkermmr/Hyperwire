"""
Provenance is the sampled record of what actually moved through a
graph.

Observing a running pipeline is a debugging tool, not a data path:
the recorder keeps a bounded ring of recent messages per node and
drops the rest, so switching it on cannot grow memory without bound
or backpressure the flow it is watching.

A record keeps a bounded snapshot of the payload rather than a
rendering of it, and renders when read. The reader picks a media type
(text/plain, application/json, application/octet-stream, ...) or lets
the recorder sniff one, and can change their mind after the fact:
records taken while one view was selected read just as well in
another. An unreadable payload still reports its type and size, so
the metadata stays useful.

Recording is off unless a pipeline is given a recorder, and it can
be turned on and off while the graph runs.
"""

from base64 import b64encode
from collections import deque
from dataclasses import asdict, is_dataclass
from json import dumps, loads
from time import time
from typing import Callable

from hyperwire.models.message import Message

AUTO = "auto"
TEXT = "text/plain"
JSON = "application/json"
BINARY = "application/octet-stream"
PYTHON = "text/x-python"

RENDERERS: dict[str, Callable[[bytes | str], str]] = {}

HEADERS = ("content-type", "content_type", "contentType", "mime")


def renderer(mime: str) -> Callable:
    """
    Register how a media type reads. The function receives the
    snapshot (bytes or text) and returns what a reader sees.
    """

    def register(function: Callable[[bytes | str], str]) -> Callable[[bytes | str], str]:
        RENDERERS[mime] = function
        return function

    return register


def decoded(payload: bytes | str) -> str:
    """The snapshot as text, replacing what is not utf-8."""
    if isinstance(payload, bytes):
        return payload.decode("utf-8", "replace")
    return payload


@renderer(TEXT)
def text(payload: bytes | str) -> str:
    return decoded(payload)


@renderer(PYTHON)
def python(payload: bytes | str) -> str:
    return decoded(payload)


@renderer(JSON)
def document(payload: bytes | str) -> str:
    """
    Pretty printed when the snapshot parses, as text when it does not
    (a truncated document, or one that was never json).
    """
    readable = decoded(payload)
    try:
        return dumps(loads(readable), indent=2, ensure_ascii=False)
    except ValueError:
        return readable


@renderer(BINARY)
def hexdump(payload: bytes | str) -> str:
    """
    Offset, sixteen hex bytes, and their printable ascii per line.
    """
    raw = payload.encode("utf-8") if isinstance(payload, str) else payload
    lines = []
    for offset in range(0, len(raw), 16):
        chunk = raw[offset : offset + 16]
        codes = " ".join(f"{byte:02x}" for byte in chunk)
        shown = "".join(chr(byte) if 32 <= byte < 127 else "." for byte in chunk)
        lines.append(f"{offset:08x}  {codes:<47}  {shown}")
    return "\n".join(lines)


@renderer("application/base64")
def base64(payload: bytes | str) -> str:
    raw = payload.encode("utf-8") if isinstance(payload, str) else payload
    return b64encode(raw).decode("ascii")


def plain(value: object) -> object:
    """
    A json default that reads common payload objects as structure:
    dataclasses, pydantic models, sets, bytes, and anything with a
    __dict__, falling back to str.
    """
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump()
    if isinstance(value, (set, frozenset)):
        return list(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    if hasattr(value, "__dict__"):
        return vars(value)
    return str(value)


def spoken(content: object) -> str:
    """repr that cannot raise."""
    try:
        return repr(content)
    except Exception:
        return f"<{type(content).__name__} not representable>"


def structured(content: object) -> bool:
    """Whether content is a value json describes natively."""
    if isinstance(content, (dict, list, tuple, int, float, bool)) or content is None:
        return True
    if is_dataclass(content) and not isinstance(content, type):
        return True
    return callable(getattr(content, "model_dump", None))


def looks_json(text: str) -> bool:
    """Whether text is a json object or array."""
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return False
    try:
        loads(stripped)
    except ValueError:
        return False
    return True


def sniff(content: object, metadata: dict) -> str:
    """
    The media type a payload most likely is. A content type the
    message carries wins when a renderer knows it.
    """
    for header in HEADERS:
        declared = str(metadata.get(header, "")).split(";")[0].strip().lower()
        if declared in RENDERERS:
            return declared
    if isinstance(content, (bytes, bytearray, memoryview)):
        try:
            readable = bytes(content).decode("utf-8")
        except UnicodeDecodeError:
            return BINARY
        return JSON if looks_json(readable) else TEXT
    if isinstance(content, str):
        return JSON if looks_json(content) else TEXT
    if structured(content):
        return JSON
    return PYTHON


def capture(content: object, width: int) -> tuple[bytes | str, int]:
    """
    A bounded snapshot of a payload and its full size. Bytes stay
    bytes so a binary view is exact; structures are captured as json
    and everything else as its repr.
    """
    if isinstance(content, (bytes, bytearray, memoryview)):
        raw = bytes(content)
        return raw[:width], len(raw)
    if isinstance(content, str):
        return content[:width], len(content)
    if structured(content):
        try:
            rendered = dumps(content, default=plain, ensure_ascii=False)
        except (TypeError, ValueError):
            rendered = spoken(content)
    else:
        rendered = spoken(content)
    return rendered[:width], len(rendered)


def render(payload: bytes | str, mime: str) -> str:
    """
    A snapshot read as one media type, as text when none is known.
    A renderer that fails falls back to text too, so a view never
    breaks the reader.
    """
    function = RENDERERS.get(mime, text)
    try:
        return function(payload)
    except Exception:
        return decoded(payload)


class Provenance:
    """
    A bounded ring of observed messages per node.

    Depth caps how many messages each node keeps and width caps how
    much of each payload a record holds, so a node carrying large
    payloads costs the same as one carrying small ones. Metadata is
    always kept whole: it is small, and it is the part that usually
    answers the question. View is the media type records read as by
    default, auto meaning whatever each record sniffed as.
    """

    def __init__(
        self,
        depth: int = 50,
        width: int = 4096,
        view: str = AUTO,
        enabled: bool = True,
    ) -> None:
        self.depth = depth
        self.width = width
        self.view = view
        self.enabled = enabled
        self.records: dict[str, deque] = {}
        self.seen = 0

    def observe(self, node: str, message: Message, edge: str = "") -> None:
        """
        Record one message leaving a node. Cheap and non blocking: it
        appends to a bounded deque and never awaits, so a recording
        pipeline runs at the same shape as an unrecorded one.
        """
        if not self.enabled:
            return
        payload, size = capture(message.content, self.width)
        ring = self.records.get(node)
        if ring is None or ring.maxlen != self.depth:
            ring = deque(ring or (), maxlen=self.depth)
            self.records[node] = ring
        ring.append(
            {
                "id": message.id,
                "at": time(),
                "node": node,
                "edge": edge,
                "type": type(message.content).__name__,
                "mime": sniff(message.content, message.metadata),
                "metadata": dict(message.metadata),
                "payload": payload,
                "size": size,
                "truncated": size > len(payload),
            }
        )
        self.seen += 1

    def read(self, node: str = "", limit: int = 0, view: str = "") -> list[dict]:
        """
        The most recent records, newest first, for one node or for the
        whole graph, rendered as view (this recorder's view when not
        given; auto reads each record as what it sniffed as).
        """
        if node:
            found = list(self.records.get(node, ()))
        else:
            found = [record for ring in self.records.values() for record in ring]
            found.sort(key=lambda record: record["at"])
        found.reverse()
        if limit:
            found = found[:limit]
        chosen = view or self.view
        return [self.rendered(record, chosen) for record in found]

    def rendered(self, record: dict, view: str) -> dict:
        """
        One record as a reader sees it: the snapshot replaced by its
        rendering in the chosen media type.
        """
        mime = record["mime"] if view in ("", AUTO) else view
        shown = {key: value for key, value in record.items() if key != "payload"}
        shown["view"] = mime
        shown["content"] = render(record["payload"], mime)
        return shown

    def clear(self) -> None:
        self.records.clear()
        self.seen = 0

    def describe(self) -> dict:
        """
        Serializable settings and counts.
        """
        return {
            "enabled": self.enabled,
            "view": self.view,
            "depth": self.depth,
            "width": self.width,
            "seen": self.seen,
            "nodes": {name: len(ring) for name, ring in self.records.items()},
            "views": [AUTO, *sorted(RENDERERS)],
        }
