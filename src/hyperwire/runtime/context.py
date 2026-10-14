"""
Context is the pipeline's pile of shared resources.
"""

from typing import Any


class Context:
    """
    Holds things processors need but messages should not carry:
    database connections, clients, configuration. Entries read as
    attributes or by key, and resolve finds an entry by type when
    callers prefer typed access.

    A processor declaring a context parameter receives the pipeline
    context on every call.
    """

    def __init__(self, **entries: Any) -> None:
        self.entries: dict[str, Any] = dict(entries)

    def set(self, key: str, value: Any) -> None:
        self.entries[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self.entries.get(key, default)

    def resolve(self, kind: type) -> Any:
        """
        Return the first entry that is an instance of kind, or None.
        """
        for value in self.entries.values():
            if isinstance(value, kind):
                return value
        return None

    def __getattr__(self, key: str) -> Any:
        try:
            return self.__dict__["entries"][key]
        except KeyError:
            raise AttributeError(f"Context has no entry {key!r}")

    def __contains__(self, key: str) -> bool:
        return key in self.entries

    def __repr__(self) -> str:
        return f"Context({sorted(self.entries)})"
