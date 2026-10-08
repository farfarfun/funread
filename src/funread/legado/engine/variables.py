"""`@put` / `@get` 的变量池。

四层链：源级 → 书级 → 章级 → 行级。书级的 snapshot 要随 BookInfo 持久化，
否则「bookInfo 里 put、toc 里 get」在换源或重启后全读到空值。
"""

from typing import Dict, Mapping, Optional


class VariableScope:
    """带父链的字符串变量池。写只写本层，读会向上查。"""

    def __init__(
        self,
        parent: Optional["VariableScope"] = None,
        data: Optional[Mapping[str, str]] = None,
    ):
        self._parent = parent
        self._data: Dict[str, str] = {str(k): str(v) for k, v in (data or {}).items()}

    def get(self, key: str) -> str:
        if key in self._data:
            return self._data[key]
        if self._parent is not None:
            return self._parent.get(key)
        return ""

    def put(self, key: str, value: str) -> None:
        self._data[str(key)] = "" if value is None else str(value)

    def update(self, data: Optional[Mapping[str, str]]) -> None:
        for key, value in (data or {}).items():
            self.put(key, value)

    def child(self, data: Optional[Mapping[str, str]] = None) -> "VariableScope":
        return VariableScope(parent=self, data=data)

    def snapshot(self) -> Dict[str, str]:
        """含继承值的扁平快照，用于持久化。"""
        merged: Dict[str, str] = {}
        if self._parent is not None:
            merged.update(self._parent.snapshot())
        merged.update(self._data)
        return merged

    def local(self) -> Dict[str, str]:
        return dict(self._data)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key in self._data:
            return True
        return self._parent is not None and key in self._parent

    def __repr__(self) -> str:
        return f"VariableScope({self.snapshot()!r})"


__all__ = ["VariableScope"]
