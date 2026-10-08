"""求值期的统一中间值。

`RuleValue(backend, items)` 是唯一的中间值类型。items 的元素类型随 backend 变化
（lxml Element / JSON 对象 / re.Match / str），所有差异都收在 Backend 实现里，
编排层只看 RuleValue。
"""

from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Protocol, Sequence, Tuple, runtime_checkable

from .nodes import Extractor, IndexSpec, Step


@runtime_checkable
class Backend(Protocol):
    """一种中间值的定位/取值能力。"""

    name: str

    def select(self, items: Tuple, step: Step, base_url: str) -> Tuple:
        """按一个定位步骤收缩/展开 items。"""

    def extract(self, items: Tuple, extractor: Extractor, base_url: str) -> List[str]:
        """把 items 取成字符串列表。"""

    def to_string(self, item: Any) -> str:
        """单个 item 的默认字符串化（列表行取整体文本时用）。"""

    def child_value(self, item: Any) -> "RuleValue":
        """把一个 item 包成独立的 RuleValue，作为列表行的子作用域。"""


@dataclass(frozen=True)
class RuleValue:
    backend: Backend
    items: Tuple[Any, ...] = ()

    @staticmethod
    def empty() -> "RuleValue":
        return RuleValue(backend=_NULL_BACKEND, items=())

    def is_empty(self) -> bool:
        if not self.items:
            return True
        return all(not str(s).strip() for s in self.as_strings())

    def as_strings(self) -> List[str]:
        return [self.backend.to_string(item) for item in self.items]

    def as_string(self, sep: str = "\n") -> str:
        parts = [s for s in (s.strip() for s in self.as_strings()) if s]
        return sep.join(parts)

    def map_strings(self, fn: Callable[[str], str]) -> "RuleValue":
        return RuleValue(
            backend=STRING_BACKEND,
            items=tuple(fn(s) for s in self.as_strings()),
        )

    def take(self, spec: Optional[IndexSpec]) -> "RuleValue":
        if spec is None:
            return self
        return RuleValue(backend=self.backend, items=spec.apply(self.items))

    def rows(self) -> List["RuleValue"]:
        return [self.backend.child_value(item) for item in self.items]

    @staticmethod
    def concat(values: Sequence["RuleValue"]) -> "RuleValue":
        """`&&`：按顺序拼接，**不去重**。

        对齐 Legado（`Elements.addAll` / `List.addAll`）。曾经想在这里顺带去重，
        但去重会误伤合法的重复值 —— 比如章节名列表里本来就有两个「第一章」，
        或者 `&&` 的两侧刻意取同一批节点的不同字段。写规则的人要的是拼接。
        """
        kept = [v for v in values if v.items]
        if not kept:
            return RuleValue.empty()
        if len(kept) == 1:
            return kept[0]
        backend = kept[0].backend
        if all(v.backend is backend for v in kept):
            items: List[Any] = []
            for value in kept:
                items.extend(value.items)
            return RuleValue(backend=backend, items=tuple(items))
        merged: List[str] = []
        for value in kept:
            merged.extend(value.as_strings())
        return RuleValue(backend=STRING_BACKEND, items=tuple(merged))

    @staticmethod
    def interleave(values: Sequence["RuleValue"]) -> "RuleValue":
        """`%%`：按下标交替取。"""
        kept = [v for v in values if v.items]
        if not kept:
            return RuleValue.empty()
        if len(kept) == 1:
            return kept[0]
        backend = kept[0].backend
        if all(v.backend is backend for v in kept):
            out: List[Any] = []
            for idx in range(max(len(v.items) for v in kept)):
                for value in kept:
                    if idx < len(value.items):
                        out.append(value.items[idx])
            return RuleValue(backend=backend, items=tuple(out))
        strings = [v.as_strings() for v in kept]
        flat: List[str] = []
        for idx in range(max(len(s) for s in strings)):
            for group in strings:
                if idx < len(group):
                    flat.append(group[idx])
        return RuleValue(backend=STRING_BACKEND, items=tuple(flat))


class StringBackend:
    """纯字符串中间值：常量、JS 返回值、取值后的结果。"""

    name = "string"

    def select(self, items: Tuple, step: Step, base_url: str) -> Tuple:
        from .errors import RuleSyntaxError

        raise RuleSyntaxError(f"字符串结果上不能再做定位：{step!r}")

    def extract(self, items: Tuple, extractor: Extractor, base_url: str) -> List[str]:
        return [self.to_string(item) for item in items]

    def to_string(self, item: Any) -> str:
        return item if isinstance(item, str) else str(item)

    def child_value(self, item: Any) -> RuleValue:
        return RuleValue(backend=self, items=(item,))


class _NullBackend(StringBackend):
    name = "null"


STRING_BACKEND = StringBackend()
_NULL_BACKEND = _NullBackend()


def string_value(values) -> RuleValue:
    if isinstance(values, str):
        values = [values]
    return RuleValue(backend=STRING_BACKEND, items=tuple(values))


__all__ = ["STRING_BACKEND", "Backend", "RuleValue", "StringBackend", "string_value"]
