"""AllInOne 正则 backend：每个 re.Match 是一行，行内用 `$1` / `$2` 取捕获组。"""

from typing import Any, List, Tuple

from ..errors import RuleSyntaxError
from ..nodes import Extractor, Step
from ..values import STRING_BACKEND, RuleValue


class RegexRowBackend:
    name = "regex"

    def select(self, items: Tuple, step: Step, base_url: str) -> Tuple:
        raise RuleSyntaxError(f"正则结果上不支持定位步骤：{step!r}")

    def extract(self, items: Tuple, extractor: Extractor, base_url: str) -> List[str]:
        if extractor.kind != "group":
            return [self.to_string(item) for item in items]
        index = int(extractor.name)
        out: List[str] = []
        for match in items:
            out.append(_group(match, index, fallback=""))
        return out

    def to_string(self, item: Any) -> str:
        if hasattr(item, "group"):
            text = _group(item, 1, fallback=None)
            return text if text is not None else (item.group(0) or "")
        return str(item)

    def child_value(self, item: Any) -> RuleValue:
        if hasattr(item, "group"):
            return RuleValue(backend=self, items=(item,))
        return RuleValue(backend=STRING_BACKEND, items=(str(item),))


def _group(match: Any, index: int, *, fallback):
    """组号越界时 re 抛 IndexError，不让它冒出去。"""
    try:
        return match.group(index) or ""
    except IndexError:
        return fallback


REGEX_ROW_BACKEND = RegexRowBackend()


def regex_rows(text: str, pattern) -> RuleValue:
    matches = tuple(pattern.finditer(text or ""))
    return RuleValue(backend=REGEX_ROW_BACKEND, items=matches)


__all__ = ["REGEX_ROW_BACKEND", "RegexRowBackend", "regex_rows"]
