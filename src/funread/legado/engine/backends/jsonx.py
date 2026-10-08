"""JSONPath backend（jsonpath-ng.ext）。

Legado 用的是 Java 的 Jayway JsonPath，jsonpath-ng 覆盖大部分但不是全部。
不认的表达式包成 `JsonPathNotSupportedError`，让那个源被标记跳过，而不是整体崩掉。
"""

import json
from functools import lru_cache
from typing import Any, List, Tuple

from ..errors import JsonPathNotSupportedError, RuleSyntaxError
from ..nodes import Extractor, Step
from ..values import STRING_BACKEND, RuleValue


@lru_cache(maxsize=4096)
def _compile(expression: str):
    try:
        from jsonpath_ng.ext import parse as jsonpath_parse
    except ImportError as exc:  # pragma: no cover - 依赖缺失路径
        raise RuleSyntaxError(
            "JSONPath 规则需要 jsonpath-ng，请安装：pip install 'funread[parse]'"
        ) from exc
    try:
        return jsonpath_parse(expression)
    except Exception as exc:
        raise JsonPathNotSupportedError(
            f"JSONPath 表达式不受支持：{expression!r}", rule=expression
        ) from exc


class JsonBackend:
    name = "json"

    def select(self, items: Tuple, step: Step, base_url: str) -> Tuple:
        raise RuleSyntaxError(f"JSON 结果上不支持 Default 定位步骤：{step!r}")

    def extract(self, items: Tuple, extractor: Extractor, base_url: str) -> List[str]:
        if extractor.kind == "attr":
            out: List[str] = []
            for item in items:
                if isinstance(item, dict):
                    out.append(self.to_string(item.get(extractor.name, "")))
                else:
                    out.append("")
            return out
        return [self.to_string(item) for item in items]

    def to_string(self, item: Any) -> str:
        if item is None:
            return ""
        if isinstance(item, str):
            return item
        if isinstance(item, bool):
            return "true" if item else "false"
        if isinstance(item, (int, float)):
            return str(item)
        return json.dumps(item, ensure_ascii=False)

    def child_value(self, item: Any) -> RuleValue:
        if isinstance(item, (dict, list)):
            return RuleValue(backend=self, items=(item,))
        return RuleValue(backend=STRING_BACKEND, items=(self.to_string(item),))


JSON_BACKEND = JsonBackend()


def loads(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuleSyntaxError(f"响应不是合法 JSON：{exc}") from exc


def document_value(text: str) -> RuleValue:
    return RuleValue(backend=JSON_BACKEND, items=(loads(text),))


def jsonpath_select(items: Tuple, expression: str) -> RuleValue:
    compiled = _compile(expression)
    found: List[Any] = []
    for item in items:
        for match in compiled.find(item):
            found.append(match.value)
    # `$.a[*]` 命中单个列表时展开成多行
    if len(found) == 1 and isinstance(found[0], list):
        found = found[0]
    return RuleValue(backend=JSON_BACKEND, items=tuple(found))


__all__ = [
    "JSON_BACKEND",
    "JsonBackend",
    "document_value",
    "jsonpath_select",
    "loads",
]
