"""JS 运行时接口。

phase 1 注入 `NullJsRuntime`（必抛），phase 2 换成 quickjs 实现 + `java.*` 桥，
解析器和编排层一行不改。`JsContext` 现在就把 phase 2 需要的全部上下文带上。
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol, runtime_checkable


@dataclass
class JsContext:
    """Legado JS 规则里可见的变量。"""

    base_url: str = ""
    result: Any = None
    book: Optional[Dict[str, Any]] = None
    chapter: Optional[Dict[str, Any]] = None
    variables: Any = None
    source: Any = None
    cookie: Any = None
    cache: Any = None
    title: str = ""
    src: str = ""
    page: int = 1
    key: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class JsRuntime(Protocol):
    name: str

    def available(self) -> bool:
        """当前运行时是否真的能执行 JS。"""

    def eval(self, script: str, context: JsContext) -> Any:
        """执行脚本并返回结果。不支持时抛 JsNotSupportedError。"""


__all__ = ["JsContext", "JsRuntime"]
