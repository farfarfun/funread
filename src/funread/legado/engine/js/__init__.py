"""JS 运行时。

两个实现：`NullJsRuntime` 一律抛错（没装 `funread[js]` 时的兜底），
`QuickJsRuntime` 真执行（quickjs + `java.*` 宿主桥）。

选 quickjs 而不是静默降级，是因为「不支持」和「今天没更新」在界面上必须分得开 ——
静默返空会让一个该修规则的源看起来像一个正常但没内容的源。
"""

from typing import Any, Callable, Literal, Optional

from .base import JsContext, JsRuntime
from .null import NullJsRuntime


def default_js_runtime(
    prefer: Literal["auto", "null", "quickjs"] = "auto",
    *,
    fetcher: Any = None,
    select: Optional[Callable[[str, str, bool], Any]] = None,
) -> JsRuntime:
    """按可用性挑运行时。

    Args:
        fetcher: 给 `java.ajax` 用。不传的话那个方法会抛，而不是静默返空 ——
            复用引擎的 fetcher 才能共享 cookie jar 和 UA，很多站点靠这个工作。
        select: 给 `java.getString` / `getElements` 用，由引擎侧注入求值器。
    """
    if prefer in ("auto", "quickjs"):
        try:
            from .quickjs_runtime import QuickJsRuntime

            runtime = QuickJsRuntime(fetcher=fetcher, select=select)
            if runtime.available():
                return runtime
            if prefer == "quickjs":
                raise ImportError("quickjs 不可用")
        except ImportError:
            if prefer == "quickjs":
                raise
    return NullJsRuntime()


__all__ = ["JsContext", "JsRuntime", "NullJsRuntime", "default_js_runtime"]
