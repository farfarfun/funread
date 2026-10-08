"""JS 运行时（phase 1 只有 Null 实现）。"""

from typing import Literal

from .base import JsContext, JsRuntime
from .null import NullJsRuntime


def default_js_runtime(prefer: Literal["auto", "null", "quickjs"] = "auto") -> JsRuntime:
    """按可用性挑运行时。phase 2 在这里接 QuickJsRuntime。"""
    if prefer in ("auto", "quickjs"):
        try:
            from .quickjs import QuickJsRuntime  # type: ignore[attr-defined]

            return QuickJsRuntime()
        except ImportError:
            if prefer == "quickjs":
                raise
    return NullJsRuntime()


__all__ = ["JsContext", "JsRuntime", "NullJsRuntime", "default_js_runtime"]
