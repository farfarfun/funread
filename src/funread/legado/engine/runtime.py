"""引擎默认的 JS 运行时：把 fetcher 和求值器接到 quickjs 的宿主桥上。

`BookSourceEngine` / `RssSourceEngine` 早先在 `js` 没传时直接落到
`NullJsRuntime()`，而 **src 里没有任何调用方传过 `js=`** —— 于是 quickjs 只在
单测里「接上」了，服务层、probe、survey 跑的全是 Null 运行时，带 JS 的源一律
报「当前阶段不支持」。2026-10-18 重测 40 个 needs_js 源时 22 个全是这一条错，
才发现的。

这里给两个引擎一个共同的默认：装了 quickjs 就真执行，没装仍然是 Null（显式报错，
不静默降级 —— 理由见 `js/__init__.py`）。
"""

from typing import Any, List, Union

from .evaluator import RuleEvaluator
from .js import JsRuntime, default_js_runtime


def _make_select(base_url: str):
    """给 `java.getString` / `getElements` 用：拿规则从一段 HTML 里取值。

    每次新建一个 `RuleEvaluator` —— 宿主调用给的 HTML 是脚本自己拼的、和当前
    页面无关，不能复用引擎手里那个求值器的文档。
    """

    def select(html: str, rule: str, want_list: bool) -> Union[str, List[str]]:
        evaluator = RuleEvaluator(html or "", base_url=base_url)
        return evaluator.strings(rule) if want_list else evaluator.string(rule)

    return select


def engine_js_runtime(fetcher: Any, base_url: str = "") -> JsRuntime:
    """引擎默认的运行时。`fetcher` 复用引擎自己那个，共享 cookie 与 UA。"""
    return default_js_runtime("auto", fetcher=fetcher, select=_make_select(base_url))


__all__ = ["engine_js_runtime"]
