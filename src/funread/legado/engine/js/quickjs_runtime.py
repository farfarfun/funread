"""基于 quickjs 的 JS 规则求值。

`base.py` 里说的 phase 2 就是这个：解析器和编排层一行不改，只是把注入的
`NullJsRuntime` 换成它。

## 为什么每次求值都新建 Context

书源规则是**第三方内容**，而且 13,264 个源里有 6,702 个带 JS。复用一个 Context
意味着 A 源污染的全局变量会被 B 源看到 —— 规则作者大量使用裸赋值（`x = 1` 而不是
`let x = 1`），这种跨源串台会表现成「某个源单独跑没问题、混在搜索里就出错」，
属于最难复现的那类 bug。新建的代价实测在毫秒级，不值得为它冒这个险。

## 资源上限

这是 quickjs 这个 Python 绑定的硬限制，不是设计选择：

    >>> ctx.set_time_limit(5.0); ctx.add_callable("f", f); ctx.eval("f()")
    InternalError: Can not call into Python with a time limit set.

时间限制和 Python 回调互斥。规则在子进程中运行，宿主回调通过管道交给父进程；
超时后可以直接终止子进程，不影响服务进程。

剩下两道都验证过确实拦得住：

- **内存上限**挡住失控拼接（`s += s` 这类，实测 0.01 秒就抛 out of memory，
  而且抛完之后回调仍然可用）。这是最常见的失控形态。
- **栈上限**挡住无限递归。

纯计算死循环（`while (true) { i++ }`）由父进程的墙钟时限终止。宿主网络
回调在父进程里执行，仍受抓取器自己的网络超时约束。
"""

from __future__ import annotations

import json
import multiprocessing
import threading
import time
from typing import Any, Callable, Dict, Optional

from farlog import getLogger

from ..errors import JsNotSupportedError
from ..fetch import Request
from .base import JsContext
from .host import HostApi

logger = getLogger("funread")

#: 单次求值的内存上限。64MB 对取值、拼串、算签名这类活绰绰有余，而失控的拼接
#: 能在几秒内吃掉几个 G。
DEFAULT_MEMORY_LIMIT = 64 * 1024 * 1024
#: 单次求值的栈上限，挡无限递归。1MB 够正常规则用好几层。
DEFAULT_STACK_SIZE = 1024 * 1024
DEFAULT_EXECUTION_TIMEOUT = 5.0
# shortcut: one process per eval has startup cost; pool workers if JS-heavy searches become slow.
_JS_SLOTS = threading.BoundedSemaphore(8)

#: 把宿主对象拼成 JS 侧的命名空间。quickjs 的 `add_callable` 只能注册顶层函数，
#: 所以先注册成带前缀的扁平名字，再用这段脚本组装成 `java.xxx` 的形状。
_BOOTSTRAP = """
var java = {
    md5Encode: __md5Encode,
    md5Encode16: __md5Encode16,
    base64Encode: __base64Encode,
    base64Decode: __base64Decode,
    base64DecodeToByteArray: __base64DecodeToByteArray,
    encodeURI: __encodeURI,
    decodeURI: __decodeURI,
    hexDecodeToString: __hexDecodeToString,
    get: __get,
    put: __put,
    timeFormat: __timeFormat,
    log: __log,
    toast: __toast,
    longToast: __toast,
    toNumChapter: __toNumChapter,
    ajax: __ajax,
    getString: __getString,
    //  Python 侧给的是 JSON 字符串（绑定返回不了数组），这里还原成真数组
    getElements: function (rule, html) { return JSON.parse(__getElements(rule, html)); },
    getElement: __getElement,
};
var source = {
    getKey: __sourceGetKey,
    getVariable: __sourceGetVariable,
    //  这两个直接用纯 JS 返回空值，不绕 Python：quickjs 的绑定转不了 dict
    //  （返回字典会得到 "Can not convert Python result to JS"），而它们本来
    //  就只需要返回一个空壳 —— 我们不支持书源登录。
    getLoginInfoMap: function () { return {}; },
    getLoginHeader: function () { return null; },
};
var cookie = {
    getKey: __cookieGetKey,
    getCookie: __cookieGetKey,
    removeCookie: __cookieRemoveCookie,
};
var cache = {
    get: __get,
    put: __put,
    getFile: function () { return null; },
};
var book = {
    putVariable: __bookPutVariable,
    getVariable: __bookGetVariable,
};
"""


def _run_js(
    conn,
    script: str,
    values: dict[str, Any],
    bindings: tuple[str, ...],
    memory_limit: int,
    stack_size: int,
) -> None:
    try:
        import quickjs

        ctx = quickjs.Context()
        ctx.set_memory_limit(memory_limit)
        ctx.set_max_stack_size(stack_size)

        def invoke(name: str, *args: Any) -> Any:
            conn.send(("call", name, args))
            return conn.recv()[1]

        for name in bindings:
            ctx.add_callable(name, lambda *args, _name=name: invoke(_name, *args))
        ctx.eval(_BOOTSTRAP)
        for name, value in values.items():
            ctx.set(name, value)
        result = _eval_script(ctx, script)
        if isinstance(result, quickjs.Object):
            result = json.loads(result.json())
        conn.send(("result", result))
    except Exception as exc:
        conn.send(("error", f"{type(exc).__name__}: {exc}"))
    finally:
        conn.close()


class QuickJsRuntime:
    """用 quickjs 执行 Legado 的 JS 规则。"""

    name = "quickjs"

    def __init__(
        self,
        *,
        fetcher: Any = None,
        select: Optional[Callable[[str, str, bool], Any]] = None,
        memory_limit: int = DEFAULT_MEMORY_LIMIT,
        stack_size: int = DEFAULT_STACK_SIZE,
        timeout: float = DEFAULT_EXECUTION_TIMEOUT,
    ):
        #: 用来实现 `java.ajax`。给 None 的话那个方法会抛，而不是静默返回空串。
        self._fetcher = fetcher
        #: 用来实现 `java.getString` / `getElements`，由引擎侧注入。
        self._select = select
        self._memory_limit = memory_limit
        self._stack_size = stack_size
        self._timeout = timeout

    def available(self) -> bool:
        try:
            import quickjs  # noqa: F401
        except ImportError:
            return False
        return True

    def eval(self, script: str, context: JsContext) -> Any:
        try:
            import quickjs  # noqa: F401
        except ImportError as exc:  # pragma: no cover - 取决于装没装 extra
            raise JsNotSupportedError(
                "需要 JS 求值但没装 quickjs，装 funread[js] 后启用", rule=script
            ) from exc

        host = self._build_host(context)
        #  本次求值期间失败的宿主调用。单独收着是为了在报错时附上 —— 见下面
        #  异常分支里的说明。
        failures: list[str] = []
        bindings = self._bindings(host, failures)
        values = {
            "result": "" if context.result is None else str(context.result),
            "baseUrl": context.base_url or "",
            "src": context.src or "",
            "title": context.title or "",
            "key": context.key or "",
            "page": int(context.page or 1),
        }

        try:
            with _JS_SLOTS:
                process_context = multiprocessing.get_context("spawn")
                parent, child = process_context.Pipe()
                process = process_context.Process(
                    target=_run_js,
                    args=(
                        child,
                        script,
                        values,
                        tuple(bindings),
                        self._memory_limit,
                        self._stack_size,
                    ),
                )
                started = False
                try:
                    process.start()
                    started = True
                    child.close()
                    deadline = time.monotonic() + self._timeout
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not parent.poll(remaining):
                            raise TimeoutError(f"JS 执行超过 {self._timeout:g} 秒")
                        try:
                            message = parent.recv()
                        except EOFError as exc:
                            raise RuntimeError("JS 子进程意外退出") from exc
                        if message[0] == "call":
                            callback_started = time.monotonic()
                            parent.send(("return", bindings[message[1]](*message[2])))
                            deadline += time.monotonic() - callback_started
                        elif message[0] == "error":
                            raise RuntimeError(message[1])
                        else:
                            return message[1]
                finally:
                    if started:
                        if process.is_alive():
                            process.terminate()
                        process.join(timeout=1)
                        if process.is_alive():
                            process.kill()
                            process.join()
                    parent.close()
                    child.close()
        except Exception as exc:
            #  quickjs 的异常类型随版本变（JSException / StackOverflow / 超时），
            #  一律折成 JsNotSupportedError：对调用方来说「这条规则没跑出来」是
            #  同一回事，而原始信息保留在 message 里供排查。
            snippet = script.strip()
            if len(snippet) > 120:
                snippet = snippet[:117] + "..."
            #  **把宿主调用的失败附在后面**。不附的话排查会被带到沟里：宿主失败
            #  降级成空串之后，规则下一步往往是 `JSON.parse(result)`，而
            #  `JSON.parse("")` 报的是 `SyntaxError: unexpected end of input` ——
            #  看起来像这段 JS 有语法错，而真正的原因是上一行的 java.ajax 没连上。
            #  这个坑在诊断归档源时真的把人骗过一次。
            hint = ""
            if failures:
                shown = "; ".join(failures[:3])
                more = f"，另有 {len(failures) - 3} 处" if len(failures) > 3 else ""
                hint = f"；**期间宿主调用失败**：{shown}{more}"
            raise JsNotSupportedError(
                f"JS 执行失败：{type(exc).__name__}: {exc}{hint}（规则：{snippet!r}）",
                rule=script,
            ) from exc

    # ------------------------------------------------------------------ 内部

    def _build_host(self, context: JsContext) -> HostApi:
        source = context.source if isinstance(context.source, dict) else {}
        return HostApi(
            base_url=context.base_url,
            variables=context.variables,
            source_key=str(source.get("key", "") or context.base_url),
            source_variables=source.get("variables") if isinstance(source, dict) else None,
            cookies=source.get("cookies") if isinstance(source, dict) else None,
            book=context.book,
            ajax=self._make_ajax(),
            select=self._select,
        )

    def _make_ajax(self) -> Optional[Callable[[str], str]]:
        if self._fetcher is None:
            return None

        def ajax(url: str) -> str:
            #  复用引擎的 fetcher，拿到的是同一个 session（cookie / UA / 超时都在）
            return self._fetcher.fetch(Request(url=url)).text

        return ajax

    def _bindings(self, host: HostApi, failures: list[str]) -> Dict[str, Callable[..., Any]]:
        bindings: Dict[str, Callable[..., Any]] = {
            "__md5Encode": host.md5_encode,
            "__md5Encode16": host.md5_encode16,
            "__base64Encode": host.base64_encode,
            "__base64Decode": host.base64_decode,
            "__base64DecodeToByteArray": host.base64_decode_to_bytes,
            "__encodeURI": host.encode_uri,
            "__decodeURI": host.decode_uri,
            "__hexDecodeToString": host.hex_decode_to_string,
            "__get": host.get,
            "__put": host.put,
            "__timeFormat": host.time_format,
            "__log": host.log,
            "__toast": host.toast,
            "__toNumChapter": host.to_num_chapter,
            "__ajax": host.ajax,
            "__getString": host.get_string,
            "__getElements": host.get_elements,
            "__getElement": host.get_element,
            "__sourceGetKey": host.source_get_key,
            "__sourceGetVariable": host.source_get_variable,
            "__cookieGetKey": host.cookie_get_key,
            "__cookieRemoveCookie": host.cookie_remove_cookie,
            "__bookPutVariable": host.book_put_variable,
            "__bookGetVariable": host.book_get_variable,
        }
        return {name: _guard(name, func, failures) for name, func in bindings.items()}


def _guard(name: str, func: Callable[..., Any], failures: list[str]) -> Callable[..., Any]:
    """包一层，把宿主函数里的异常挡在 JS 边界外，并记下来。

    不包的话，一个 `java.ajax` 的网络异常会穿过 quickjs 的 C 栈往上抛，那条路径
    上的行为取决于绑定实现，最糟是整个解释器状态不可用。折成返回空串则让规则
    自己走它的失败分支 —— 规则作者本来就得处理「这个接口没返回东西」。

    但**降级必须留痕**：空串会让规则在后面某处炸，而那个位置报出来的错和真正
    的原因毫无关系（典型是 `JSON.parse("")` 的 "unexpected end of input"）。
    所以失败记进 `failures`，由 `eval` 在抛错时一并带出去。
    """
    short = name.lstrip("_")

    def wrapper(*args: Any) -> Any:
        try:
            return func(*args)
        except Exception as exc:  # noqa: BLE001 — 边界处必须拦住全部
            detail = f"{short}: {type(exc).__name__}: {exc}"
            logger.debug("[js] 宿主调用失败 {}", detail)
            failures.append(detail[:200])
            return ""

    return wrapper


def _eval_script(ctx: Any, script: str) -> Any:
    """执行规则脚本。先按原样跑，顶层 `return` 的再包成函数重试。

    quickjs 的 `eval` 本来就支持多语句并返回最后一个表达式的值
    （`var x = 1; x + 2` -> 3），唯一跑不了的是**顶层 return** —— 那在 program
    模式下是语法错。所以只需要为这一种情况兜底。

    早先这里是反过来的：先猜脚本是表达式还是语句块，判据是 `"return" in body`。
    那个判据错得很隐蔽 —— `var s = "returned"; s` 里的 "return" 只是字符串的一
    部分，却会让脚本走进「语句块」分支；反过来多语句脚本没有 return 时会被包成
    `return (var x = 1; ...)`，直接语法错。实测 40 个 needs_js 的源里有 7 个栽在
    这上面，而它们报的是 SyntaxError，看起来像源的规则有问题，不像我们的 bug。

    让引擎自己判断，比猜可靠。
    """
    body = script.strip()
    if not body:
        return ""
    try:
        return ctx.eval(body)
    except Exception as exc:
        #  只认这一种：别的语法错就是规则真有问题，包一层也救不回来，
        #  而无条件重试会把真实错误替换成第二次的错误、更难排查。
        if "return not in a function" not in str(exc):
            raise
        return ctx.eval(f"(function () {{\n{body}\n}})()")


__all__ = ["QuickJsRuntime", "DEFAULT_MEMORY_LIMIT", "DEFAULT_STACK_SIZE"]
