"""Legado JS 规则可见的宿主 API（`java.*` / `source.*` / `cookie.*` / `book.*`）。

## 实现哪些，怎么定的

扫了归档里 13,264 个书源：6,702 个（50.5%）含 JS，一共用到 **209 个**不同的宿主
方法，但**前 10 个覆盖了 75.8% 的调用**。所以这里不照 Legado 文档全实现，而是按
实测频率从高到低做，长尾留给后续按需补 —— 覆盖率曲线最陡的那一段在前面。

注意频率榜前列的 `result.match` / `result.replace` / `result.split` **不在这里**：
它们是 JS 原生的 String 方法，只要把 `result` 作为字符串注入，引擎自己就支持了。
把它们当宿主 API 实现是白做功。

## 没实现的一律抛错，不返回空

静默返回 `""` 或 `null` 会让「这个源不支持」和「这个源今天没更新」在界面上长得
一模一样，而前者该修规则、后者该等明天。整个引擎在这件事上是一致的
（见 `NullJsRuntime`、`WebViewNotSupportedError`）。
"""

from __future__ import annotations

import base64
import hashlib
import time
import urllib.parse
from typing import Any, Callable, Dict, Optional

from farlog import getLogger

from ..errors import UnsupportedFeatureError

logger = getLogger("funread")

#: 中文数字 -> 值。`java.toNumChapter` 用它把「第一百二十三章」里的数字抠出来。
_CN_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "壹": 1,
    "二": 2,
    "贰": 2,
    "两": 2,
    "三": 3,
    "叁": 3,
    "四": 4,
    "肆": 4,
    "五": 5,
    "伍": 5,
    "六": 6,
    "陆": 6,
    "七": 7,
    "柒": 7,
    "八": 8,
    "捌": 8,
    "九": 9,
    "玖": 9,
}
_CN_UNITS = {"十": 10, "拾": 10, "百": 100, "佰": 100, "千": 1000, "仟": 1000}


def cn_to_int(text: str) -> Optional[int]:
    """「一百二十三」-> 123。认不出返回 `None`。

    只处理万以内：章节号再大也到不了「万」这一级，而支持它要引入节权位的进位
    逻辑，收益和复杂度不成比例。
    """
    if not text:
        return None
    total = 0
    current = 0
    seen = False
    for char in text:
        if char in _CN_DIGITS:
            current = _CN_DIGITS[char]
            seen = True
        elif char in _CN_UNITS:
            unit = _CN_UNITS[char]
            #  「十二」这种省略了前导一
            total += (current or 1) * unit
            current = 0
            seen = True
        else:
            return None
    if not seen:
        return None
    return total + current


class HostApi:
    """一次 JS 求值期间可用的宿主对象集合。

    每次 `eval` 新建一个：`java.put` 写的变量要落到**这一次**求值的作用域上，
    而不是泄漏给下一个源。真正需要跨规则存活的东西（`@put`/`@get` 变量、cookie）
    由外面传进来的 `variables` / `cookie_jar` 持有，它们本来就是共享的。
    """

    def __init__(
        self,
        *,
        base_url: str = "",
        variables: Any = None,
        source_key: str = "",
        source_variables: Optional[Dict[str, str]] = None,
        cookies: Optional[Dict[str, str]] = None,
        book: Optional[Dict[str, Any]] = None,
        ajax: Optional[Callable[[str], str]] = None,
        select: Optional[Callable[[str, str, bool], Any]] = None,
    ):
        self.base_url = base_url
        self.variables = variables
        self.source_key = source_key
        self.source_variables = source_variables if source_variables is not None else {}
        self.cookies = cookies if cookies is not None else {}
        self.book = book or {}
        #: 发一次 HTTP GET 并返回正文。由 runtime 从 fetcher 适配过来；没有就抛。
        self._ajax = ajax
        #: `(html, rule, want_list) -> 结果`，把 `java.getString` 这类回调给求值器。
        self._select = select

    # ------------------------------------------------------------------ 摘要与编码

    def md5_encode(self, text: Any) -> str:
        return hashlib.md5(_as_text(text).encode("utf-8")).hexdigest()

    def md5_encode16(self, text: Any) -> str:
        """Legado 的 16 位 md5 就是 32 位取中间 16 位，不是另一种算法。"""
        return self.md5_encode(text)[8:24]

    def base64_encode(self, text: Any, flags: Any = None) -> str:
        return base64.b64encode(_as_text(text).encode("utf-8")).decode("ascii")

    def base64_decode(self, text: Any, flags: Any = None) -> str:
        return _b64_bytes(text).decode("utf-8", errors="replace")

    def base64_decode_to_bytes(self, text: Any, flags: Any = None) -> str:
        """字节串，但用 **latin-1 字符串**承载。

        quickjs 的 Python 绑定只能返回 str/int/float/bool/None —— list、dict、
        bytes 一律 "Can not convert Python result to JS"。而 latin-1 是唯一一个
        字节值和码点一一对应的编码，所以 `bytes <-> str` 往返无损，JS 侧拿到的
        `.length` 也正好是字节数。

        规则里对它的用法几乎全是「再喂回另一个 `java.*`」（典型是 AES 解密的
        key/iv），那条路径上 Python 侧用 `as_bytes()` 还原即可，JS 从头到尾不用
        知道它其实是字节。
        """
        return _b64_bytes(text).decode("latin-1")

    def encode_uri(self, text: Any, enc: Any = None) -> str:
        return urllib.parse.quote(_as_text(text), safe="~@#$&()*!+=:;,?/'")

    def decode_uri(self, text: Any, enc: Any = None) -> str:
        return urllib.parse.unquote(_as_text(text))

    def hex_decode_to_string(self, text: Any) -> str:
        try:
            return bytes.fromhex(_as_text(text)).decode("utf-8", errors="replace")
        except ValueError:
            return ""

    # ------------------------------------------------------------------ 变量存取

    def get(self, key: Any) -> str:
        """`java.get` —— 读 `@put` 存下的变量。调用量排第 3。"""
        if self.variables is None:
            return ""
        return self.variables.get(_as_text(key))

    def put(self, key: Any, value: Any = "") -> Any:
        """`java.put` —— 写变量。按 Legado 的语义**返回写入的值**，规则里常直接
        把它的返回值当表达式结果用。"""
        if self.variables is not None:
            self.variables.put(_as_text(key), _as_text(value))
        return value

    # ------------------------------------------------------------------ 时间与日志

    def time_format(self, millis: Any) -> str:
        try:
            seconds = float(millis) / 1000.0
        except (TypeError, ValueError):
            return ""
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(seconds))

    def log(self, message: Any = "") -> Any:
        #: 规则作者的调试输出。降到 debug：一次聚合搜索会跑几十个源，按 info 打
        #: 会把真正的日志淹掉。
        logger.debug("[js] {}", _as_text(message))
        return message

    def toast(self, message: Any = "") -> Any:
        """原生 APP 里弹吐司。服务端没有界面，等同于 log。

        **不能抛错**：规则作者常拿它当调试输出，抛错会让一堆本来能跑的源挂掉。
        """
        logger.debug("[js:toast] {}", _as_text(message))
        return message

    # ------------------------------------------------------------------ 章节号

    def to_num_chapter(self, text: Any) -> str:
        """把章节名里的中文数字换成阿拉伯数字：「第一百二十三章」->「第123章」。"""
        raw = _as_text(text)
        if not raw:
            return ""
        out: list[str] = []
        buffer: list[str] = []

        def flush() -> None:
            if not buffer:
                return
            chunk = "".join(buffer)
            value = cn_to_int(chunk)
            out.append(chunk if value is None else str(value))
            buffer.clear()

        for char in raw:
            if char in _CN_DIGITS or char in _CN_UNITS:
                buffer.append(char)
            else:
                flush()
                out.append(char)
        flush()
        return "".join(out)

    # ------------------------------------------------------------------ 网络与解析

    def ajax(self, url: Any) -> str:
        """同步 GET，返回正文。调用量排第 5。

        走引擎自己的 fetcher 而不是另起一个 HTTP 客户端：那样才能共用这个源的
        cookie jar、UA 和超时设置 —— 很多站点靠「首页下发 cookie、接口校验 cookie」
        工作，另开一个 session 会直接被挡。
        """
        if self._ajax is None:
            raise UnsupportedFeatureError("java.ajax 不可用：这次求值没有注入 fetcher")
        return self._ajax(_as_text(url))

    def get_string(self, rule: Any, html: Any = None) -> str:
        """拿规则从 HTML 里取一个字符串。调用量排第 6。"""
        if self._select is None:
            raise UnsupportedFeatureError("java.getString 不可用：这次求值没有注入求值器")
        return _as_text(
            self._select(_as_text(html) if html is not None else "", _as_text(rule), False)
        )

    def get_elements(self, rule: Any, html: Any = None) -> str:
        """返回 **JSON 字符串**，由 bootstrap 里的包装函数 `JSON.parse` 成真数组。

        同样是绑定不能返回 list 的缘故。绕一趟 JSON 而不是改成「返回长度 + 逐个
        取」那种接口，是因为规则里对结果的用法是任意的数组操作（遍历、slice、
        map），只有还原成真数组才都能用。
        """
        import json as _json

        if self._select is None:
            raise UnsupportedFeatureError("java.getElements 不可用：这次求值没有注入求值器")
        items = self._select(_as_text(html) if html is not None else "", _as_text(rule), True)
        if not isinstance(items, list):
            items = [] if items is None else [items]
        return _json.dumps([_as_text(item) for item in items], ensure_ascii=False)

    def get_element(self, rule: Any, html: Any = None) -> str:
        """取第一个。在 Python 侧切出来，省得 JS 侧再 parse 一遍整个数组。"""
        if self._select is None:
            raise UnsupportedFeatureError("java.getElement 不可用：这次求值没有注入求值器")
        items = self._select(_as_text(html) if html is not None else "", _as_text(rule), True)
        if isinstance(items, list):
            return _as_text(items[0]) if items else ""
        return _as_text(items)

    # ------------------------------------------------------------------ source.*

    def source_get_key(self) -> str:
        return self.source_key

    def source_get_variable(self) -> str:
        return _as_text(self.source_variables.get("variable", ""))

    def source_get_login_info_map(self) -> Dict[str, str]:
        """登录信息。我们不支持书源登录，返回空表而不是抛错。

        规则里典型的用法是 `source.getLoginInfoMap()?.get("token")`，拿不到就走
        未登录分支 —— 返回空表让这条分支正常工作，抛错反而把整个源判死。
        """
        return {}

    # ------------------------------------------------------------------ cookie.*

    def cookie_get_key(self, url: Any = "", key: Any = "") -> str:
        """调用量排第 4。按 key 取 cookie，没有返回空串。"""
        name = _as_text(key)
        if not name:
            #  不给 key 时 Legado 返回整条 cookie 串
            return "; ".join(f"{k}={v}" for k, v in self.cookies.items())
        return _as_text(self.cookies.get(name, ""))

    def cookie_remove_cookie(self, url: Any = "") -> None:
        self.cookies.clear()

    # ------------------------------------------------------------------ book.*

    def book_put_variable(self, key: Any, value: Any = "") -> Any:
        self.book.setdefault("variables", {})[_as_text(key)] = _as_text(value)
        return value

    def book_get_variable(self, key: Any) -> str:
        return _as_text(self.book.get("variables", {}).get(_as_text(key), ""))


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        #  JS 的 true/false 转成字符串必须是小写，Python 的 str(True) 是 "True"
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        #  quickjs 的数字统一是 float，而规则里 `java.get(1)` 期望的是 "1" 不是 "1.0"
        return str(int(value))
    return str(value)


def as_bytes(value: Any) -> bytes:
    """把 `base64DecodeToByteArray` 产出的 latin-1 字符串还原成字节。

    接收字节参数的宿主方法（AES 那几个）都该先过这里，而不是直接 `.encode()`
    —— utf-8 会把 0x80 以上的字节编成两个，key 就错了。
    """
    if isinstance(value, bytes):
        return value
    return _as_text(value).encode("latin-1", errors="replace")


def _b64_bytes(text: Any) -> bytes:
    raw = _as_text(text).strip()
    #  书源里的 base64 常常缺尾部填充，严格解码会直接抛
    padding = (-len(raw)) % 4
    try:
        return base64.b64decode(raw + "=" * padding, validate=False)
    except Exception:
        return b""


__all__ = ["HostApi", "as_bytes", "cn_to_int"]
