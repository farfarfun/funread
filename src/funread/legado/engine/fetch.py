"""抓取抽象：`Page` + `Fetcher` 协议 + URL 尾部选项解析。

**这里不许 import requests。** `engine/` 的分层禁令要求纯解析，真实 HTTP 实现放在
`funread/legado/net/`，由调用方注入。好处有两个：四段流程能用假 fetcher 做离线
单测（不打网），以及 `import funread.legado.engine` 不会把 requests 拉进来。

Legado 的 URL 可以带一段尾部 JSON 选项：

    https://x.com/s?k={{key}},{"method":"POST","body":"k={{key}}","charset":"gbk"}

解析难点是那个逗号 —— URL 自己也可能含逗号和 `{}`，所以只能从**末尾**找配平的
`{...}`，不能用 `split(",")`。
"""

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol, Tuple, runtime_checkable


@dataclass
class Page:
    """一次抓取的结果。

    `url` 是**重定向之后**的最终地址 —— 页内相对链接必须按它解析，按请求前的
    地址解析会在站点做 301 时整页链接算错。
    """

    url: str = ""
    text: str = ""
    status: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    encoding: str = ""
    from_cache: bool = False

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 400


@dataclass
class Request:
    """一次抓取请求。由 `parse_url_options` 从规则串里解析出来。"""

    url: str = ""
    method: str = "GET"
    body: str = ""
    charset: str = ""
    headers: Dict[str, str] = field(default_factory=dict)
    retry: int = 0
    #  `type` 非空表示期望的响应类型（源里常见 `json`/`text`/图片）
    type: str = ""
    #  `webView`：Legado 用内置浏览器渲染。phase 1 不支持，由 net 层决定降级还是拒绝
    web_view: bool = False
    js: str = ""
    options: Dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class Fetcher(Protocol):
    """抓取器接口。真实实现见 `funread/legado/net/`。"""

    def fetch(self, request: Request) -> Page:
        """发请求并返回 `Page`。失败时抛 `FetchError`。"""


def _find_trailing_json(text: str) -> Optional[int]:
    """从末尾找配平的 `{...}`，返回它的起始下标。

    不能用 `split(",")`：URL 的 query 里可以有逗号，JSON 的 value 里也有。
    从后往前数括号层级，顺手跳过字符串字面量里的括号。
    """
    stripped = text.rstrip()
    if not stripped.endswith("}"):
        return None
    depth = 0
    in_string = False
    index = len(stripped) - 1
    while index >= 0:
        char = stripped[index]
        if in_string:
            if char == '"' and (index == 0 or stripped[index - 1] != "\\"):
                in_string = False
        elif char == '"':
            in_string = True
        elif char == "}":
            depth += 1
        elif char == "{":
            depth -= 1
            if depth == 0:
                return index
        index -= 1
    return None


_IDENT_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-$.")


def _is_number(word: str) -> bool:
    try:
        float(word)
    except ValueError:
        return False
    return True


def _relax(text: str) -> str:
    """把 JS 对象字面量修成合法 JSON：单引号、裸键名、尾逗号。

    Legado 用 GSON 解析这段选项，GSON 默认是**宽松模式** —— `{Referer:''}`、
    `{'method':'POST',}` 在 Legado 里都能跑，所以这不是容错，是对齐行为。
    只在 `json.loads` 失败后才走这条路。
    """
    out: list = []
    index = 0
    total = len(text)
    while index < total:
        char = text[index]

        if char == '"':
            #  已经是合法字符串：原样抄到闭合引号
            out.append(char)
            index += 1
            while index < total:
                out.append(text[index])
                if text[index] == "\\" and index + 1 < total:
                    index += 1
                    out.append(text[index])
                elif text[index] == '"':
                    index += 1
                    break
                index += 1
            continue

        if char == "\\":
            #  字符串**外面**的反斜杠 = 被二次 JSON 编码过了，`{\n "body": …}` 里的
            #  `\n` 是字面两个字符。采集链路上确实这么存的（实测 haitang29）。
            nxt = text[index + 1] if index + 1 < total else ""
            if nxt in "ntr":
                out.append(" ")
                index += 2
            else:
                index += 1  # 丢掉反斜杠，`\"` 就还原成正常的字符串开头
            continue

        if char == "'":
            #  单引号串 → 双引号串
            index += 1
            buf: list = []
            while index < total and text[index] != "'":
                if text[index] == "\\" and index + 1 < total:
                    buf.append(text[index + 1] if text[index + 1] != "'" else "'")
                    index += 2
                    continue
                buf.append(text[index])
                index += 1
            index += 1
            out.append(json.dumps("".join(buf), ensure_ascii=False))
            continue

        if char in _IDENT_CHARS:
            start = index
            while index < total and text[index] in _IDENT_CHARS:
                index += 1
            word = text[start:index]
            after = index
            while after < total and text[after].isspace():
                after += 1
            if after < total and text[after] == ":":
                out.append(json.dumps(word, ensure_ascii=False))  # 键名
            elif word in {"true", "false", "null"} or _is_number(word):
                out.append(word)  # 字面量，别加引号
            else:
                #  裸字符串值：`{method:POST}`。GSON 宽松模式同样当字符串收。
                out.append(json.dumps(word, ensure_ascii=False))
            continue

        if char in "}]":
            #  丢掉尾逗号：`{"a":1,}`
            while out and (out[-1].isspace() or out[-1] == ","):
                if out[-1] == ",":
                    out.pop()
                    break
                out.pop()

        out.append(char)
        index += 1
    return "".join(out)


def loads_lax(text: str) -> Optional[Any]:
    """按 GSON 的宽松程度解析一段 JSON。解析不出来返回 None。"""
    stripped = (text or "").strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass
    try:
        return json.loads(_relax(stripped))
    except json.JSONDecodeError:
        return None


def split_url_options(raw: str) -> Tuple[str, Dict[str, Any]]:
    """把 `url,{options}` 切成 `(url, options)`。没有选项就返回空 dict。"""
    text = (raw or "").strip()
    if not text:
        return "", {}
    start = _find_trailing_json(text)
    if start is None or start == 0:
        return text, {}
    head = text[:start].rstrip()
    if not head.endswith(","):
        # `{...}` 不是独立的选项段（例如 URL 本身以 `}` 结尾）
        return text, {}

    options = loads_lax(text[start:])
    url = head[:-1].rstrip()
    if not isinstance(options, dict):
        #  选项段**语法上**确实是选项段（逗号 + 配平的 `{}`），只是内容读不懂。
        #  照样得从 URL 上摘掉：带着 `,{...}` 发出去必然 404（实测 25zw / vipyzw
        #  就是这么挂的）。选项丢掉就丢掉，至少请求本身是对的。
        return url, {}
    return url, options


def parse_url_options(raw: str, *, default_headers: Optional[Dict[str, str]] = None) -> Request:
    """解析一条带选项的 URL 规则成 `Request`。"""
    url, options = split_url_options(raw)

    headers: Dict[str, str] = dict(default_headers or {})
    raw_headers = options.get("headers")
    if isinstance(raw_headers, dict):
        headers.update({str(k): str(v) for k, v in raw_headers.items()})
    elif isinstance(raw_headers, str) and raw_headers.strip():
        #  `"headers":"{Referer:''}"` —— 真实源里很常见的 JS 对象写法，GSON 认。
        #  实在读不懂就不设 header：请求照发，别为一个 Referer 判死整个源。
        parsed = loads_lax(raw_headers)
        if isinstance(parsed, dict):
            headers.update({str(k): str(v) for k, v in parsed.items()})

    body = options.get("body", "")
    if isinstance(body, (dict, list)):
        body = json.dumps(body, ensure_ascii=False)

    method = str(options.get("method") or ("POST" if body else "GET")).upper()

    return Request(
        url=url,
        method=method,
        body=str(body or ""),
        charset=str(options.get("charset") or ""),
        headers=headers,
        retry=_safe_int(options.get("retry")),
        type=str(options.get("type") or ""),
        web_view=bool(options.get("webView") or options.get("webview")),
        js=str(options.get("js") or ""),
        options=options,
    )


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


class StaticFetcher:
    """按 URL 查表的假 fetcher。给离线单测用，也方便在 CLI 里重放抓到的页面。"""

    def __init__(self, pages: Optional[Dict[str, str]] = None):
        self._pages: Dict[str, str] = dict(pages or {})
        self.requests: list = []

    def add(self, url: str, text: str) -> "StaticFetcher":
        self._pages[url] = text
        return self

    def close(self) -> None:
        """没有资源要放，但得有这个方法 —— 调用方对真假 fetcher 一视同仁。"""

    def __enter__(self) -> "StaticFetcher":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def fetch(self, request: Request) -> Page:
        from .errors import FetchError

        self.requests.append(request)
        if request.url not in self._pages:
            raise FetchError(f"StaticFetcher 里没有这个 URL：{request.url}")
        return Page(url=request.url, text=self._pages[request.url], status=200)


__all__ = [
    "Fetcher",
    "Page",
    "Request",
    "StaticFetcher",
    "parse_url_options",
    "split_url_options",
]
