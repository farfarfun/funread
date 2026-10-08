"""`Fetcher` 的 requests 实现。

放在 `net/` 而不是 `engine/` 里，是为了守住分层禁令：`import
funread.legado.engine` 不该把 requests 拉进来，四段流程也才能用假 fetcher
离线单测。引擎只认 `engine.fetch.Fetcher` 协议，这里是它唯一的真实实现。
"""

import time
from typing import Dict, Optional, Union
from urllib.parse import quote, urlsplit, urlunsplit

import requests

from ..engine.errors import FetchError, WebViewNotSupportedError
from ..engine.fetch import Page, Request
from . import charset as charset_mod

#  连接/读取超时。读取给得比连接宽：小说站首字节慢是常态。
DEFAULT_TIMEOUT = (10, 20)
MAX_RETRY = 3

#  响应体上限。章节正文最大也就几百 KB，再大基本是抓错了（视频/下载页）。
MAX_BODY_BYTES = 8 * 1024 * 1024


def _default_headers() -> Dict[str, str]:
    """funfake 生成的浏览器头。拿不到就用一个保守的固定 UA。"""
    try:
        from funfake.headers import Headers

        generated = Headers().generate()
        if isinstance(generated, dict) and generated:
            return {str(k): str(v) for k, v in generated.items()}
    except Exception:  # pragma: no cover - funfake 内部变动不该让抓取挂掉
        pass
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept": "*/*",
        "Connection": "keep-alive",
    }


#  表单 POST 的默认 Content-Type。requests 只在 data 是 dict 时自动带上，
#  我们传的是字符串/字节，必须自己设 —— PHP 站少了这个头就填不出 `$_POST`。
FORM_CONTENT_TYPE = "application/x-www-form-urlencoded"


def encode_url(url: str, charset: str) -> str:
    """把 URL 里的非 ASCII 按源声明的字符集百分号编码。

    requests 一律按 utf-8 编码，但 gbk 站要的是 `%BD%A3%C0%B4` 而不是
    `%E5%89%91%E6%9D%A5` —— 编错了搜索结果必然是空的。`%` 放进 safe 集合，
    已经编码过的串不会被二次编码。
    """
    parts = urlsplit(url)
    path = quote(parts.path, safe="/%:@&=+$,;~!*'()", encoding=charset, errors="replace")
    query = quote(parts.query, safe="%:/?#[]@!$&'()*+,;=~", encoding=charset, errors="replace")
    return urlunsplit((parts.scheme, parts.netloc, path, query, parts.fragment))


class RequestsFetcher:
    """一个源一个实例 —— 它持有这个源的 cookie jar。

    Legado 的很多站点靠「首页下发 cookie、搜索接口校验 cookie」工作，所以
    session 必须按源复用；跨源共用反而会把 A 站的 cookie 发给 B 站。
    """

    def __init__(
        self,
        *,
        session: Optional[requests.Session] = None,
        timeout=DEFAULT_TIMEOUT,
        max_retry: int = MAX_RETRY,
        allow_web_view: bool = False,
    ):
        self.session = session or requests.Session()
        self.timeout = timeout
        self.max_retry = max_retry
        self.allow_web_view = allow_web_view
        self._base_headers = _default_headers()

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "RequestsFetcher":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # ------------------------------------------------------------------ 抓取

    def fetch(self, request: Request) -> Page:
        if request.web_view and not self.allow_web_view:
            raise WebViewNotSupportedError("该请求要求 WebView 渲染，当前不支持", rule=request.url)
        if not request.url.strip():
            raise FetchError("请求 URL 为空")

        #  header 合并顺序：源/规则给的覆盖 funfake 生成的默认值
        headers = dict(self._base_headers)
        headers.update(request.headers or {})

        url, body = self._encode(request)
        if body is not None and not any(k.lower() == "content-type" for k in headers):
            headers["Content-Type"] = FORM_CONTENT_TYPE

        attempts = max(1, min(self.max_retry, (request.retry or 0) + 1))
        last_error: Optional[Exception] = None

        for attempt in range(attempts):
            try:
                response = self.session.request(
                    request.method or "GET",
                    url,
                    data=body,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=True,
                    stream=True,
                )
            except requests.RequestException as exc:
                last_error = exc
                if attempt + 1 < attempts:
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise FetchError(f"请求失败：{exc}", url=request.url) from exc

            try:
                raw = self._read_body(response)
            finally:
                response.close()

            if response.status_code >= 400:
                #  4xx 重试没有意义，只有 5xx 值得再试一次
                if response.status_code >= 500 and attempt + 1 < attempts:
                    last_error = FetchError(
                        f"HTTP {response.status_code}",
                        url=request.url,
                        status=response.status_code,
                    )
                    time.sleep(0.5 * (attempt + 1))
                    continue
                raise FetchError(
                    f"HTTP {response.status_code}",
                    url=request.url,
                    status=response.status_code,
                )

            text, encoding = charset_mod.decode(
                raw,
                declared=request.charset,
                content_type=response.headers.get("Content-Type", ""),
            )
            return Page(
                #  重定向后的最终地址 —— 页内相对链接必须按它解析
                url=response.url or request.url,
                text=text,
                status=response.status_code,
                headers={str(k): str(v) for k, v in response.headers.items()},
                encoding=encoding,
            )

        raise FetchError(f"请求失败：{last_error}", url=request.url)

    def _encode(self, request: Request) -> tuple:
        """按源声明的字符集编码 URL 与请求体。

        Legado 的 `charset` 选项管的是**两头**：响应怎么解码，以及请求怎么编码。
        只做前者的话，gbk 站的搜索会稳定返回空列表 —— 请求发出去的就是错的。
        """
        body: Union[str, bytes, None] = request.body or None
        charset = charset_mod.normalize(request.charset) if request.charset else ""
        if not charset or charset == "utf-8":
            return request.url, body
        url = encode_url(request.url, charset)
        if isinstance(body, str):
            #  原样的字节交给对方，和 Legado 的 `body.toByteArray(charset)` 一致
            body = body.encode(charset, errors="replace")
        return url, body

    def _read_body(self, response: requests.Response) -> bytes:
        """流式读取并截断。`response.content` 对一个误抓的大文件会直接吃满内存。"""
        chunks = []
        total = 0
        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total >= MAX_BODY_BYTES:
                break
        return b"".join(chunks)


__all__ = ["DEFAULT_TIMEOUT", "MAX_BODY_BYTES", "MAX_RETRY", "RequestsFetcher"]
