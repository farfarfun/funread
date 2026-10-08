"""`RequestsFetcher`：用假 session 验行为，**不打网**。

重点不是「能发请求」，而是那些出了问题很难查的细节：header 合并顺序、
重定向后的 `page.url`、4xx 不重试 / 5xx 重试、响应体截断。
"""

import pytest
import requests

from funread.legado.engine.errors import FetchError, WebViewNotSupportedError
from funread.legado.engine.fetch import Request
from funread.legado.net import MAX_BODY_BYTES, RequestsFetcher


class FakeResponse:
    def __init__(self, body: bytes, *, status=200, url="", headers=None):
        self.content = body
        self.status_code = status
        self.url = url
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, size):
        for i in range(0, len(self.content), size):
            yield self.content[i : i + size]

    def close(self):
        self.closed = True


class FakeSession:
    """记录调用并按队列返回响应。队列用尽就重复最后一个。"""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.closed = False

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        item = self.responses[min(len(self.calls) - 1, len(self.responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    def close(self):
        self.closed = True


def make(*responses, **kwargs):
    session = FakeSession(*responses)
    return RequestsFetcher(session=session, **kwargs), session


# ------------------------------------------------------------------ 基础


def test_fetch_returns_decoded_page():
    page, _ = make(FakeResponse("剑来".encode(), url="https://x.com/s"))
    result = page.fetch(Request(url="https://x.com/s"))
    assert result.text == "剑来"
    assert result.status == 200
    assert result.ok


def test_page_url_is_post_redirect_url():
    """页内相对链接按 `page.url` 解析；用请求前的地址会在 301 站点上整页算错。"""
    fetcher, _ = make(FakeResponse(b"x", url="https://x.com/final/"))
    page = fetcher.fetch(Request(url="https://x.com/start"))
    assert page.url == "https://x.com/final/"


def test_page_url_falls_back_to_request_url():
    fetcher, _ = make(FakeResponse(b"x", url=""))
    assert fetcher.fetch(Request(url="https://x.com/s")).url == "https://x.com/s"


def test_empty_url_rejected():
    fetcher, session = make(FakeResponse(b"x"))
    with pytest.raises(FetchError):
        fetcher.fetch(Request(url="   "))
    assert session.calls == []


def test_web_view_rejected_before_any_request():
    fetcher, session = make(FakeResponse(b"x"))
    with pytest.raises(WebViewNotSupportedError):
        fetcher.fetch(Request(url="https://x.com", web_view=True))
    assert session.calls == []


def test_web_view_allowed_when_opted_in():
    fetcher, _ = make(FakeResponse(b"ok"), allow_web_view=True)
    assert fetcher.fetch(Request(url="https://x.com", web_view=True)).text == "ok"


# ------------------------------------------------------------------ header


def test_request_headers_override_generated_defaults():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", headers={"User-Agent": "源指定的UA"}))
    assert session.calls[0]["headers"]["User-Agent"] == "源指定的UA"


def test_generated_defaults_kept_when_not_overridden():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", headers={"Referer": "https://x.com"}))
    headers = session.calls[0]["headers"]
    assert headers["Referer"] == "https://x.com"
    assert "User-Agent" in headers


def test_base_headers_not_mutated_between_requests():
    """上一次请求的 header 串到下一次，是最难查的一类串味 bug。"""
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", headers={"X-One": "1"}))
    fetcher.fetch(Request(url="https://x.com"))
    assert "X-One" not in session.calls[1]["headers"]


# ------------------------------------------------------------------ 方法与体


def test_post_body_passed_as_data():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", method="POST", body="kw=剑来"))
    assert session.calls[0]["method"] == "POST"
    assert session.calls[0]["data"] == "kw=剑来"


def test_empty_body_sent_as_none():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com"))
    assert session.calls[0]["data"] is None


def test_form_content_type_set_for_body():
    """requests 只在 data 是 dict 时自动带 Content-Type；我们传的是串，得自己设。

    少了这个头，PHP 站的 `$_POST` 是空的 —— 搜索稳定返回 0 条，还很难查。
    """
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", method="POST", body="kw=1"))
    assert session.calls[0]["headers"]["Content-Type"] == "application/x-www-form-urlencoded"


def test_explicit_content_type_not_overwritten():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(
        Request(
            url="https://x.com",
            method="POST",
            body='{"a":1}',
            headers={"content-type": "application/json"},
        )
    )
    sent = session.calls[0]["headers"]
    assert sent["content-type"] == "application/json"
    assert "Content-Type" not in sent


def test_no_content_type_without_body():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com"))
    assert "Content-Type" not in session.calls[0]["headers"]


# -------------------------------------------------------- 请求侧字符集


def test_body_encoded_with_declared_charset():
    """`charset` 管的是两头：响应怎么解码，**请求怎么编码**。

    只做响应那一头的话，gbk 站的搜索会稳定返回空列表 —— 发出去的就是错的。
    """
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com", method="POST", body="kw=剑来", charset="gbk"))
    assert session.calls[0]["data"] == "kw=剑来".encode("gb18030")


def test_url_percent_encoded_with_declared_charset():
    """requests 一律按 utf-8 编码 URL，gbk 站要的是 `%BD%A3%C0%B4`。"""
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com/s?q=剑来", charset="gbk"))
    assert session.calls[0]["url"] == "https://x.com/s?q=%BD%A3%C0%B4"


def test_utf8_charset_left_to_requests():
    """声明 utf-8 就什么都不做 —— requests 自己的编码已经是对的。"""
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(
        Request(url="https://x.com/s?q=剑来", method="POST", body="kw=剑来", charset="utf-8")
    )
    assert session.calls[0]["url"] == "https://x.com/s?q=剑来"
    assert session.calls[0]["data"] == "kw=剑来"


def test_already_encoded_url_not_double_encoded():
    fetcher, session = make(FakeResponse(b"x"))
    fetcher.fetch(Request(url="https://x.com/s?q=%BD%A3%C0%B4", charset="gbk"))
    assert session.calls[0]["url"] == "https://x.com/s?q=%BD%A3%C0%B4"


# ------------------------------------------------------------------ 编码


def test_declared_charset_used():
    body = "第一章".encode("gb18030")
    fetcher, _ = make(FakeResponse(body, headers={"Content-Type": "text/html"}))
    page = fetcher.fetch(Request(url="https://x.com", charset="gb2312"))
    assert page.text == "第一章"
    assert page.encoding == "gb18030"


def test_content_type_charset_used():
    body = "第一章".encode("gb18030")
    fetcher, _ = make(FakeResponse(body, headers={"Content-Type": "text/html; charset=gbk"}))
    assert fetcher.fetch(Request(url="https://x.com")).text == "第一章"


def test_iso_8859_1_header_ignored_in_favour_of_meta():
    """requests 的 ISO-8859-1 默认值会把整页中文变成乱码，必须跳过它看 meta。"""
    body = '<html><meta charset="gbk"><body>剑来</body></html>'.encode("gb18030")
    fetcher, _ = make(FakeResponse(body, headers={"Content-Type": "text/html; charset=ISO-8859-1"}))
    assert "剑来" in fetcher.fetch(Request(url="https://x.com")).text


# ------------------------------------------------------------------ 错误与重试


def test_404_raises_with_status():
    fetcher, _ = make(FakeResponse(b"nope", status=404))
    with pytest.raises(FetchError) as excinfo:
        fetcher.fetch(Request(url="https://x.com/missing"))
    assert excinfo.value.status == 404
    assert excinfo.value.url == "https://x.com/missing"


def test_4xx_not_retried():
    """4xx 重试没有意义，只会把站点的限流打得更死。"""
    fetcher, session = make(FakeResponse(b"", status=403))
    with pytest.raises(FetchError):
        fetcher.fetch(Request(url="https://x.com", retry=2))
    assert len(session.calls) == 1


def test_5xx_retried_then_succeeds(monkeypatch):
    monkeypatch.setattr("funread.legado.net.fetcher.time.sleep", lambda _: None)
    fetcher, session = make(
        FakeResponse(b"", status=502),
        FakeResponse(b"ok", url="https://x.com"),
    )
    assert fetcher.fetch(Request(url="https://x.com", retry=1)).text == "ok"
    assert len(session.calls) == 2


def test_retry_capped_by_max_retry(monkeypatch):
    monkeypatch.setattr("funread.legado.net.fetcher.time.sleep", lambda _: None)
    fetcher, session = make(FakeResponse(b"", status=500), max_retry=2)
    with pytest.raises(FetchError):
        fetcher.fetch(Request(url="https://x.com", retry=99))
    assert len(session.calls) == 2


def test_network_error_retried_then_raises(monkeypatch):
    monkeypatch.setattr("funread.legado.net.fetcher.time.sleep", lambda _: None)
    fetcher, session = make(requests.ConnectionError("boom"))
    with pytest.raises(FetchError) as excinfo:
        fetcher.fetch(Request(url="https://x.com", retry=2))
    assert "boom" in str(excinfo.value)
    assert len(session.calls) == 3


def test_response_always_closed():
    response = FakeResponse(b"x")
    fetcher, _ = make(response)
    fetcher.fetch(Request(url="https://x.com"))
    assert response.closed


def test_response_closed_even_on_error_status():
    response = FakeResponse(b"x", status=404)
    fetcher, _ = make(response)
    with pytest.raises(FetchError):
        fetcher.fetch(Request(url="https://x.com"))
    assert response.closed


# ------------------------------------------------------------------ 截断


def test_oversized_body_truncated():
    """误抓到视频/下载页时 `response.content` 会直接吃满内存。"""
    fetcher, _ = make(FakeResponse(b"a" * (MAX_BODY_BYTES + 100_000)))
    page = fetcher.fetch(Request(url="https://x.com/big"))
    assert MAX_BODY_BYTES <= len(page.text) < MAX_BODY_BYTES + 64 * 1024


def test_context_manager_closes_session():
    session = FakeSession(FakeResponse(b"x"))
    with RequestsFetcher(session=session):
        pass
    assert session.closed
