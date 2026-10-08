"""URL 尾部 JSON 选项：`url,{"method":"POST",...}`。

最容易出事的是**切分**那一步：URL 的 query 里有逗号、JSON 的 value 里有逗号和
花括号，而 `{{key}}` 本身就以 `}` 结尾。切错了要么丢掉参数，要么把规则当选项。
"""

import pytest

from funread.legado.engine import parse_url_options
from funread.legado.engine.fetch import StaticFetcher, split_url_options

# ------------------------------------------------------------------ 切分


def test_no_options_returns_whole_url():
    assert split_url_options("https://x.com/s?q=1") == ("https://x.com/s?q=1", {})


def test_empty_input():
    assert split_url_options("") == ("", {})


@pytest.mark.parametrize(
    "raw",
    [
        "/search?q={{key}}",
        "/search?q={{key}}&p={{page}}",
        "/api/{{key}}",
    ],
)
def test_template_tail_is_not_options(raw):
    """`{{key}}` 以 `}` 收尾，不能被当成选项 JSON —— 否则搜索 URL 整条作废。

    靠的是「`{` 前面必须紧跟一个逗号」这条约束，不是靠 JSON 合法性。
    """
    assert split_url_options(raw) == (raw, {})


def test_options_split_off():
    assert split_url_options('/s,{"method":"POST"}') == ("/s", {"method": "POST"})


def test_comma_in_query_is_not_a_split_point():
    raw = "https://x.com/s?tags=a,b,c"
    assert split_url_options(raw) == (raw, {})


def test_comma_inside_options_value():
    url, options = split_url_options('/s,{"body":"a=1,b=2","method":"POST"}')
    assert url == "/s"
    assert options == {"body": "a=1,b=2", "method": "POST"}


def test_brace_inside_options_string_literal():
    """选项的字符串字面量里可以有 `}`，括号配平时必须跳过它。"""
    url, options = split_url_options('/s,{"body":"q={{key}}"}')
    assert url == "/s"
    assert options == {"body": "q={{key}}"}


def test_nested_options_object():
    url, options = split_url_options('/s,{"headers":{"X":"1"},"method":"POST"}')
    assert url == "/s"
    assert options["headers"] == {"X": "1"}


def test_template_before_options():
    url, options = split_url_options('/s?q={{key}},{"method":"POST"}')
    assert url == "/s?q={{key}}"
    assert options == {"method": "POST"}


def test_unbalanced_tail_left_in_url():
    raw = '/s,{"method":"POST"'
    assert split_url_options(raw) == (raw, {})


def test_js_object_literal_options_accepted():
    """Legado 用 GSON 解析选项段，GSON 默认宽松：裸键名、裸值、单引号、尾逗号都认。"""
    assert split_url_options("/s,{method:POST}") == ("/s", {"method": "POST"})
    assert split_url_options("/s,{'charset':'gbk',}") == ("/s", {"charset": "gbk"})
    assert split_url_options("/s,{retry:3,webView:true}") == (
        "/s",
        {"retry": 3, "webView": True},
    )


def test_double_encoded_escapes_in_options():
    """采集链路上二次 JSON 编码过的选项段：`\\n` 是字面两个字符，不是换行。

    实测 haitang29 的 searchUrl 就长这样；不还原的话 `method`/`body` 全丢，
    该发的 POST 变成 GET，搜索必然 0 条。
    """
    raw = '/modules/article/search.php,{\\n  "body": "searchkey={{key}}",\\n  "method": "POST"\\n}'
    url, options = split_url_options(raw)
    assert url == "/modules/article/search.php"
    assert options == {"body": "searchkey={{key}}", "method": "POST"}


def test_unreadable_options_still_stripped_from_url():
    """选项段读不懂 → 选项丢掉，但必须从 URL 上摘干净。

    回归：以前整段原样留在 URL 里，于是请求发成 `…/search.php,{` —— 必然 404。
    实测 25zw / vipyzw / haitang29 三个源都是这么挂的。
    """
    assert split_url_options('/s,{"a" "b"}') == ("/s", {})


def test_options_not_an_object_is_tolerated():
    raw = "/s,[1,2]"
    assert split_url_options(raw) == (raw, {})


def test_whitespace_tolerated():
    assert split_url_options('/s , {"method":"POST"}') == ("/s", {"method": "POST"})


def test_url_that_is_only_options():
    """整条就是一个 `{...}`：没有 URL 可切，原样返回。"""
    raw = '{"method":"POST"}'
    assert split_url_options(raw) == (raw, {})


# -------------------------------------------------------------- 选项解析


def test_defaults():
    request = parse_url_options("/s")
    assert request.url == "/s"
    assert request.method == "GET"
    assert request.body == ""
    assert request.charset == ""
    assert request.retry == 0
    assert request.web_view is False
    assert request.headers == {}


def test_method_uppercased():
    assert parse_url_options('/s,{"method":"post"}').method == "POST"


def test_body_present_implies_post():
    """只给 body 不给 method 的源不少，按 Legado 当 POST 处理。"""
    request = parse_url_options('/s,{"body":"kw=x"}')
    assert request.method == "POST"
    assert request.body == "kw=x"


def test_body_as_json_object_serialized():
    request = parse_url_options('/s,{"body":{"kw":"x"},"method":"POST"}')
    assert request.body == '{"kw": "x"}'


def test_body_json_keeps_non_ascii():
    request = parse_url_options('/s,{"body":{"kw":"剑来"}}')
    assert "剑来" in request.body


def test_charset_and_retry():
    request = parse_url_options('/s,{"charset":"gbk","retry":3}')
    assert request.charset == "gbk"
    assert request.retry == 3


def test_retry_as_string():
    assert parse_url_options('/s,{"retry":"2"}').retry == 2


def test_retry_garbage_falls_back_to_zero():
    assert parse_url_options('/s,{"retry":"many"}').retry == 0


def test_headers_merged_over_defaults():
    request = parse_url_options(
        '/s,{"headers":{"Referer":"https://x.com"}}',
        default_headers={"User-Agent": "UA", "Referer": "https://old"},
    )
    assert request.headers["User-Agent"] == "UA"
    assert request.headers["Referer"] == "https://x.com"


def test_headers_as_json_string():
    request = parse_url_options('/s,{"headers":"{\\"X\\":\\"1\\"}"}')
    assert request.headers["X"] == "1"


def test_headers_as_js_object_string():
    """`"headers":"{Referer:''}"` —— 真实源里的常见写法，GSON 认，我们也得认。"""
    request = parse_url_options('/s,{"headers":"{Referer:\'https://x.com\'}"}')
    assert request.headers["Referer"] == "https://x.com"


def test_unreadable_headers_do_not_kill_the_request():
    """header 读不懂就不设 —— 请求照发，别为一个 Referer 判死整个源。"""
    request = parse_url_options('/s,{"headers":"{nope"}')
    assert request.url == "/s"
    assert request.headers == {}


def test_default_headers_copied_not_shared():
    """源级 header 是共享 dict，被某一次请求的选项污染就全局串味了。"""
    shared = {"User-Agent": "UA"}
    request = parse_url_options('/s,{"headers":{"X":"1"}}', default_headers=shared)
    assert request.headers["X"] == "1"
    assert "X" not in shared


def test_web_view_flag_recorded():
    """解析阶段只记下来，拒绝由引擎做 —— 解析器不该知道当前阶段支持什么。"""
    assert parse_url_options('/s,{"webView":true}').web_view is True


def test_web_view_lowercase_spelling():
    assert parse_url_options('/s,{"webview":true}').web_view is True


def test_type_and_js_recorded():
    request = parse_url_options('/s,{"type":"json","js":"var a=1"}')
    assert request.type == "json"
    assert request.js == "var a=1"


def test_raw_options_kept():
    """未识别的选项键要留在 `options` 里，net 层还能用。"""
    request = parse_url_options('/s,{"method":"POST","serverID":7}')
    assert request.options["serverID"] == 7


# -------------------------------------------------------------- StaticFetcher


def test_static_fetcher_records_requests():
    fetcher = StaticFetcher({"https://x.com/s": "<html>ok</html>"})
    page = fetcher.fetch(parse_url_options("https://x.com/s"))
    assert page.ok
    assert page.text == "<html>ok</html>"
    assert [r.url for r in fetcher.requests] == ["https://x.com/s"]


def test_static_fetcher_page_url_is_request_url():
    """页内相对链接按 `page.url` 解析，所以这个字段不能空着。"""
    fetcher = StaticFetcher().add("https://x.com/a/b", "x")
    assert fetcher.fetch(parse_url_options("https://x.com/a/b")).url == "https://x.com/a/b"
