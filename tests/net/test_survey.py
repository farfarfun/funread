"""批量实测的分类与抽样逻辑。离线 —— 真打网的部分不在这里。"""

from funread.legado.engine import (
    FetchError,
    JsNotSupportedError,
    RuleError,
    RuleSyntaxError,
    StaticFetcher,
    WebViewNotSupportedError,
    load_source,
)
from funread.legado.net.survey import classify, run_survey, search_needs_js, summarize


def test_classify_covers_the_table_in_the_tech_debt_doc():
    assert classify([1, 2])[0] == "ok"
    assert classify([])[0] == "empty"
    assert classify(FetchError("x", url="u"))[0] == "net:unreachable"
    assert classify(FetchError("x", url="u", status=503))[0] == "net:http"
    assert classify(JsNotSupportedError("java.foo 没实现"))[0] == "js"
    assert classify(WebViewNotSupportedError("要浏览器"))[0] == "webview"
    assert classify(RuleSyntaxError("选择器写错了"))[0] == "rule"
    assert classify(RuleError("quickjs: SyntaxError（宿主调用失败：java.ajax）"))[0] == "js"
    assert classify(ConnectionResetError("reset"))[0] == "net:other"
    assert classify(ValueError("?"))[0] == "other"


def test_search_needs_js_only_looks_at_the_search_chain():
    base = {
        "bookSourceUrl": "https://a.example.com",
        "bookSourceName": "a",
        "searchUrl": "/s?q={{key}}",
        "ruleSearch": {"bookList": "class.item", "name": "tag.a@text", "bookUrl": "tag.a@href"},
        "ruleBookInfo": {"tocUrl": "@js:result"},
        "ruleToc": {"chapterList": "tag.li", "chapterName": "text", "chapterUrl": "a@href"},
        "ruleContent": {"content": "id.c@html"},
    }
    spec = load_source(base)
    assert spec.needs_js() is True
    assert search_needs_js(spec) is False
    spec = load_source({**base, "ruleSearch": {**base["ruleSearch"], "name": "@js:result"}})
    assert search_needs_js(spec) is True


def test_run_survey_is_offline_capable_and_sorted_by_url_id():
    spec = load_source(
        {
            "bookSourceUrl": "https://a.example.com",
            "bookSourceName": "a",
            "searchUrl": "/s?q={{key}}",
            "ruleSearch": {"bookList": "class.item", "name": "tag.a@text", "bookUrl": "tag.a@href"},
        }
    )
    pages = {"https://a.example.com/s?q=k": '<div class="item"><a href="/b/1">书</a></div>'}
    results = run_survey(
        [(20, spec), (10, spec)], "k", lambda: StaticFetcher(dict(pages)), workers=2
    )
    assert [r["url_id"] for r in results] == [10, 20]
    assert {r["category"] for r in results} == {"ok"}
    assert "成功" in summarize(results) and "样本 2" in summarize(results)
