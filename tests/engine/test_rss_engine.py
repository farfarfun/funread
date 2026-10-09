"""`RssSourceEngine`：分类 → 列表 → 正文，全程离线（注入 `StaticFetcher`）。"""

import pytest

from funread.legado.engine import (
    JsNotSupportedError,
    RssSourceEngine,
    RuleEmptyError,
    StaticFetcher,
    WebViewNotSupportedError,
    absolutize_html,
    load_source,
)

BASE = "https://feed.example.com"

LIST_HTML = """
<html><body>
  <div class="list">
    <div class="item">
      <h2><a href="/a/1">第一篇</a></h2>
      <span class="date">2026-10-01</span>
      <img src="/img/1.png">
      <p class="summary">第一篇摘要</p>
    </div>
    <div class="item">
      <h2><a href="/a/2">第二篇</a></h2>
      <span class="date">2026-10-02</span>
    </div>
    <div class="item">
      <h2><a href="/a/3"></a></h2>
    </div>
  </div>
  <a class="next" href="/list?p=2">下一页</a>
</body></html>
"""

PAGE_TWO_HTML = """
<html><body>
  <div class="list">
    <div class="item"><h2><a href="/a/9">第九篇</a></h2></div>
  </div>
  <a class="next" href="/list?p=2">下一页</a>
</body></html>
"""

ARTICLE_HTML = """
<html><body>
  <h1 class="headline">第一篇</h1>
  <div class="body"><p>正文第一段</p><img src="/img/inline.png"><a href="/a/2">相关</a></div>
</body></html>
"""


def spec(**fields):
    base = {
        "sourceUrl": BASE,
        "sourceName": "示例源",
        "ruleArticles": "class.item",
        "ruleTitle": "tag.h2@text",
        "ruleLink": "tag.a@href",
        "rulePubDate": "class.date@text",
        "ruleImage": "tag.img@src",
        "ruleDescription": "class.summary@text",
    }
    base.update(fields)
    return load_source(base, source_type="rss")


def engine(source=None, pages=None, **kwargs):
    fetcher = StaticFetcher(pages or {BASE: LIST_HTML})
    return RssSourceEngine(source or spec(), fetcher, **kwargs)


# ---------------------------------------------------------------- 分类


def test_categories_without_sort_url_is_the_source_itself():
    kinds = engine().categories()
    assert [(k.name, k.url) for k in kinds] == [("示例源", BASE)]


def test_categories_from_sort_url():
    source = spec(sortUrl=f"头条::{BASE}/top\n科技::{BASE}/tech")
    assert [k.name for k in engine(source).categories()] == ["头条", "科技"]


def test_categories_touches_no_network():
    fetcher = StaticFetcher({})
    RssSourceEngine(spec(), fetcher).categories()
    assert fetcher.requests == []


# ---------------------------------------------------------------- 列表


def test_articles_parses_one_page():
    result = engine().articles()

    assert [a.title for a in result.items] == ["第一篇", "第二篇"]
    first = result.items[0]
    assert first.link == f"{BASE}/a/1"
    assert first.pub_date == "2026-10-01"
    assert first.image == f"{BASE}/img/1.png"
    assert first.description == "第一篇摘要"
    assert first.source_url == BASE
    assert first.source_name == "示例源"


def test_articles_drops_entries_without_a_title():
    """没标题的条目在界面上是一行空白，不如丢掉。"""
    assert len(engine().articles().items) == 2


def test_articles_returns_one_page_and_the_next_url():
    """默认只抓一页：订阅列表是「加载更多」，一次全抓既慢又没人看。"""
    source = spec(ruleNextPage="class.next@href")
    eng = engine(source, pages={BASE: LIST_HTML, f"{BASE}/list?p=2": PAGE_TWO_HTML})
    result = eng.articles()

    assert [a.title for a in result.items] == ["第一篇", "第二篇"]
    assert result.next_url == f"{BASE}/list?p=2"
    #  第二页没被抓 —— 只发了一个请求
    assert len(eng.fetcher.requests) == 1


def test_articles_without_a_next_page_rule_has_no_next_url():
    source = spec()
    assert engine(source).articles().next_url == ""


def test_articles_follows_the_given_next_url():
    source = spec(ruleNextPage="class.next@href")
    eng = engine(source, pages={BASE: LIST_HTML, f"{BASE}/list?p=2": PAGE_TWO_HTML})
    result = eng.articles(next_url=f"{BASE}/list?p=2")
    assert [a.title for a in result.items] == ["第九篇"]


def test_a_self_referential_next_page_is_not_handed_back():
    """真实源里 nextPage 指回当前页是常态，放过去调用方会死循环。"""
    source = spec(ruleNextPage="class.next@href")
    eng = engine(source, pages={f"{BASE}/list?p=2": PAGE_TWO_HTML})
    result = eng.articles(next_url=f"{BASE}/list?p=2")
    assert result.next_url == ""


def test_follow_walks_the_pages_and_stops_on_a_repeat():
    source = spec(ruleNextPage="class.next@href")
    eng = engine(source, pages={BASE: LIST_HTML, f"{BASE}/list?p=2": PAGE_TWO_HTML})
    result = eng.articles(follow=True)
    assert [a.title for a in result.items] == ["第一篇", "第二篇", "第九篇"]
    assert result.next_url == ""


def test_follow_respects_the_page_cap():
    """每页都指向一个新 URL 的源不能把进程拖死。"""
    from funread.legado.engine import MAX_ARTICLE_PAGES

    pages = {}
    for index in range(MAX_ARTICLE_PAGES + 5):
        url = BASE if index == 0 else f"{BASE}/list?p={index}"
        pages[url] = (
            f'<div class="list"><div class="item"><h2><a href="/a/{index}">第{index}篇'
            f'</a></h2></div></div><a class="next" href="/list?p={index + 1}">next</a>'
        )
    source = spec(ruleNextPage="class.next@href")
    result = engine(source, pages=pages).articles(follow=True)
    assert len(result.items) == MAX_ARTICLE_PAGES


def test_an_empty_list_stops_the_walk():
    source = spec(ruleNextPage="class.next@href")
    pages = {
        BASE: LIST_HTML,
        f"{BASE}/list?p=2": '<div class="list"></div><a class="next" href="/list?p=3">n</a>',
    }
    result = engine(source, pages=pages).articles(follow=True)
    assert [a.title for a in result.items] == ["第一篇", "第二篇"]


def test_link_falls_back_to_the_rows_own_href():
    """ruleLink 只有 40.3% 的源有。"""
    source = spec(ruleArticles="class.item@tag.a", ruleTitle="text", ruleLink="")
    result = engine(source).articles()
    assert [a.link for a in result.items] == [f"{BASE}/a/1", f"{BASE}/a/2"]


def test_articles_needs_a_list_rule():
    source = spec(ruleArticles="")
    with pytest.raises(RuleEmptyError, match="ruleArticles"):
        engine(source).articles()


def test_a_category_name_selects_its_url():
    source = spec(sortUrl=f"头条::{BASE}/top\n科技::{BASE}/tech")
    eng = engine(source, pages={f"{BASE}/tech": LIST_HTML})
    assert len(eng.articles("科技").items) == 2


def test_an_unregistered_url_is_fetched_as_given():
    """sortUrl 不一定穷举了所有入口，拒绝反而把能用的路径堵死。"""
    eng = engine(pages={f"{BASE}/other": LIST_HTML})
    assert len(eng.articles(f"{BASE}/other").items) == 2


# ---------------------------------------------------------------- 正文


def test_article_extracts_the_content_html():
    source = spec(ruleContent="class.body@html")
    eng = engine(source, pages={f"{BASE}/a/1": ARTICLE_HTML})
    article = eng.article(f"{BASE}/a/1")
    assert "正文第一段" in article.content
    assert article.link == f"{BASE}/a/1"


def test_article_falls_back_to_the_description_rule():
    """ruleContent 只有 67.6% 的源有；ruleDescription 只有 2.0%，但有就用。"""
    source = spec(ruleContent="", ruleDescription="class.body@html")
    eng = engine(source, pages={f"{BASE}/a/1": ARTICLE_HTML})
    assert "正文第一段" in eng.article(f"{BASE}/a/1").content


def test_an_article_rule_free_source_says_so():
    """这类源只能看列表，界面该说清楚，不该给一篇空白文章。"""
    source = spec(ruleContent="", ruleDescription="")
    with pytest.raises(RuleEmptyError, match="只能看列表"):
        engine(source).article(f"{BASE}/a/1")


def test_article_needs_a_link():
    with pytest.raises(RuleEmptyError, match="没有文章链接"):
        engine(spec(ruleContent="class.body@html")).article("")


def test_load_with_base_url_absolutises_the_content_links():
    """94.4% 的源开着这个开关。不绝对化，正文里的图片在我们这边全是死的。"""
    source = spec(ruleContent="class.body@html", loadWithBaseUrl=True)
    eng = engine(source, pages={f"{BASE}/a/1": ARTICLE_HTML})
    content = eng.article(f"{BASE}/a/1").content
    assert f"{BASE}/img/inline.png" in content
    assert f"{BASE}/a/2" in content


def test_without_load_with_base_url_the_links_stay_relative():
    source = spec(ruleContent="class.body@html", loadWithBaseUrl=False)
    eng = engine(source, pages={f"{BASE}/a/1": ARTICLE_HTML})
    content = eng.article(f"{BASE}/a/1").content
    assert 'src="/img/inline.png"' in content


# ---------------------------------------------------------------- 不支持的源


def test_a_single_url_source_raises_instead_of_returning_nothing():
    """静默返空会让「这个源不支持」和「今天没更新」长得一模一样。"""
    source = spec(singleUrl=f"{BASE}/page")
    with pytest.raises(WebViewNotSupportedError, match="singleUrl"):
        engine(source).articles()
    with pytest.raises(WebViewNotSupportedError):
        engine(source).article(f"{BASE}/a/1")


def test_allow_web_view_lets_a_single_url_source_through():
    """给将来接上浏览器环境时留的开关。"""
    source = spec(singleUrl=f"{BASE}/page")
    eng = engine(source, allow_web_view=True)
    assert len(eng.articles().items) == 2


def test_a_js_rule_raises_rather_than_silently_returning_nothing():
    source = spec(ruleTitle="@js:title()")
    with pytest.raises(JsNotSupportedError):
        engine(source).articles()


# ---------------------------------------------------------------- absolutize_html


@pytest.mark.parametrize(
    ("html", "expected"),
    [
        ('<img src="/a.png">', "https://h.example.com/a.png"),
        ('<a href="b.html">x</a>', "https://h.example.com/p/b.html"),
        ('<img src="https://cdn.example.com/a.png">', "https://cdn.example.com/a.png"),
    ],
)
def test_absolutize_html(html, expected):
    assert expected in absolutize_html(html, "https://h.example.com/p/page")


def test_absolutize_html_keeps_the_text_intact():
    out = absolutize_html("<p>正文</p><img src='/a.png'>", "https://h.example.com/")
    assert "正文" in out


def test_absolutize_html_is_a_no_op_without_a_base():
    assert absolutize_html("<img src='/a.png'>", "") == "<img src='/a.png'>"


def test_absolutize_html_returns_the_input_when_it_cannot_parse():
    """正文拿不到图片比正文整段丢掉要好得多。"""
    assert absolutize_html("", "https://h.example.com/") == ""
