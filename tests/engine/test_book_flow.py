"""书源四段流程：搜索 → 详情 → 目录 → 正文。

全程用 `StaticFetcher` 离线跑，不打网。重点覆盖三件最容易写错的事：
相对 URL 的 base、翻页终止条件、变量跨阶段传递。
"""

import pytest

from funread.legado.engine import (
    BookSourceEngine,
    Page,
    Request,
    StaticFetcher,
    load_source,
    pick_best,
)
from funread.legado.engine.errors import (
    FetchError,
    RuleEmptyError,
    WebViewNotSupportedError,
)

BASE = "https://example.com"

SEARCH_HTML = """
<html><body><ul class="result">
  <li><a href="/book/1">剑来</a><span class="author">烽火戏诸侯</span></li>
  <li><a href="/book/2">雪中悍刀行</a><span class="author">烽火戏诸侯</span></li>
  <li><span class="author">没名字的脏数据</span></li>
</ul></body></html>
"""

INFO_HTML = """
<html><body>
  <div id="detail" data-bid="1" data-list="list" data-body="content">
    <h1>剑来</h1>
    <p id="intro">少年持剑</p>
    <img id="cover" src="/img/1.jpg">
    <a id="toc" href="/book/1/toc">目录</a>
  </div>
</body></html>
"""

TOC_P1 = """
<html><body>
  <div id="list">
    <a href="/c/1">第一章 惊蛰</a>
    <a href="/c/2">第二章 骤雨</a>
  </div>
  <a id="next" href="/book/1/toc?p=2">下一页</a>
</body></html>
"""

TOC_P2 = """
<html><body>
  <div id="list"><a href="/c/3">第三章 剑气</a></div>
  <a id="next" href="/book/1/toc?p=2">下一页</a>
</body></html>
"""

CONTENT_P1 = """
<html><body><div id="content">正文第一段<br>正文第二段</div>
<a id="cnext" href="/c/1_2">下一页</a></body></html>
"""

CONTENT_P2 = '<html><body><div id="content">正文第三段</div></body></html>'

SOURCE = {
    "bookSourceUrl": BASE,
    "bookSourceName": "示例源",
    "searchUrl": "/search?q={{key}}&p={{page}}",
    "ruleSearch": {
        "bookList": "class.result@tag.li",
        "name": "tag.a@text",
        "bookUrl": "tag.a@href",
        "author": "class.author@text",
    },
    "ruleBookInfo": {
        "init": "id.detail",
        "name": "tag.h1@text",
        "intro": "id.intro@text",
        "coverUrl": "id.cover@src",
        "tocUrl": "id.toc@href",
        "@put": "",
    },
    "ruleToc": {
        "chapterList": "id.list@tag.a",
        "chapterName": "text",
        "chapterUrl": "href",
        "nextTocUrl": "id.next@href",
    },
    "ruleContent": {
        "content": "id.content@textNodes",
        "nextContentUrl": "id.cnext@href",
    },
}


def build(pages=None, source=None):
    fetcher = StaticFetcher(
        pages
        if pages is not None
        else {
            f"{BASE}/search?q=剑来&p=1": SEARCH_HTML,
            f"{BASE}/book/1": INFO_HTML,
            f"{BASE}/book/1/toc": TOC_P1,
            f"{BASE}/book/1/toc?p=2": TOC_P2,
            f"{BASE}/c/1": CONTENT_P1,
            f"{BASE}/c/1_2": CONTENT_P2,
        }
    )
    return BookSourceEngine(load_source(source or SOURCE), fetcher), fetcher


# ------------------------------------------------------------------ 搜索


def test_search_fills_key_and_page_into_url():
    engine, fetcher = build()
    engine.search("剑来")
    assert fetcher.requests[0].url == f"{BASE}/search?q=剑来&p=1"


def test_search_parses_rows():
    engine, _ = build()
    books = engine.search("剑来")
    assert [b.name for b in books] == ["剑来", "雪中悍刀行"]
    assert books[0].author == "烽火戏诸侯"
    assert books[0].source_name == "示例源"


def test_search_drops_unusable_rows():
    """没名字或没链接的行直接丢掉 —— 带着它们往下走只会在详情页报错。"""
    engine, _ = build()
    books = engine.search("剑来")
    assert len(books) == 2
    assert all(b.is_usable for b in books)


def test_search_absolutizes_book_url():
    engine, _ = build()
    assert engine.search("剑来")[0].book_url == f"{BASE}/book/1"


def test_search_without_search_url_raises():
    source = {k: v for k, v in SOURCE.items() if k != "searchUrl"}
    engine, _ = build(source=source)
    with pytest.raises(RuleEmptyError):
        engine.search("剑来")


def test_search_page_parameter():
    pages = {f"{BASE}/search?q=剑来&p=3": SEARCH_HTML}
    engine, fetcher = build(pages=pages)
    engine.search("剑来", page=3)
    assert fetcher.requests[0].url.endswith("p=3")


# ------------------------------------------------------------------ 详情


def test_book_info_uses_init_scope_and_fields():
    engine, _ = build()
    book = engine.search("剑来")[0]
    info = engine.book_info(book)
    assert info.name == "剑来"
    assert info.intro == "少年持剑"
    assert info.cover_url == f"{BASE}/img/1.jpg"
    assert info.toc_url == f"{BASE}/book/1/toc"


def test_book_info_skipped_when_no_rules():
    """没有任何 ruleBookInfo 规则时不该多发一次请求。"""
    source = {k: v for k, v in SOURCE.items() if k != "ruleBookInfo"}
    engine, fetcher = build(source=source)
    book = engine.search("剑来")[0]
    info = engine.book_info(book)
    assert len(fetcher.requests) == 1
    assert info.name == "剑来"
    assert info.toc_url == book.book_url


def test_book_info_falls_back_to_search_fields():
    source = dict(SOURCE)
    source["ruleBookInfo"] = {"init": "id.detail", "intro": "id.intro@text"}
    engine, _ = build(source=source)
    book = engine.search("剑来")[0]
    info = engine.book_info(book)
    assert info.author == "烽火戏诸侯"  # 详情页没给 author，沿用搜索结果


def test_book_info_toc_url_defaults_to_page_url():
    source = dict(SOURCE)
    source["ruleBookInfo"] = {"name": "tag.h1@text"}
    engine, _ = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    assert info.toc_url == f"{BASE}/book/1"


# ------------------------------------------------------------------ 目录


def test_toc_follows_next_page():
    engine, _ = build()
    info = engine.book_info(engine.search("剑来")[0])
    chapters = engine.toc(info)
    assert [c.name for c in chapters] == ["第一章 惊蛰", "第二章 骤雨", "第三章 剑气"]
    assert [c.index for c in chapters] == [0, 1, 2]


def test_toc_stops_on_repeated_url():
    """`nextTocUrl` 指回自己是真实源的常态，只靠「空结果」会死循环。"""
    engine, fetcher = build()
    info = engine.book_info(engine.search("剑来")[0])
    engine.toc(info)
    toc_requests = [r.url for r in fetcher.requests if "toc" in r.url]
    assert toc_requests == [f"{BASE}/book/1/toc", f"{BASE}/book/1/toc?p=2"]


def test_toc_absolutizes_chapter_urls():
    engine, _ = build()
    info = engine.book_info(engine.search("剑来")[0])
    assert engine.toc(info)[0].url == f"{BASE}/c/1"


def test_toc_stops_on_empty_list():
    pages = {
        f"{BASE}/book/1/toc": '<html><body><div id="list"></div>'
        '<a id="next" href="/book/1/toc?p=2"></a></body></html>',
    }
    engine, fetcher = build(pages=pages)
    from funread.legado.engine import BookInfo

    assert engine.toc(BookInfo(toc_url=f"{BASE}/book/1/toc")) == []
    assert len(fetcher.requests) == 1


def test_toc_without_list_rule_raises():
    source = {k: v for k, v in SOURCE.items() if k != "ruleToc"}
    engine, _ = build(source=source)
    from funread.legado.engine import BookInfo

    with pytest.raises(RuleEmptyError):
        engine.toc(BookInfo(toc_url=f"{BASE}/book/1/toc"))


def test_toc_page_limit_is_bounded():
    """构造一个永远翻不完、且每页 URL 都不同的目录，必须被页数上限截断。"""
    from funread.legado.engine import MAX_TOC_PAGES, BookInfo

    class EndlessFetcher:
        def __init__(self):
            self.count = 0

        def fetch(self, request: Request) -> Page:
            self.count += 1
            html = (
                f'<html><body><div id="list"><a href="/c/{self.count}">第{self.count}章</a>'
                f'</div><a id="next" href="/toc?p={self.count + 1}"></a></body></html>'
            )
            return Page(url=request.url, text=html, status=200)

    fetcher = EndlessFetcher()
    engine = BookSourceEngine(load_source(SOURCE), fetcher)
    chapters = engine.toc(BookInfo(toc_url=f"{BASE}/toc?p=1"))
    assert fetcher.count == MAX_TOC_PAGES
    assert len(chapters) == MAX_TOC_PAGES


# ------------------------------------------------------------------ 正文


def test_content_merges_next_pages():
    engine, _ = build()
    info = engine.book_info(engine.search("剑来")[0])
    chapters = engine.toc(info)
    content = engine.content(chapters[0], info=info)
    assert content.text.splitlines() == ["正文第一段", "正文第二段", "正文第三段"]
    assert len(content.pages) == 2
    assert content.title == "第一章 惊蛰"


def test_content_without_next_rule_single_page():
    source = dict(SOURCE)
    source["ruleContent"] = {"content": "id.content@textNodes"}
    engine, fetcher = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    content = engine.content(engine.toc(info)[0], info=info)
    assert content.text.splitlines() == ["正文第一段", "正文第二段"]
    assert sum(1 for r in fetcher.requests if r.url.startswith(f"{BASE}/c/")) == 1


def test_content_replace_regex_applied():
    source = dict(SOURCE)
    source["ruleContent"] = {
        "content": "id.content@textNodes",
        "replaceRegex": "##正文##章节",
    }
    engine, _ = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    content = engine.content(engine.toc(info)[0], info=info)
    assert "章节第一段" in content.text
    assert "正文" not in content.text


def test_content_without_rule_raises():
    source = {k: v for k, v in SOURCE.items() if k != "ruleContent"}
    engine, _ = build(source=source)
    from funread.legado.engine import Chapter

    with pytest.raises(RuleEmptyError):
        engine.content(Chapter(url=f"{BASE}/c/1"))


def test_content_tidies_blank_lines():
    pages = {
        f"{BASE}/c/1": '<html><body><div id="content">  甲  <br><br><br>  乙  </div></body></html>'
    }
    source = dict(SOURCE)
    source["ruleContent"] = {"content": "id.content@textNodes"}
    engine, _ = build(pages=pages, source=source)
    from funread.legado.engine import Chapter

    assert engine.content(Chapter(url=f"{BASE}/c/1")).text == "甲\n乙"


# -------------------------------------------------------- 变量跨阶段传递


def variable_source():
    """`init` 收缩到 `#detail` 后，`data-*` 是这个节点自己的属性（单段 → 属性名）。"""
    source = dict(SOURCE)
    source["ruleBookInfo"] = {
        "init": "id.detail",
        "name": ("tag.h1@text@put:{bid:data-bid}@put:{listId:data-list}@put:{bodyId:data-body}"),
        "tocUrl": "id.toc@href",
    }
    return source


def test_put_captures_variables_in_init_scope():
    engine, _ = build(source=variable_source())
    info = engine.book_info(engine.search("剑来")[0])
    assert info.name == "剑来"
    assert info.variables.get("bid") == "1"
    assert info.variables.get("listId") == "list"


def test_variables_reach_toc_stage():
    """`@get:{}` 先替换进规则串再当规则解析 —— 这是 Legado 动态拼选择器的常用手法。"""
    source = variable_source()
    source["ruleToc"] = dict(SOURCE["ruleToc"], chapterList="id.@get:{listId}@tag.a")
    engine, _ = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    assert [c.name for c in engine.toc(info)] == ["第一章 惊蛰", "第二章 骤雨", "第三章 剑气"]


def test_variables_reach_content_stage():
    """详情页 `@put` 的变量必须一路活到正文阶段 —— 否则「详情里 put、正文里 get」全空。"""
    source = variable_source()
    source["ruleContent"] = {"content": "id.@get:{bodyId}@textNodes"}
    engine, _ = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    content = engine.content(engine.toc(info)[0], info=info)
    assert content.text.splitlines() == ["正文第一段", "正文第二段"]


def test_toc_stage_without_variables_misses():
    """反证：同一条规则在拿不到变量时必须落空，否则上面两条测试什么都没验证。"""
    source = dict(SOURCE)
    source["ruleBookInfo"] = {"init": "id.detail", "tocUrl": "id.toc@href"}
    source["ruleToc"] = dict(SOURCE["ruleToc"], chapterList="id.@get:{listId}@tag.a")
    engine, _ = build(source=source)
    info = engine.book_info(engine.search("剑来")[0])
    assert engine.toc(info) == []


def test_webview_rule_is_rejected():
    """`webView` 在 phase 1 不支持，必须显式报错而不是静默降级成普通 GET。"""
    source = dict(SOURCE)
    source["searchUrl"] = '/search?q={{key}},{"webView":true}'
    engine, _ = build(source=source)
    with pytest.raises(WebViewNotSupportedError):
        engine.search("剑来")


def test_url_options_method_and_body():
    source = dict(SOURCE)
    source["searchUrl"] = '/search,{"method":"POST","body":"kw={{key}}&p={{page}}"}'
    pages = {f"{BASE}/search": SEARCH_HTML}
    engine, fetcher = build(pages=pages, source=source)
    engine.search("剑来", page=2)
    request = fetcher.requests[0]
    assert request.method == "POST"
    assert request.body == "kw=剑来&p=2"
    assert request.url == f"{BASE}/search"


def test_source_header_passed_through():
    source = dict(SOURCE, header={"Referer": BASE})
    engine, fetcher = build(source=source)
    engine.search("剑来")
    assert fetcher.requests[0].headers["Referer"] == BASE


def test_missing_page_raises_fetch_error():
    engine, _ = build(pages={})
    with pytest.raises(FetchError):
        engine.search("剑来")


# ------------------------------------------------------------------ 换源辅助


def test_pick_best_prefers_exact_name_and_author():
    books = engine_books()
    best = pick_best(books, "剑来", "烽火戏诸侯")
    assert best.name == "剑来" and best.author == "烽火戏诸侯"


def test_pick_best_falls_back_to_partial_name():
    books = engine_books()
    assert pick_best(books, "剑").name in {"剑来"}


def test_pick_best_on_empty_list():
    assert pick_best([], "剑来") is None


def engine_books():
    engine, _ = build()
    return engine.search("剑来")
