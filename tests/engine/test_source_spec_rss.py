"""订阅源的归一化：别名、核心字段、needs_js、singleUrl、sortUrl。

这些断言的由来是对 `funread-dat/hubs/rss/source` 全量 1,344 个归档的实测，
字段覆盖率写在各用例的注释里。
"""

import pytest

from funread.legado.engine import RSS_CORE_FIELDS, load_source


def rss(**fields):
    base = {"sourceUrl": "https://feed.example.com", "sourceName": "示例源"}
    base.update(fields)
    return load_source(base, source_type="rss")


# ---------------------------------------------------------------- 顶层字段


def test_rss_top_level_fields():
    spec = rss(
        sourceGroup="科技",
        sourceIcon="https://feed.example.com/icon.png",
        sortUrl="头条::https://feed.example.com/top\n科技::https://feed.example.com/tech",
        loadWithBaseUrl=True,
        enabled=True,
    )
    assert spec.source_type == "rss"
    assert spec.is_rss is True
    assert spec.url == "https://feed.example.com"
    assert spec.name == "示例源"
    assert spec.group == "科技"
    assert spec.icon == "https://feed.example.com/icon.png"
    assert spec.load_with_base_url is True


def test_load_with_base_url_defaults_off_but_is_usually_on():
    """实测 94.4% 的源显式开着它，所以默认值几乎总会被覆盖。"""
    assert rss().load_with_base_url is False
    assert rss(loadWithBaseUrl=True).load_with_base_url is True
    #  源里偶见字符串形态
    assert rss(loadWithBaseUrl="true").load_with_base_url is True
    assert rss(loadWithBaseUrl="false").load_with_base_url is False


# ---------------------------------------------------------------- 规则归一化


def test_flat_rules_land_in_the_rss_group():
    spec = rss(
        ruleArticles="class.item",
        ruleTitle="tag.h2@text",
        ruleLink="tag.a@href",
        rulePubDate="class.date@text",
        ruleImage="tag.img@src",
        ruleDescription="class.summary@text",
        ruleContent="class.body@html",
        ruleNextPage="class.next@href",
    )
    assert spec.rule("ruleRss", "articles") == "class.item"
    assert spec.rule("ruleRss", "title") == "tag.h2@text"
    assert spec.rule("ruleRss", "link") == "tag.a@href"
    assert spec.rule("ruleRss", "pubDate") == "class.date@text"
    assert spec.rule("ruleRss", "image") == "tag.img@src"
    assert spec.rule("ruleRss", "description") == "class.summary@text"
    assert spec.rule("ruleRss", "content") == "class.body@html"
    assert spec.rule("ruleRss", "nextPage") == "class.next@href"


def test_rule_next_page_and_rule_next_articles_are_the_same_key():
    """前者是归档里真正在用的名字（33.4%），后者是我们自家 publish/rss.py 产出的。"""
    assert rss(ruleNextPage="a.next@href").rule("ruleRss", "nextPage") == "a.next@href"
    assert rss(ruleNextArticles="a.more@href").rule("ruleRss", "nextPage") == "a.more@href"


def test_rule_next_page_wins_when_both_are_present():
    """优先级由 _RSS_FLAT_TO_GROUP 的声明顺序定，不随 JSON 键序变。"""
    spec = rss(ruleNextArticles="a.old@href", ruleNextPage="a.new@href")
    assert spec.rule("ruleRss", "nextPage") == "a.new@href"
    #  反过来写也一样
    spec = load_source(
        {
            "sourceUrl": "https://x.example.com",
            "ruleNextPage": "a.new@href",
            "ruleNextArticles": "a.old@href",
        },
        source_type="rss",
    )
    assert spec.rule("ruleRss", "nextPage") == "a.new@href"


def test_nested_rule_rss_group_is_accepted_and_wins():
    spec = rss(
        ruleArticles="class.flat",
        ruleRss={"articles": "class.nested", "title": "h1@text"},
    )
    assert spec.rule("ruleRss", "articles") == "class.nested"
    assert spec.rule("ruleRss", "title") == "h1@text"


def test_nested_group_aliases():
    spec = rss(ruleRss={"list": "class.item", "nextUrl": "a@href", "date": "span@text"})
    assert spec.rule("ruleRss", "articles") == "class.item"
    assert spec.rule("ruleRss", "nextPage") == "a@href"
    assert spec.rule("ruleRss", "pubDate") == "span@text"


def test_an_rss_source_never_grows_book_groups():
    """两套归一化完全分派：RSS 源的 ruleContent 不该变成书源的正文组。"""
    spec = rss(ruleArticles="class.item", ruleContent="class.body@html")
    assert spec.group_rules("ruleToc") == {}
    assert spec.group_rules("ruleContent") == {}
    assert spec.rule("ruleRss", "content") == "class.body@html"


def test_a_book_source_never_grows_the_rss_group():
    spec = load_source(
        {
            "bookSourceUrl": "https://book.example.com",
            "ruleContent": {"content": "class.text@html"},
        },
        source_type="book",
    )
    assert spec.group_rules("ruleRss") == {}
    assert spec.rule("ruleContent", "content") == "class.text@html"


# ---------------------------------------------------------------- 核心字段


def test_rss_core_fields_are_three_not_eight():
    assert len(RSS_CORE_FIELDS) == 3
    assert ("sourceUrl",) in RSS_CORE_FIELDS
    assert ("ruleRss", "articles") in RSS_CORE_FIELDS
    assert ("ruleRss", "title") in RSS_CORE_FIELDS


def test_a_complete_rss_source():
    spec = rss(ruleArticles="class.item", ruleTitle="h2@text")
    assert spec.missing_core_fields() == []
    assert spec.is_complete is True


def test_rule_link_is_not_required():
    """实测只有 40.3% 的源有它，缺了能用列表项自身的 href 兜底。"""
    spec = rss(ruleArticles="class.item", ruleTitle="h2@text")
    assert spec.has_rule("ruleRss", "link") is False
    assert spec.is_complete is True


@pytest.mark.parametrize(
    ("fields", "missing"),
    [
        ({"ruleTitle": "h2@text"}, ["ruleRss.articles"]),
        ({"ruleArticles": "class.item"}, ["ruleRss.title"]),
        ({}, ["ruleRss.articles", "ruleRss.title"]),
    ],
)
def test_incomplete_rss_sources(fields, missing):
    assert rss(**fields).missing_core_fields() == missing


def test_an_rss_source_is_not_measured_against_the_book_core_fields():
    """这是整个订阅源缺口的根因：拿书源那八个字段去量 RSS 源，is_complete
    永远 False，enabled 永远 0，一个订阅源都进不了候选池。"""
    fields = {"ruleArticles": "class.item", "ruleTitle": "h2@text"}
    assert rss(**fields).is_complete is True
    #  同一份数据按书源解析就是不完整的 —— 对比着看才说明问题
    as_book = load_source(
        {"sourceUrl": "https://feed.example.com", **fields}, source_type="book"
    )
    assert as_book.is_complete is False


def test_missing_source_url_is_an_incomplete_source():
    spec = load_source(
        {"ruleArticles": "class.item", "ruleTitle": "h2@text"}, source_type="rss"
    )
    assert "sourceUrl" in spec.missing_core_fields()


# ---------------------------------------------------------------- needs_js


def test_needs_js_scans_the_rss_rules():
    assert rss(ruleArticles="@js:1+1").needs_js() is True
    assert rss(ruleTitle="<js>x</js>").needs_js() is True
    assert rss(ruleContent="class.body@html").needs_js() is False


def test_needs_js_scans_sort_url_and_single_url():
    assert rss(sortUrl="全部::@js:url()").needs_js() is True
    assert rss(singleUrl="@js:build()").needs_js() is True


def test_a_placeholder_template_is_not_js():
    """`{{key}}` 是模板占位，不是 JS 表达式。"""
    assert rss(ruleArticles="class.item", sortUrl="全部::/list?p={{page}}").needs_js() is False


def test_needs_js_ignores_book_rules_on_an_rss_source():
    """RSS 源的 needs_js 不该被一个它根本不用的书源字段拖下水。"""
    spec = rss(ruleArticles="class.item", ruleChapterList="@js:nope")
    assert spec.needs_js() is False


# ---------------------------------------------------------------- singleUrl


def test_single_url_marks_a_web_view_source():
    """占归档 21.9%。不是「规则不全」—— 规则可能齐，但工作方式需要浏览器。"""
    spec = rss(singleUrl="https://feed.example.com/page", ruleArticles="x", ruleTitle="y")
    assert spec.is_web_view is True
    #  规则齐，所以 is_complete 仍然是 True：两件事要分开报给界面
    assert spec.is_complete is True


def test_a_blank_single_url_is_not_a_web_view_source():
    assert rss(singleUrl="   ").is_web_view is False
    assert rss().is_web_view is False


# ---------------------------------------------------------------- sortUrl


def test_sort_url_parses_into_categories():
    spec = rss(
        sortUrl="头条::https://feed.example.com/top\n科技::https://feed.example.com/tech"
    )
    assert [(k.name, k.url) for k in spec.rss_categories] == [
        ("头条", "https://feed.example.com/top"),
        ("科技", "https://feed.example.com/tech"),
    ]


def test_without_sort_url_there_is_one_category_at_the_source_url():
    spec = rss()
    assert [(k.name, k.url) for k in spec.rss_categories] == [
        ("示例源", "https://feed.example.com")
    ]


def test_a_source_without_a_url_has_no_categories():
    spec = load_source({"sourceName": "无地址"}, source_type="rss")
    assert spec.rss_categories == []
