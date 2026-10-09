"""`RssService`：两条腿按 `kind` 分派，全程离线（注入 `StaticFetcher`）。"""

import json

import pytest

from funread.legado.engine import RuleSyntaxError, WebViewNotSupportedError
from funread.legado.reader import storage
from funread.legado.reader.registry import SourceRegistry
from funread.legado.reader.rss_service import RssService

FEED_URL = "https://blog.example.com/feed.xml"
SOURCE_BASE = "https://feed.example.com"

RSS_XML = """<?xml version="1.0"?>
<rss version="2.0"><channel>
  <title>示例博客</title>
  <item><title>第一篇</title><link>https://blog.example.com/1</link>
        <description>摘要一</description></item>
  <item><title>第二篇</title><link>https://blog.example.com/2</link>
        <description>摘要二</description></item>
</channel></rss>
"""

LIST_HTML = """
<div class="list">
  <div class="item"><h2><a href="/a/1">归档第一篇</a></h2><span class="date">2026-10-01</span></div>
  <div class="item"><h2><a href="/a/2">归档第二篇</a></h2></div>
</div>
<a class="next" href="/list?p=2">下一页</a>
"""

ARTICLE_HTML = '<div class="body"><p>归档正文</p></div>'

LEGADO_SOURCE = {
    "sourceUrl": SOURCE_BASE,
    "sourceName": "归档订阅源",
    "sourceGroup": "科技",
    "sourceIcon": f"{SOURCE_BASE}/icon.png",
    "sortUrl": f"头条::{SOURCE_BASE}\n科技::{SOURCE_BASE}/tech",
    "ruleArticles": "class.item",
    "ruleTitle": "tag.h2@text",
    "ruleLink": "tag.a@href",
    "rulePubDate": "class.date@text",
    "ruleContent": "class.body@html",
    "ruleNextPage": "class.next@href",
}

WEB_VIEW_SOURCE = {**LEGADO_SOURCE, "singleUrl": f"{SOURCE_BASE}/page", "sourceName": "WebView 源"}

JS_SOURCE = {
    "sourceUrl": SOURCE_BASE,
    "sourceName": "需要 JS",
    "ruleArticles": "@js:items()",
    "ruleTitle": "text",
}


def _write_source(hub, url_id, payload):
    bucket = (url_id // 100) * 100
    target = hub / "rss" / "source" / f"{bucket}-{bucket + 100}"
    target.mkdir(parents=True, exist_ok=True)
    (target / f"{url_id}.json").write_text(json.dumps(payload), encoding="utf-8")


@pytest.fixture
def hub(tmp_path):
    root = tmp_path / "hubs"
    _write_source(root, 1, LEGADO_SOURCE)
    _write_source(root, 2, WEB_VIEW_SOURCE)
    _write_source(root, 3, JS_SOURCE)
    return root


@pytest.fixture
def service(tmp_path, hub, monkeypatch):
    from funread.legado.engine import StaticFetcher

    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'rss-service.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    storage.init_reader_db(url)

    pages = {
        FEED_URL: RSS_XML,
        SOURCE_BASE: LIST_HTML,
        f"{SOURCE_BASE}/tech": LIST_HTML,
        f"{SOURCE_BASE}/list?p=2": '<div class="list"></div>',
        f"{SOURCE_BASE}/a/1": ARTICLE_HTML,
    }

    def factory(timeout=None):
        return StaticFetcher(dict(pages))

    registry = SourceRegistry(cache_root=str(hub), source_type="rss", database_url=url)
    registry.scan()
    return RssService(database_url=url, registry=registry, fetcher_factory=factory)


# ---------------------------------------------------------------- 源目录


def test_available_sources_lists_only_usable_ones(service):
    """规则不全、需要 JS、singleUrl 型都已被 scan 排除在 enabled 之外。"""
    page = service.available_sources()
    assert [item["name"] for item in page["items"]] == ["归档订阅源"]
    assert page["total"] == 1


def test_available_sources_includes_group_icon_and_categories(service):
    item = service.available_sources()["items"][0]
    assert item["group"] == "科技"
    assert item["icon"] == f"{SOURCE_BASE}/icon.png"
    assert item["categories"] == ["头条", "科技"]


def test_available_sources_filters_by_keyword(service):
    assert service.available_sources(q="归档")["total"] == 1
    assert service.available_sources(q="不存在")["total"] == 0


def test_available_sources_paginates(service):
    page = service.available_sources(limit=1, offset=5)
    assert page["items"] == []
    assert page["total"] == 1


# ---------------------------------------------------------------- 订阅 legado


def test_subscribe_to_an_archived_source(service):
    result = service.subscribe_legado(1, user_id=1)
    assert result["kind"] == "legado"
    assert result["title"] == "归档订阅源"
    assert result["icon"] == f"{SOURCE_BASE}/icon.png"


def test_subscribing_to_a_web_view_source_is_refused_up_front(service):
    """订上一个点开永远是错误提示的源，比当场拒绝要糟得多。"""
    with pytest.raises(WebViewNotSupportedError):
        service.subscribe_legado(2, user_id=1)
    assert service.subscriptions(1) == []


def test_subscribing_to_an_unknown_source_is_a_lookup_error(service):
    with pytest.raises(LookupError):
        service.subscribe_legado(999, user_id=1)


# ---------------------------------------------------------------- 订阅 feed


def test_subscribe_to_a_standard_feed(service):
    result = service.subscribe_feed(FEED_URL, user_id=1)
    assert result["kind"] == "feed"
    #  标题自动从 feed 里取
    assert result["title"] == "示例博客"
    assert result["preview"] == 2
    assert result["last_fetched_at"]


def test_subscribe_feed_honours_an_explicit_title(service):
    assert service.subscribe_feed(FEED_URL, user_id=1, title="我的叫法")["title"] == "我的叫法"


def test_subscribe_feed_rejects_a_non_http_url(service):
    for bad in ("ftp://x/f", "file:///etc/passwd", "javascript:alert(1)", "x"):
        with pytest.raises(RuleSyntaxError, match="http"):
            service.subscribe_feed(bad, user_id=1)


def test_subscribe_feed_validates_before_saving(service, tmp_path, monkeypatch, hub):
    """九成的失败是贴错了地址，当场说清楚比订上之后每次刷新都空着有用。"""
    from funread.legado.engine import StaticFetcher

    def factory(timeout=None):
        return StaticFetcher({"https://blog.example.com/wrong": "<html><h1>首页</h1></html>"})

    monkeypatch.setattr(service, "fetcher_factory", factory)
    with pytest.raises(RuleSyntaxError, match="HTML 页面"):
        service.subscribe_feed("https://blog.example.com/wrong", user_id=1)
    assert service.subscriptions(1) == []


# ---------------------------------------------------------------- 列表


def test_articles_from_a_feed_subscription(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    result = service.articles(sub_id, user_id=1)

    assert [a["title"] for a in result["items"]] == ["第一篇", "第二篇"]
    assert result["items"][0]["read"] is False
    assert result["items"][0]["article_key"]
    #  标准 feed 一次给全部条目，没有翻页
    assert result["next_url"] == ""
    assert result["has_content"] is True


def test_articles_from_a_legado_subscription(service):
    sub_id = service.subscribe_legado(1, user_id=1)["sub_id"]
    result = service.articles(sub_id, user_id=1)

    assert [a["title"] for a in result["items"]] == ["归档第一篇", "归档第二篇"]
    assert result["next_url"] == f"{SOURCE_BASE}/list?p=2"
    assert result["has_content"] is True


def test_legado_articles_respect_the_category(service):
    sub_id = service.subscribe_legado(1, user_id=1)["sub_id"]
    assert len(service.articles(sub_id, user_id=1, category="科技")["items"]) == 2


def test_articles_carry_the_read_state(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    first = service.articles(sub_id, user_id=1)["items"][0]
    service.mark_read(sub_id, 1, first["article_key"])

    refreshed = service.articles(sub_id, user_id=1)["items"][0]
    assert refreshed["read"] is True


def test_articles_respects_the_limit(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    assert len(service.articles(sub_id, user_id=1, limit=1)["items"]) == 1


def test_articles_on_an_unknown_subscription_is_a_lookup_error(service):
    with pytest.raises(LookupError):
        service.articles("nope", user_id=1)


def test_another_users_subscription_is_not_reachable(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    with pytest.raises(LookupError):
        service.articles(sub_id, user_id=2)


def test_a_fetch_failure_is_recorded_on_the_subscription(service, monkeypatch):
    """界面要能解释「为什么这个订阅是空的」，而不是干摆一个空列表。"""
    from funread.legado.engine import StaticFetcher

    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    monkeypatch.setattr(service, "fetcher_factory", lambda timeout=None: StaticFetcher({}))

    with pytest.raises(Exception):
        service.articles(sub_id, user_id=1)

    assert service.subscription(sub_id, 1)["last_error"]


def test_a_successful_fetch_clears_the_previous_error(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    storage.record_subscription_fetch(
        sub_id, user_id=1, error="上次失败了", database_url=service.database_url
    )
    service.articles(sub_id, user_id=1)
    assert service.subscription(sub_id, 1)["last_error"] == ""


# ---------------------------------------------------------------- 分类


def test_categories_for_a_legado_subscription(service):
    sub_id = service.subscribe_legado(1, user_id=1)["sub_id"]
    assert [c["name"] for c in service.categories(sub_id, 1)] == ["头条", "科技"]


def test_a_feed_subscription_has_one_pseudo_category(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    assert len(service.categories(sub_id, 1)) == 1


# ---------------------------------------------------------------- 正文


def test_article_from_a_legado_subscription(service):
    sub_id = service.subscribe_legado(1, user_id=1)["sub_id"]
    article = service.article(sub_id, 1, f"{SOURCE_BASE}/a/1")
    assert "归档正文" in article["content_html"]
    assert article["article_key"]


def test_article_from_a_feed_subscription_comes_from_the_feed(service):
    """feed 的正文在列表里就带着，重抓一次比维护会过期的正文缓存简单。"""
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    article = service.article(sub_id, 1, "https://blog.example.com/1")
    assert article["title"] == "第一篇"
    assert "摘要一" in article["content_html"]


def test_an_article_that_left_the_feed_is_a_lookup_error(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    with pytest.raises(LookupError, match="已不在 feed 里"):
        service.article(sub_id, 1, "https://blog.example.com/999")


# ---------------------------------------------------------------- 状态


def test_mark_read_and_favorite(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    item = service.articles(sub_id, user_id=1)["items"][0]
    service.mark_read(sub_id, 1, item["article_key"], meta={"title": item["title"]})
    service.mark_favorite(sub_id, 1, item["article_key"], meta={"title": item["title"]})

    favorites = service.favorites(1)
    assert [row["title"] for row in favorites] == ["第一篇"]
    assert favorites[0]["read"] is True


def test_mark_all_read(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    items = service.articles(sub_id, user_id=1)["items"]
    keys = [item["article_key"] for item in items]

    assert service.mark_all_read(sub_id, 1, keys) == 2
    assert all(a["read"] for a in service.articles(sub_id, user_id=1)["items"])


def test_read_count_shows_up_on_the_subscription(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    keys = [a["article_key"] for a in service.articles(sub_id, user_id=1)["items"]]
    service.mark_all_read(sub_id, 1, keys)

    assert service.subscriptions(1)[0]["read_count"] == 2


def test_unsubscribe(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    assert service.unsubscribe(sub_id, 1) is True
    assert service.subscriptions(1) == []


def test_unsubscribing_someone_elses_subscription_fails(service):
    sub_id = service.subscribe_feed(FEED_URL, user_id=1)["sub_id"]
    assert service.unsubscribe(sub_id, 2) is False
    assert len(service.subscriptions(1)) == 1
