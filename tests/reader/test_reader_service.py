"""`ReaderService`：聚合搜索、缓存命中、失败记账。注入 `StaticFetcher`，不打网。"""

import json

import pytest

from funread.legado.engine import Chapter, StaticFetcher
from funread.legado.engine.errors import JsNotSupportedError
from funread.legado.reader import ReaderService, SourceRegistry, storage


def _source(host, name):
    return {
        "bookSourceUrl": f"https://{host}",
        "bookSourceName": name,
        "searchUrl": "/s?q={{key}}",
        "ruleSearch": {
            "bookList": "class.r@tag.li",
            "name": "class.n@text",
            "author": "class.a@text",
            "bookUrl": "tag.a@href",
        },
        "ruleToc": {"chapterList": "id.l@tag.a", "chapterName": "text", "chapterUrl": "href"},
        "ruleContent": {"content": "id.c@text"},
    }


def _search_html(rows):
    items = "".join(
        f'<li><span class="n">{name}</span><span class="a">{author}</span>'
        f'<a href="/b/{index}">去</a></li>'
        for index, (name, author) in enumerate(rows)
    )
    return f'<html><body><ul class="r">{items}</ul></body></html>'


def _write(root, url_id, source):
    bucket = (url_id // 100) * 100
    path = root / "book" / "source" / f"{bucket}-{bucket + 100}" / f"{url_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "url_id": url_id,
                "status": 2,
                "available": True,
                "candidate": [{"md5_list": ["m"], "source": source}],
            }
        ),
        encoding="utf-8",
    )


@pytest.fixture
def make_service(tmp_path):
    """装好归档 + 扫描过的 service，fetcher 由测试逐个指定。"""

    def _make(pages, sources, **kwargs):
        hubs = tmp_path / "hubs"
        for url_id, source in sources.items():
            _write(hubs, url_id, source)
        db = f"sqlite:///{tmp_path / 'reader.db'}"
        registry = SourceRegistry(cache_root=str(hubs), database_url=db)
        registry.scan()
        fetcher = StaticFetcher(pages)
        service = ReaderService(
            database_url=db,
            registry=registry,
            fetcher_factory=lambda timeout=None: fetcher,
            **kwargs,
        )
        service.fetcher = fetcher
        return service

    return _make


# ------------------------------------------------------------------ 聚合搜索


def test_search_merges_the_same_book_across_sources(make_service):
    """同一本书在两个源下的 URL 完全不同，但只能出现一条结果。"""
    service = make_service(
        pages={
            "https://a.example.com/s?q=剑来": _search_html([("剑来", "烽火戏诸侯")]),
            "https://b.example.com/s?q=剑来": _search_html([("剑来", "烽火戏诸侯")]),
        },
        sources={1: _source("a.example.com", "甲源"), 2: _source("b.example.com", "乙源")},
    )

    results = service.search("剑来")

    assert len(results) == 1
    assert results[0]["name"] == "剑来"
    assert {item["url_id"] for item in results[0]["sources"]} == {1, 2}


def test_search_keeps_different_books_apart(make_service):
    service = make_service(
        pages={
            "https://a.example.com/s?q=剑": _search_html(
                [("剑来", "烽火戏诸侯"), ("剑道独尊", "青鸾峰上")]
            )
        },
        sources={1: _source("a.example.com", "甲源")},
    )

    assert len(service.search("剑")) == 2


def test_more_sources_sorts_first(make_service):
    """被多个站点同时收录的书通常就是用户要找的那本，也更有换源余地。"""
    service = make_service(
        pages={
            "https://a.example.com/s?q=剑": _search_html([("独苗", "甲"), ("热门", "乙")]),
            "https://b.example.com/s?q=剑": _search_html([("热门", "乙")]),
        },
        sources={1: _source("a.example.com", "甲源"), 2: _source("b.example.com", "乙源")},
    )

    assert service.search("剑")[0]["name"] == "热门"


def test_one_dead_source_does_not_kill_the_search(make_service):
    """候选池里绝大多数源其实已经死了 —— 一个源抛异常就 500 的话这功能等于不存在。"""
    service = make_service(
        pages={"https://a.example.com/s?q=剑来": _search_html([("剑来", "烽火戏诸侯")])},
        sources={
            1: _source("a.example.com", "活源"),
            2: _source("dead.example.com", "死源"),  # StaticFetcher 里没有这个 URL
        },
    )

    results = service.search("剑来")

    assert len(results) == 1
    assert [item["url_id"] for item in results[0]["sources"]] == [1]


def test_all_sources_dead_returns_empty_not_an_error(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})

    assert service.search("剑来") == []


def test_blank_keyword_short_circuits(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})

    assert service.search("   ") == []
    assert service.fetcher.requests == []


# ------------------------------------------------------------------ 失败记账


def test_failure_is_recorded_against_the_source(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})

    service.search("剑来")

    pref = storage.list_source_prefs(enabled_only=False, database_url=service.database_url)[0]
    assert pref.fail_count == 1
    assert pref.last_error


def test_success_is_recorded_against_the_source(make_service):
    service = make_service(
        pages={"https://a.example.com/s?q=剑来": _search_html([("剑来", "烽火戏诸侯")])},
        sources={1: _source("a.example.com", "甲源")},
    )

    service.search("剑来")

    assert storage.list_source_prefs(database_url=service.database_url)[0].last_ok_at is not None


def test_js_source_is_disabled_on_the_spot(make_service, monkeypatch):
    """JS 不支持是结构性的，下次选它结果完全一样，不能只记一次失败。"""
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})

    def _boom(self, *args, **kwargs):
        raise JsNotSupportedError("需要 JS")

    monkeypatch.setattr("funread.legado.engine.BookSourceEngine.search", _boom)
    service.search("剑来")

    assert storage.list_source_prefs(database_url=service.database_url) == []


# ------------------------------------------------------------------ 正文缓存


def test_content_is_served_from_cache_without_fetching(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=service.database_url)
    storage.save_cached_chapter(
        book_key,
        1,
        "https://a.example.com/c/1",
        "第一章",
        "缓存正文",
        database_url=service.database_url,
    )

    result = service.content(
        1, Chapter(index=1, url="https://a.example.com/c/1"), book_key=book_key
    )

    assert result.text == "缓存正文"
    assert service.fetcher.requests == []


def test_fetched_content_is_written_to_cache(make_service):
    service = make_service(
        pages={"https://a.example.com/c/1": '<div id="c">新抓的正文</div>'},
        sources={1: _source("a.example.com", "甲源")},
    )
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=service.database_url)

    service.content(
        1, Chapter(index=1, name="第一章", url="https://a.example.com/c/1"), book_key=book_key
    )

    cached = storage.get_cached_chapter(book_key, 1, database_url=service.database_url)
    assert cached.content == "新抓的正文"


def test_content_without_a_book_key_is_not_cached(make_service):
    """没进书架的书（比如试读）不该污染缓存表。"""
    service = make_service(
        pages={"https://a.example.com/c/1": '<div id="c">正文</div>'},
        sources={1: _source("a.example.com", "甲源")},
    )

    service.content(1, Chapter(index=1, url="https://a.example.com/c/1"))

    factory = storage.get_session_factory(service.database_url)
    with factory() as session:
        assert session.query(storage.ReaderChapterCache).count() == 0


def test_unknown_source_raises_lookup_error(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})

    with pytest.raises(LookupError):
        service.content(999999, Chapter(index=1, url="https://x/1"))


# ------------------------------------------------------------------ 离线下载


def test_download_skips_already_cached_chapters(make_service):
    service = make_service(
        pages={"https://a.example.com/c/2": '<div id="c">第二章正文</div>'},
        sources={1: _source("a.example.com", "甲源")},
    )
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=service.database_url)
    storage.save_cached_chapter(
        book_key, 1, "u", "第一章", "已有", database_url=service.database_url
    )

    stats = service.download_chapters(
        book_key,
        1,
        [
            Chapter(index=1, url="https://a.example.com/c/1"),
            Chapter(index=2, url="https://a.example.com/c/2"),
        ],
        interval=0,
    )

    assert stats == {"total": 2, "downloaded": 1, "cached": 1, "failed": 0}


def test_download_counts_failures_without_raising(make_service):
    """后台任务，一章抓不到不该让整批中断。"""
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=service.database_url)

    stats = service.download_chapters(
        book_key, 1, [Chapter(index=1, url="https://a.example.com/c/1")], interval=0
    )

    assert stats["failed"] == 1


# ------------------------------------------------------------------ 书架


def test_shelf_carries_progress(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    book_key = service.add_to_shelf({"name": "剑来", "author": "烽火戏诸侯"})
    service.save_progress(book_key, chapter_index=5, chapter_name="第五章", char_offset=42)

    item = service.shelf()[0]

    assert item["progress"] == {"chapter_index": 5, "chapter_name": "第五章", "char_offset": 42}


def test_shelf_entry_without_progress_is_none(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    service.add_to_shelf({"name": "剑来"})

    assert service.shelf()[0]["progress"] is None


def test_switching_source_clears_the_cache(make_service):
    """不同源的章节切分方式不同，序号对不上 —— 留着缓存等于串章。"""
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    book_key = service.add_to_shelf({"name": "剑来", "url_id": 1})
    storage.save_cached_chapter(
        book_key, 1, "u", "t", "甲源的第一章", database_url=service.database_url
    )

    service.switch_source(book_key, url_id=2, book_url="https://b.example.com/b/9")

    assert storage.get_cached_chapter(book_key, 1, database_url=service.database_url) is None
    assert storage.get_shelf_book(book_key, database_url=service.database_url).url_id == 2


# ------------------------------------------------------------------ 分波搜索


def _many_sources(count, host_prefix="s"):
    return {i: _source(f"{host_prefix}{i}.example.com", f"源{i}") for i in range(1, count + 1)}


def _pages_for(sources, keyword, rows):
    """给每个源都造一个能搜到 rows 的页面。"""
    return {
        f"https://{host_prefix}/s?q={keyword}": _search_html(rows)
        for host_prefix in (
            source["bookSourceUrl"].removeprefix("https://") for source in sources.values()
        )
    }


def test_search_stops_once_enough_sources_answered(make_service):
    """够了就停 —— 再往下搜只是把同一本书的来源列表堆长。"""
    sources = _many_sources(60)
    service = make_service(
        pages=_pages_for(sources, "剑来", [("剑来", "烽火戏诸侯")]),
        sources=sources,
    )

    report = service.search_report("剑来", enough_hits=5)

    assert report["stopped_by"] == "enough"
    assert report["hits"] >= 5
    #  一波 24 个，所以第一波就够了 —— 不该把 60 个都试完
    assert report["sources_tried"] < 60
    assert report["waves"] == 1


def test_search_walks_more_waves_when_the_early_sources_are_dead(make_service):
    """这是把上限从 12 提到分波搜的全部意义：前面的源死了也要能找到书。"""
    sources = _many_sources(40)
    #  只有第 40 个源有这本书，其余全部抓取失败（StaticFetcher 里没有它们的 URL）
    service = make_service(
        pages={"https://s40.example.com/s?q=仙逆": _search_html([("仙逆", "耳根")])},
        sources=sources,
    )

    report = service.search_report("仙逆", enough_hits=5)

    assert [item["name"] for item in report["items"]] == ["仙逆"]
    assert report["waves"] >= 2
    assert report["failed"] >= 30
    assert report["hits"] == 1


def test_search_respects_the_source_cap(make_service):
    sources = _many_sources(60)
    service = make_service(pages={}, sources=sources)

    report = service.search_report("查无此书", max_sources=30, enough_hits=99)

    assert report["sources_tried"] == 30
    assert report["stopped_by"] == "max_sources"
    assert report["exhausted"] is False


def test_search_reports_an_exhausted_pool(make_service):
    """池子搜完了，「没搜到」才是确定的结论而不是「还没搜到那么深」。"""
    sources = _many_sources(3)
    service = make_service(pages={}, sources=sources)

    report = service.search_report("查无此书", enough_hits=99)

    assert report["exhausted"] is True
    assert report["stopped_by"] == "exhausted"
    assert report["sources_tried"] == 3


def test_search_respects_the_wall_clock_budget(make_service, monkeypatch):
    """超预算是把已有结果返回，不是报错。"""
    sources = _many_sources(60)
    service = make_service(
        pages=_pages_for(sources, "剑来", [("剑来", "烽火戏诸侯")]),
        sources=sources,
    )
    #  预算设成 0：第一波跑完就必然超
    report = service.search_report("剑来", enough_hits=99, budget=0.0)

    assert report["stopped_by"] == "budget"
    assert report["waves"] == 1
    #  已有结果照常返回
    assert [item["name"] for item in report["items"]] == ["剑来"]


def test_search_records_elapsed_and_waves(make_service):
    sources = _many_sources(5)
    service = make_service(
        pages=_pages_for(sources, "剑来", [("剑来", "烽火戏诸侯")]), sources=sources
    )

    report = service.search_report("剑来")

    assert report["elapsed"] >= 0
    assert report["waves"] == 1
    assert report["sources_ok"] == 5


# ------------------------------------------------------------------ 换源列表


def test_sources_for_finds_sources_whose_author_spelling_differs(make_service):
    """按 book_key 精确筛会静默丢掉这些源 —— 而它们往往恰恰是还活着的那批。"""
    service = make_service(
        pages={
            "https://a.example.com/s?q=仙逆": _search_html([("仙逆", "耳根")]),
            #  作者空着
            "https://b.example.com/s?q=仙逆": _search_html([("仙逆", "")]),
            #  作者多了「（著）」
            "https://c.example.com/s?q=仙逆": _search_html([("仙逆", "耳根（著）")]),
        },
        sources={
            1: _source("a.example.com", "甲源"),
            2: _source("b.example.com", "乙源"),
            3: _source("c.example.com", "丙源"),
        },
    )
    book_key = service.add_to_shelf(
        {"name": "仙逆", "author": "耳根", "url_id": 1, "book_url": "https://a.example.com/b/0"}
    )

    report = service.sources_for(book_key)

    assert {item["url_id"] for item in report["items"]} == {1, 2, 3}
    #  精确命中的标出来，但其余的不藏
    exact = {item["url_id"] for item in report["items"] if item["exact"]}
    assert exact == {1}
    #  精确的排前面
    assert report["items"][0]["exact"] is True


def test_sources_for_marks_the_current_source(make_service):
    service = make_service(
        pages={
            "https://a.example.com/s?q=仙逆": _search_html([("仙逆", "耳根")]),
            "https://b.example.com/s?q=仙逆": _search_html([("仙逆", "耳根")]),
        },
        sources={1: _source("a.example.com", "甲源"), 2: _source("b.example.com", "乙源")},
    )
    book_key = service.add_to_shelf(
        {"name": "仙逆", "author": "耳根", "url_id": 2, "book_url": "https://b.example.com/b/0"}
    )

    report = service.sources_for(book_key)
    current = [item for item in report["items"] if item["current"]]

    assert [item["url_id"] for item in current] == [2]
    #  当前在读的排最前 —— 用户要先看到自己现在在哪
    assert report["items"][0]["current"] is True


def test_sources_for_excludes_merely_similar_titles(make_service):
    """搜索是模糊的，「仙逆」会搜出同前缀的别的书，那些不是换源选项。"""
    service = make_service(
        pages={
            "https://a.example.com/s?q=仙逆": _search_html([("仙逆", "耳根")]),
            "https://b.example.com/s?q=仙逆": _search_html([("仙逆之再生", "跟风者")]),
        },
        sources={1: _source("a.example.com", "甲源"), 2: _source("b.example.com", "乙源")},
    )
    book_key = service.add_to_shelf(
        {"name": "仙逆", "author": "耳根", "url_id": 1, "book_url": "https://a.example.com/b/0"}
    )

    report = service.sources_for(book_key)

    assert {item["url_id"] for item in report["items"]} == {1}


def test_sources_for_carries_the_search_stats(make_service):
    """界面要能解释「为什么换源列表是空的」—— 真没有，还是试的源都挂了。"""
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    book_key = service.add_to_shelf(
        {"name": "仙逆", "url_id": 1, "book_url": "https://a.example.com/b/0"}
    )

    report = service.sources_for(book_key)

    assert report["items"] == []
    assert report["sources_tried"] >= 1
    assert report["sources_ok"] == 0
    assert report["exhausted"] is True


def test_sources_for_on_a_book_not_on_the_shelf_raises(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    with pytest.raises(LookupError):
        service.sources_for("不存在的key")


# ------------------------------------------------------------------ 发现页


def _explore_source(host, name, kinds="玄幻::/list/1\n都市::/list/2"):
    source = _source(host, name)
    source["exploreUrl"] = kinds
    source["ruleExplore"] = {
        "bookList": "class.r@tag.li",
        "name": "class.n@text",
        "author": "class.a@text",
        "bookUrl": "tag.a@href",
    }
    return source


def test_explore_sources_lists_only_sources_with_explore_rules(make_service):
    """没有发现页规则的源不该出现在这个列表里 —— 点进去什么都没有。"""
    service = make_service(
        pages={},
        sources={
            1: _explore_source("a.example.com", "有分类的源"),
            2: _source("b.example.com", "只能搜的源"),
        },
    )

    page = service.explore_sources()

    assert [item["name"] for item in page["items"]] == ["有分类的源"]
    assert page["total"] == 1


def test_explore_sources_includes_the_category_names(make_service):
    service = make_service(
        pages={}, sources={1: _explore_source("a.example.com", "甲源")}
    )

    assert service.explore_sources()["items"][0]["kinds"] == ["玄幻", "都市"]


def test_explore_sources_filters_and_paginates(make_service):
    service = make_service(
        pages={},
        sources={
            1: _explore_source("a.example.com", "甲源"),
            2: _explore_source("b.example.com", "乙源"),
        },
    )

    assert service.explore_sources(q="甲")["total"] == 1
    assert service.explore_sources(limit=1)["items"] and len(
        service.explore_sources(limit=1)["items"]
    ) == 1
    assert service.explore_sources(limit=1, offset=9)["items"] == []


def test_explore_kinds_touches_no_network(make_service):
    """exploreUrl 是源 JSON 里的静态字段。"""
    service = make_service(pages={}, sources={1: _explore_source("a.example.com", "甲源")})

    kinds = service.explore_kinds(1)

    assert [k["name"] for k in kinds] == ["玄幻", "都市"]
    #  原样返回源里声明的串，不拼 base_url —— 它可能带 URL 选项，拼的顺序反了
    #  会把选项弄坏。调用方当不透明令牌回传即可。
    assert [k["url"] for k in kinds] == ["/list/1", "/list/2"]
    assert service.fetcher.requests == []


def test_explore_accepts_the_token_as_declared(make_service):
    """分类 URL 原样回传就能用 —— 绝对化由引擎做。"""
    service = make_service(
        pages={"https://a.example.com/list/1": _search_html([("剑来", "烽火")])},
        sources={1: _explore_source("a.example.com", "甲源")},
    )
    token = service.explore_kinds(1)[0]["url"]

    assert [item["name"] for item in service.explore(1, token)] == ["剑来"]


def test_explore_kinds_on_a_source_without_them_raises(make_service):
    service = make_service(pages={}, sources={1: _source("a.example.com", "甲源")})
    with pytest.raises(LookupError, match="没有可浏览的分类"):
        service.explore_kinds(1)


def test_explore_kinds_on_an_unknown_source_raises(make_service):
    service = make_service(pages={}, sources={1: _explore_source("a.example.com", "甲源")})
    with pytest.raises(LookupError):
        service.explore_kinds(999)


def test_explore_returns_books_shaped_like_search_results(make_service):
    """形状和搜索一致，详情页那条链才不必分两种情况处理。"""
    service = make_service(
        pages={
            "https://a.example.com/list/1": _search_html(
                [("剑来", "烽火戏诸侯"), ("雪中悍刀行", "烽火戏诸侯")]
            )
        },
        sources={1: _explore_source("a.example.com", "甲源")},
    )

    items = service.explore(1, "/list/1")

    assert [item["name"] for item in items] == ["剑来", "雪中悍刀行"]
    assert items[0]["book_key"]
    #  浏览是单源行为，所以 sources 恒为一项
    assert items[0]["sources"] == [
        {"url_id": 1, "source_name": "甲源", "book_url": "https://a.example.com/b/0"}
    ]


def test_explore_drops_entries_without_a_name(make_service):
    service = make_service(
        pages={"https://a.example.com/list/1": _search_html([("", "无名"), ("剑来", "烽火")])},
        sources={1: _explore_source("a.example.com", "甲源")},
    )

    assert [item["name"] for item in service.explore(1, "/list/1")] == ["剑来"]


def test_explore_records_the_source_result(make_service):
    """浏览失败也要记进 fail_count —— 它和搜索共用一个候选池排序。"""
    service = make_service(pages={}, sources={1: _explore_source("a.example.com", "甲源")})

    with pytest.raises(Exception):
        service.explore(1, "/list/1")

    pref = storage.list_source_prefs(enabled_only=False, database_url=service.database_url)[0]
    assert pref.fail_count == 1


def test_scan_counts_explore_capable_sources(make_service):
    service = make_service(
        pages={},
        sources={
            1: _explore_source("a.example.com", "甲源"),
            2: _source("b.example.com", "乙源"),
        },
    )
    stats = service.registry.scan()

    assert stats["has_explore"] == 1
    assert stats["enabled"] == 2
