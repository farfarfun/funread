"""候选源池：按 url_id 装载、全量扫描、选源。全部走临时归档目录，不打网。"""

import json
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from funread.legado.manage.source.storage import add_source_detail_url
from funread.legado.reader import SourceRegistry, storage

#  一个字段齐全、不含 JS 的最小可用源
GOOD = {
    "bookSourceUrl": "https://good.example.com",
    "bookSourceName": "好源",
    "searchUrl": "/s?q={{key}}",
    "ruleSearch": {"bookList": "class.r@tag.li", "name": "tag.a@text", "bookUrl": "tag.a@href"},
    "ruleToc": {"chapterList": "id.l@tag.a", "chapterName": "text", "chapterUrl": "href"},
    "ruleContent": {"content": "id.c@html"},
}

#  空壳源：只有 url + name。实测占代表源的 19%，必须被挡在候选池外
SHELL = {"bookSourceUrl": "https://shell.example.com", "bookSourceName": "空壳源"}

#  字段齐全但正文规则要 JS
NEEDS_JS = {
    **GOOD,
    "bookSourceUrl": "https://js.example.com",
    "bookSourceName": "JS源",
    "ruleContent": {"content": "@js:java.ajax(result)"},
}


def _write(root, url_id, source, status=2):
    bucket = (url_id // 100) * 100
    path = root / "book" / "source" / f"{bucket}-{bucket + 100}" / f"{url_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "url_id": url_id,
                "status": status,
                "available": True,
                "candidate": [{"md5_list": ["m"], "source": source}],
            }
        ),
        encoding="utf-8",
    )
    return path


@pytest.fixture
def registry(tmp_path):
    db = f"sqlite:///{tmp_path / 'reader.db'}"
    return SourceRegistry(cache_root=str(tmp_path / "hubs"), database_url=db)


# ------------------------------------------------------------------ 装载


def test_load_spec_by_url_id(registry, tmp_path):
    _write(tmp_path / "hubs", 101, GOOD)
    registry.import_archive()

    spec = registry.load_spec(101)
    assert spec is not None
    assert spec.name == "好源"
    assert spec.is_complete


def test_missing_source_is_none_not_an_error(registry):
    assert registry.load_spec(999999) is None


def test_missing_spec_is_not_cached_across_servers(registry):
    assert registry.load_spec(101) is None
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 101, "source_json": GOOD}],
        database_url=registry.database_url,
    )
    assert registry.load_spec(101) is not None


def test_cached_spec_refreshes_after_ttl(registry, monkeypatch):
    now = [100.0]
    monkeypatch.setattr("funread.legado.reader.registry.time.monotonic", lambda: now[0])
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 101, "source_json": GOOD}],
        database_url=registry.database_url,
    )
    assert registry.load_spec(101).name == "好源"
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 101, "source_json": {**GOOD, "bookSourceName": "新源"}}],
        database_url=registry.database_url,
    )
    assert registry.load_spec(101).name == "好源"
    now[0] += 61
    assert registry.load_spec(101).name == "新源"


def test_scan_refreshes_cached_missing_source(registry, tmp_path):
    assert registry.load_spec(101) is None
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 101, "source_json": GOOD}],
        database_url=registry.database_url,
    )

    registry.scan()

    assert registry.load_spec(101) is not None


# ------------------------------------------------------------------ 扫描


def test_scan_records_static_verdicts(registry, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "funread.legado.reader.registry.default_js_runtime",
        lambda: SimpleNamespace(available=lambda: False),
    )
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    _write(hubs, 2, SHELL)
    _write(hubs, 3, NEEDS_JS)

    stats = registry.import_archive()

    assert stats["scanned"] == 3
    assert stats["complete"] == 2  # GOOD + NEEDS_JS
    assert stats["needs_js"] == 1
    assert stats["enabled"] == 1  # 只有 GOOD


def test_only_complete_js_free_sources_are_enabled(registry, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "funread.legado.reader.registry.default_js_runtime",
        lambda: SimpleNamespace(available=lambda: False),
    )
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    _write(hubs, 2, SHELL)
    _write(hubs, 3, NEEDS_JS)
    registry.import_archive()

    assert [pref.url_id for pref in registry.prefs()] == [1]


def test_scan_enables_js_sources_when_runtime_is_available(registry, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "funread.legado.reader.registry.default_js_runtime",
        lambda: SimpleNamespace(available=lambda: True),
    )
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    _write(hubs, 2, NEEDS_JS)

    assert registry.import_archive()["enabled"] == 2
    assert [pref.url_id for pref in registry.prefs()] == [1, 2]


def test_status_flag_is_only_a_weak_weight(registry, tmp_path):
    """采集侧的 `status==2` 只值一分，绝不当过滤条件。

    那个标记只代表某次 GET 过站点首页（探活任务从头到尾不碰 searchUrl），
    实测 150 个 `status==2` 的源里只有 6 个能真搜出书。
    """
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD, status=3)
    _write(hubs, 2, {**GOOD, "bookSourceUrl": "https://two.example.com"}, status=2)
    registry.import_archive()

    prefs = {pref.url_id: pref for pref in registry.prefs()}
    #  status != 2 的照样进候选池
    assert set(prefs) == {1, 2}
    assert prefs[1].weight == 0
    assert prefs[2].weight == 1


def test_rescan_keeps_live_results(registry, tmp_path):
    """重扫只刷新静态判定，不该把实跑积累下来的 fail_count 抹掉。"""
    _write(tmp_path / "hubs", 1, GOOD)
    registry.import_archive()
    storage.record_source_result(
        "book", 1, ok=False, error="boom", database_url=registry.database_url
    )

    registry.scan()

    pref = storage.list_source_prefs(database_url=registry.database_url)[0]
    assert pref.fail_count == 1
    assert pref.last_error == "boom"


def test_scan_survives_a_corrupt_file(registry, tmp_path):
    """归档有 13k+ 个文件，一个坏文件不能让整次扫描挂掉。"""
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    bad = hubs / "book" / "source" / "0-100" / "2.json"
    bad.write_text("{not json", encoding="utf-8")

    assert registry.import_archive()["enabled"] == 1


def test_scan_on_an_empty_database(registry):
    assert registry.scan() == {
        "scanned": 0,
        "complete": 0,
        "needs_js": 0,
        "web_view": 0,
        "has_explore": 0,
        "enabled": 0,
    }


def test_scan_does_not_import_local_archive(registry, tmp_path):
    _write(tmp_path / "hubs", 1, GOOD)

    assert registry.scan()["scanned"] == 0
    assert registry.load_spec(1) is None
    assert registry.import_archive()["scanned"] == 1
    assert registry.load_spec(1) is not None


def test_stale_metadata_without_rules_is_not_a_candidate(registry):
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 1, "enabled": True, "has_explore": True}],
        database_url=registry.database_url,
    )

    assert registry.prefs(explore_only=True) == []


# ------------------------------------------------------------------ 选源


def test_candidates_survive_removed_archive(registry, tmp_path):
    """扫描后规则应保存在数据库，不再依赖本地归档。"""
    hubs = tmp_path / "hubs"
    path = _write(hubs, 1, GOOD)
    other = _write(hubs, 2, {**GOOD, "bookSourceUrl": "https://two.example.com"})
    registry.import_archive()
    path.unlink()
    other.unlink()
    registry.invalidate()

    assert [url_id for url_id, _ in registry.candidates(limit=5)] == [1, 2]
    assert registry.scan()["scanned"] == 2


def test_saved_rules_take_precedence_over_old_archive(registry, tmp_path):
    path = _write(tmp_path / "hubs", 1, GOOD)
    registry.import_archive()
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 1, "source_json": {**GOOD, "bookSourceName": "新规则"}}],
        database_url=registry.database_url,
    )
    registry.scan()

    assert registry.load_spec(1).name == "新规则"
    assert path.exists()


def test_scan_saves_one_rule_without_ad_comments(registry, tmp_path):
    source = {**GOOD, "exploreUrl": "/browse", "bookSourceComment": "广告" * 1000}
    path = _write(tmp_path / "hubs", 1, source)
    wrapper = json.loads(path.read_text(encoding="utf-8"))
    wrapper["candidate"].append({"source": {**source, "bookSourceName": "重复版本"}})
    path.write_text(json.dumps(wrapper), encoding="utf-8")

    registry.import_archive()

    saved = storage.get_source_json("book", 1, registry.database_url)
    assert "candidate" not in saved
    assert "bookSourceComment" not in saved
    assert saved["searchUrl"] == GOOD["searchUrl"]
    assert saved["exploreUrl"] == "/browse"
    assert registry.load_spec(1).is_complete


def test_missing_source_does_not_create_archive_directories(registry, tmp_path):
    assert registry.load_spec(999999) is None
    assert not (tmp_path / "hubs").exists()


def test_concurrent_collection_uses_distinct_ids(registry):
    def collect(index):
        source = {**GOOD, "bookSourceUrl": f"https://source{index}.example.com"}
        return registry.ingest([[source]])

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(collect, range(20))) == [1] * 20
    assert registry.scan()["scanned"] == 20


def test_ingest_deduplicates_rss_before_writing(registry):
    rss = type(registry)(source_type="rss", database_url=registry.database_url)
    url = "https://feeds.example.com/rss"
    incomplete = {"sourceUrl": url, "sourceName": "重复空壳"}
    complete = {
        "sourceUrl": url,
        "sourceName": "可用订阅",
        "sourceComment": "广告" * 1000,
        "ruleArticles": "class.item",
        "ruleTitle": "tag.h2@text",
    }

    assert rss.ingest([[incomplete, complete, complete]]) == 1
    prefs = storage.list_source_prefs("rss", database_url=registry.database_url)
    assert len(prefs) == 1
    assert prefs[0].name == "可用订阅"
    saved = storage.get_source_json("rss", prefs[0].url_id, registry.database_url)
    assert "sourceComment" not in saved


def test_ingest_keeps_distinct_rss_paths_and_reuses_saved_ids(registry):
    rss = type(registry)(source_type="rss", database_url=registry.database_url)
    base = {
        "sourceName": "订阅",
        "ruleArticles": "class.item",
        "ruleTitle": "tag.h2@text",
    }
    first = {**base, "sourceUrl": "https://feeds.example.com/news"}
    same = {**base, "sourceUrl": "https://FEEDS.example.com/news/#top"}
    second = {**base, "sourceUrl": "https://feeds.example.com/sports"}

    assert rss.ingest([[first, same, second]]) == 2
    before = {pref.url_id for pref in rss.prefs()}
    assert len(before) == 2
    assert rss.ingest([[second, first]]) == 2
    assert {pref.url_id for pref in rss.prefs()} == before


def test_ingest_reuses_legacy_rss_host_id(registry):
    rss = type(registry)(source_type="rss", database_url=registry.database_url)
    legacy = add_source_detail_url("feeds.example.com", "rss", database_url=registry.database_url)
    source = {
        "sourceUrl": "https://feeds.example.com/news",
        "sourceName": "旧订阅",
        "ruleArticles": "class.item",
        "ruleTitle": "tag.h2@text",
    }
    storage.upsert_source_prefs(
        [{"source_type": "rss", "url_id": legacy.id, "source_json": source}],
        database_url=registry.database_url,
    )

    assert rss.ingest([[source]]) == 1
    assert {pref.url_id for pref in rss.prefs()} == {legacy.id}


def test_ingest_prefers_browsable_duplicate(registry):
    plain = {**GOOD, "bookSourceName": "普通版本"}
    browsable = {
        **GOOD,
        "bookSourceName": "可浏览版本",
        "exploreUrl": "/browse",
        "ruleExplore": {"bookList": "class.item"},
    }

    assert registry.ingest([[plain, browsable]]) == 1
    pref = registry.prefs(explore_only=True)[0]
    assert pref.name == "可浏览版本"
    assert registry.load_spec(pref.url_id).explore_url == "/browse"


def test_ingest_excludes_sources_without_parseable_explore_kinds(registry):
    source = {
        **GOOD,
        "exploreUrl": '[{"title":"空分类","url":""}]',
        "ruleExplore": {"bookList": "class.item"},
    }

    assert registry.ingest([[source]]) == 1
    assert registry.prefs(explore_only=True) == []


def test_candidates_respects_the_limit(registry, tmp_path):
    hubs = tmp_path / "hubs"
    for index in range(1, 6):
        _write(hubs, index, {**GOOD, "bookSourceUrl": f"https://s{index}.example.com"})
    registry.import_archive()

    assert len(registry.candidates(limit=3)) == 3
