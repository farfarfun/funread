"""候选源池：按 url_id 装载、全量扫描、选源。全部走临时归档目录，不打网。"""

import json

import pytest

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

#  字段齐全但正文规则要 JS —— phase 1 跑不了
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

    spec = registry.load_spec(101)
    assert spec is not None
    assert spec.name == "好源"
    assert spec.is_complete


def test_missing_source_is_none_not_an_error(registry):
    assert registry.load_spec(999999) is None


def test_load_spec_is_cached_including_misses(registry, tmp_path):
    """「这个 url_id 没有文件」本身也值得缓存 —— 聚合搜索会反复问同一批 id。"""
    assert registry.load_spec(101) is None
    _write(tmp_path / "hubs", 101, GOOD)
    assert registry.load_spec(101) is None

    registry.invalidate(101)
    assert registry.load_spec(101) is not None


# ------------------------------------------------------------------ 扫描


def test_scan_records_static_verdicts(registry, tmp_path):
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    _write(hubs, 2, SHELL)
    _write(hubs, 3, NEEDS_JS)

    stats = registry.scan()

    assert stats["scanned"] == 3
    assert stats["complete"] == 2  # GOOD + NEEDS_JS
    assert stats["needs_js"] == 1
    assert stats["enabled"] == 1  # 只有 GOOD


def test_only_complete_js_free_sources_are_enabled(registry, tmp_path):
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD)
    _write(hubs, 2, SHELL)
    _write(hubs, 3, NEEDS_JS)
    registry.scan()

    assert [pref.url_id for pref in registry.prefs()] == [1]


def test_status_flag_is_only_a_weak_weight(registry, tmp_path):
    """采集侧的 `status==2` 只值一分，绝不当过滤条件。

    那个标记只代表某次 GET 过站点首页（探活任务从头到尾不碰 searchUrl），
    实测 150 个 `status==2` 的源里只有 6 个能真搜出书。
    """
    hubs = tmp_path / "hubs"
    _write(hubs, 1, GOOD, status=3)
    _write(hubs, 2, {**GOOD, "bookSourceUrl": "https://two.example.com"}, status=2)
    registry.scan()

    prefs = {pref.url_id: pref for pref in registry.prefs()}
    #  status != 2 的照样进候选池
    assert set(prefs) == {1, 2}
    assert prefs[1].weight == 0
    assert prefs[2].weight == 1


def test_rescan_keeps_live_results(registry, tmp_path):
    """重扫只刷新静态判定，不该把实跑积累下来的 fail_count 抹掉。"""
    _write(tmp_path / "hubs", 1, GOOD)
    registry.scan()
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

    assert registry.scan()["enabled"] == 1


def test_scan_on_an_empty_archive(registry):
    assert registry.scan() == {
        "scanned": 0,
        "complete": 0,
        "needs_js": 0,
        "web_view": 0,
        "enabled": 0,
    }


# ------------------------------------------------------------------ 选源


def test_candidates_skip_entries_whose_file_vanished(registry, tmp_path):
    """表里 enabled 但文件没了是会发生的（归档清理 / url_id 重算）。"""
    hubs = tmp_path / "hubs"
    path = _write(hubs, 1, GOOD)
    _write(hubs, 2, {**GOOD, "bookSourceUrl": "https://two.example.com"})
    registry.scan()
    path.unlink()
    registry.invalidate()

    assert [url_id for url_id, _ in registry.candidates(limit=5)] == [2]


def test_candidates_respects_the_limit(registry, tmp_path):
    hubs = tmp_path / "hubs"
    for index in range(1, 6):
        _write(hubs, index, {**GOOD, "bookSourceUrl": f"https://s{index}.example.com"})
    registry.scan()

    assert len(registry.candidates(limit=3)) == 3
