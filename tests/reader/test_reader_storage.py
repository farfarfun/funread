"""阅读端四张表：源偏好、书架、进度、章节缓存。

库由全局 autouse 的 `_isolate_database` 指到临时 SQLite，这里只传 `database_url`
显式一点，不依赖那层兜底。
"""

import pytest

from funread.legado.reader import storage


@pytest.fixture
def db(tmp_path):
    url = f"sqlite:///{tmp_path / 'reader.db'}"
    storage.init_reader_db(database_url=url)
    return url


# ------------------------------------------------------------------ book_key


def test_book_key_is_stable_for_name_and_author():
    assert storage.compute_book_key("剑来", "烽火戏诸侯") == storage.compute_book_key(
        " 剑来 ", " 烽火戏诸侯 "
    )


def test_book_key_distinguishes_missing_author():
    """作者缺失在搜索结果里很常见，不能和「有作者」的同名书撞上。"""
    assert storage.compute_book_key("剑来") != storage.compute_book_key("剑来", "烽火戏诸侯")


def test_book_key_requires_a_name():
    with pytest.raises(ValueError):
        storage.compute_book_key("   ", "   ")


# ------------------------------------------------------------------ 源偏好


def test_upsert_source_prefs_inserts_then_updates(db):
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 1, "name": "甲", "weight": 1}], database_url=db
    )
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 1, "name": "甲改名"}], database_url=db
    )

    rows = storage.list_source_prefs(database_url=db)
    assert len(rows) == 1
    assert rows[0].name == "甲改名"
    #  没给的字段保持原值
    assert rows[0].weight == 1


def test_list_source_prefs_skips_disabled(db):
    storage.upsert_source_prefs(
        [
            {"source_type": "book", "url_id": 1, "enabled": True},
            {"source_type": "book", "url_id": 2, "enabled": False},
        ],
        database_url=db,
    )

    assert [row.url_id for row in storage.list_source_prefs(database_url=db)] == [1]
    assert len(storage.list_source_prefs(enabled_only=False, database_url=db)) == 2


def test_list_source_prefs_is_partitioned_by_type(db):
    storage.upsert_source_prefs(
        [
            {"source_type": "book", "url_id": 1},
            {"source_type": "rss", "url_id": 1},
        ],
        database_url=db,
    )

    assert len(storage.list_source_prefs(source_type="book", database_url=db)) == 1
    assert len(storage.list_source_prefs(source_type="rss", database_url=db)) == 1


def test_proven_sources_sort_ahead_of_untried_ones(db):
    """实跑成功过的必须排最前 —— 这是整个选源策略里唯一可信的信号。

    采集侧的 `status==2`（这里体现为 weight）只说明首页能 GET，实测 150 个这样的
    源里只有 6 个真能搜出书，所以它压不过一次真实的成功。
    """
    storage.upsert_source_prefs(
        [
            {"source_type": "book", "url_id": 1, "weight": 99},
            {"source_type": "book", "url_id": 2, "weight": 0},
        ],
        database_url=db,
    )
    storage.record_source_result("book", 2, ok=True, database_url=db)

    assert [row.url_id for row in storage.list_source_prefs(database_url=db)] == [2, 1]


def test_failures_sort_to_the_back(db):
    storage.upsert_source_prefs(
        [{"source_type": "book", "url_id": 1}, {"source_type": "book", "url_id": 2}],
        database_url=db,
    )
    storage.record_source_result("book", 1, ok=False, error="boom", database_url=db)

    rows = storage.list_source_prefs(database_url=db)
    assert [row.url_id for row in rows] == [2, 1]
    assert rows[1].fail_count == 1
    assert rows[1].last_error == "boom"


def test_success_clears_the_failure_counter(db):
    storage.upsert_source_prefs([{"source_type": "book", "url_id": 1}], database_url=db)
    storage.record_source_result("book", 1, ok=False, error="boom", database_url=db)
    storage.record_source_result("book", 1, ok=True, database_url=db)

    row = storage.list_source_prefs(database_url=db)[0]
    assert row.fail_count == 0
    assert row.last_error is None
    assert row.last_ok_at is not None


def test_failures_do_not_disable_by_default(db):
    """站点临时抽风很常见，自动停用太激进会把能用的源一点点耗光。"""
    storage.upsert_source_prefs([{"source_type": "book", "url_id": 1}], database_url=db)
    for _ in range(5):
        storage.record_source_result("book", 1, ok=False, database_url=db)

    assert storage.list_source_prefs(database_url=db)[0].enabled is True


def test_disable_after_one_failure_for_structural_errors(db):
    """JS 规则这种结构性不支持要立刻停用：下次选它结果完全一样。"""
    storage.upsert_source_prefs([{"source_type": "book", "url_id": 1}], database_url=db)
    storage.record_source_result(
        "book", 1, ok=False, error="需要 JS", disable_after=1, database_url=db
    )

    assert storage.list_source_prefs(database_url=db) == []
    assert storage.list_source_prefs(enabled_only=False, database_url=db)[0].enabled is False


# ------------------------------------------------------------------ 书架


def test_shelf_book_key_derived_from_name_and_author(db):
    book_key = storage.upsert_shelf_book({"name": "剑来", "author": "烽火戏诸侯"}, database_url=db)

    assert book_key == storage.compute_book_key("剑来", "烽火戏诸侯")
    assert storage.get_shelf_book(book_key, database_url=db).name == "剑来"


def test_shelf_upsert_is_idempotent_on_the_same_book(db):
    payload = {"name": "剑来", "author": "烽火戏诸侯", "url_id": 1}
    storage.upsert_shelf_book(payload, database_url=db)
    storage.upsert_shelf_book({**payload, "url_id": 2}, database_url=db)

    books = storage.list_shelf(database_url=db)
    assert len(books) == 1
    assert books[0].url_id == 2


def test_removing_a_book_takes_progress_and_cache_with_it(db):
    """留着就是永远不会被读到的垃圾 —— 而且换源重加同名书时会读到旧缓存。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.save_progress(book_key, chapter_index=3, database_url=db)
    storage.save_cached_chapter(book_key, 3, "u", "第三章", "正文", database_url=db)

    assert storage.remove_shelf_book(book_key, database_url=db) is True
    assert storage.get_progress(book_key, database_url=db) is None
    assert storage.get_cached_chapter(book_key, 3, database_url=db) is None


def test_removing_a_missing_book_is_false_not_an_error(db):
    assert storage.remove_shelf_book("deadbeef", database_url=db) is False


# ------------------------------------------------------------------ 进度


def test_progress_round_trips(db):
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.save_progress(
        book_key, chapter_index=7, chapter_name="第七章", char_offset=120, database_url=db
    )

    progress = storage.get_progress(book_key, database_url=db)
    assert (progress.chapter_index, progress.chapter_name, progress.char_offset) == (
        7,
        "第七章",
        120,
    )


def test_progress_overwrites_rather_than_appends(db):
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.save_progress(book_key, chapter_index=1, database_url=db)
    storage.save_progress(book_key, chapter_index=2, database_url=db)

    assert storage.get_progress(book_key, database_url=db).chapter_index == 2


def test_negative_offset_clamped_to_zero(db):
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.save_progress(book_key, chapter_index=1, char_offset=-5, database_url=db)

    assert storage.get_progress(book_key, database_url=db).char_offset == 0


def test_saving_progress_bumps_the_shelf_entry(db):
    """书架按 updated_at 倒序排，在读的书得被顶上去。"""
    older = storage.upsert_shelf_book({"name": "甲"}, database_url=db)
    storage.upsert_shelf_book({"name": "乙"}, database_url=db)
    storage.save_progress(older, chapter_index=1, database_url=db)

    assert [book.name for book in storage.list_shelf(database_url=db)][0] == "甲"


# ------------------------------------------------------------------ 章节缓存


def test_chapter_cache_round_trips(db):
    storage.save_cached_chapter("bk", 1, "https://x/1", "第一章", "正文内容", database_url=db)

    cached = storage.get_cached_chapter("bk", 1, database_url=db)
    assert cached.title == "第一章"
    assert cached.content == "正文内容"


def test_empty_content_is_not_cached(db):
    """缓存一次失败的抓取 = 永久坏掉这一章，之后再也不会重试。"""
    storage.save_cached_chapter("bk", 1, "https://x/1", "第一章", "   ", database_url=db)

    assert storage.get_cached_chapter("bk", 1, database_url=db) is None


def test_recaching_overwrites(db):
    storage.save_cached_chapter("bk", 1, "u", "第一章", "旧", database_url=db)
    storage.save_cached_chapter("bk", 1, "u", "第一章", "新", database_url=db)

    assert storage.get_cached_chapter("bk", 1, database_url=db).content == "新"


def test_cached_indexes_are_sorted(db):
    for index in (3, 1, 2):
        storage.save_cached_chapter("bk", index, "u", "t", "正文", database_url=db)

    assert storage.list_cached_chapter_indexes("bk", database_url=db) == [1, 2, 3]


def test_cache_is_scoped_per_book(db):
    storage.save_cached_chapter("a", 1, "u", "t", "甲", database_url=db)
    storage.save_cached_chapter("b", 1, "u", "t", "乙", database_url=db)

    assert storage.clear_chapter_cache("a", database_url=db) == 1
    assert storage.get_cached_chapter("b", 1, database_url=db).content == "乙"
