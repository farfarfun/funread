"""阅读端四张表：源偏好、书架、进度、章节缓存。

库由全局 autouse 的 `_isolate_database` 指到临时 SQLite，这里只传 `database_url`
显式一点，不依赖那层兜底。
"""

import pytest
from sqlalchemy import text

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


# ------------------------------------------------------------------ 书架分组


def test_a_new_book_is_ungrouped(db):
    """未分组是空串而不是 NULL —— 否则「NULL 和空串算不算同一组」要处理两次。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)

    assert storage.get_shelf_book(book_key, database_url=db).group == ""


def test_listing_by_group_is_not_the_same_as_not_filtering(db):
    """`group=""` 是「只看未分组」，`group=None` 是「整个书架」。"""
    grouped = storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=db)
    loose = storage.upsert_shelf_book({"name": "仙逆"}, database_url=db)

    assert {book.book_key for book in storage.list_shelf(database_url=db)} == {grouped, loose}
    assert [book.book_key for book in storage.list_shelf(group="", database_url=db)] == [loose]
    assert [book.book_key for book in storage.list_shelf(group="玄幻", database_url=db)] == [
        grouped
    ]


def test_moving_books_into_a_group_is_one_batch(db):
    first = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    second = storage.upsert_shelf_book({"name": "仙逆"}, database_url=db)
    storage.upsert_shelf_book({"name": "遮天"}, database_url=db)

    assert storage.set_shelf_group([first, second], "玄幻", database_url=db) == 2
    assert len(storage.list_shelf(group="玄幻", database_url=db)) == 2


def test_moving_books_does_not_reorder_the_shelf(db):
    """`updated_at` 排的是「最近在读」。分组是整理，不是阅读。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    before = storage.get_shelf_book(book_key, database_url=db).updated_at

    storage.set_shelf_group([book_key], "玄幻", database_url=db)

    assert storage.get_shelf_book(book_key, database_url=db).updated_at == before


def test_grouping_cannot_reach_another_users_shelf(db):
    mine = storage.upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    theirs = storage.upsert_shelf_book({"name": "剑来"}, user_id=2, database_url=db)

    assert storage.set_shelf_group([mine, theirs], "玄幻", user_id=1, database_url=db) == 1
    assert storage.get_shelf_book(theirs, user_id=2, database_url=db).group == ""


def test_an_empty_key_list_changes_nothing(db):
    """没有 `WHERE book_key IN ()` 兜底的话这会变成「把整个书架都移走」。"""
    book_key = storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=db)

    assert storage.set_shelf_group([], "", database_url=db) == 0
    assert storage.get_shelf_book(book_key, database_url=db).group == "玄幻"


def test_groups_are_listed_with_counts_and_ungrouped_last(db):
    storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=db)
    storage.upsert_shelf_book({"name": "仙逆", "group": "玄幻"}, database_url=db)
    storage.upsert_shelf_book({"name": "活着", "group": "文学"}, database_url=db)
    storage.upsert_shelf_book({"name": "遮天"}, database_url=db)

    assert storage.list_shelf_groups(database_url=db) == [
        {"name": "文学", "count": 1},
        {"name": "玄幻", "count": 2},
        {"name": "", "count": 1},
    ]


def test_groups_are_per_user(db):
    storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, user_id=1, database_url=db)
    storage.upsert_shelf_book({"name": "活着", "group": "文学"}, user_id=2, database_url=db)

    assert [item["name"] for item in storage.list_shelf_groups(user_id=1, database_url=db)] == [
        "玄幻"
    ]


def test_renaming_a_group_moves_its_books(db):
    book_key = storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=db)

    assert storage.rename_shelf_group("玄幻", "东方玄幻", database_url=db) == 1
    assert storage.get_shelf_book(book_key, database_url=db).group == "东方玄幻"


def test_renaming_a_group_to_nothing_dissolves_it_and_keeps_the_books(db):
    """删掉一个标签不该删掉书。"""
    book_key = storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=db)

    assert storage.rename_shelf_group("玄幻", "", database_url=db) == 1
    assert storage.get_shelf_book(book_key, database_url=db) is not None
    assert storage.list_shelf_groups(database_url=db) == [{"name": "", "count": 1}]


def test_renaming_the_ungrouped_pseudo_group_is_refused(db):
    """ "" 不是分组名而是「未分组」这个状态，给它改名等于把散书全塞进一个组。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)

    assert storage.rename_shelf_group("", "玄幻", database_url=db) == 0
    assert storage.get_shelf_book(book_key, database_url=db).group == ""


# ------------------------------------------------------------------ 检查更新


def test_a_successful_check_records_the_count_and_the_latest_chapter(db):
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)

    assert (
        storage.record_update_check(
            book_key, chapter_count=1200, last_chapter="第一千二百章", database_url=db
        )
        is True
    )

    book = storage.get_shelf_book(book_key, database_url=db)
    assert book.chapter_count == 1200
    assert book.last_chapter == "第一千二百章"
    assert book.last_checked_at is not None
    assert book.last_check_error is None


def test_a_failed_check_keeps_the_last_known_count(db):
    """清零会让未读角标凭空消失，看起来像「更新没了」而不是「这次没查到」。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.record_update_check(book_key, chapter_count=1200, database_url=db)

    storage.record_update_check(book_key, error="源站超时", database_url=db)

    book = storage.get_shelf_book(book_key, database_url=db)
    assert book.chapter_count == 1200
    assert book.last_check_error == "源站超时"


def test_a_later_success_clears_the_error(db):
    """留着会让界面一直显示一个已经修好的错误。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    storage.record_update_check(book_key, error="源站超时", database_url=db)

    storage.record_update_check(book_key, chapter_count=1200, database_url=db)

    assert storage.get_shelf_book(book_key, database_url=db).last_check_error is None


def test_checking_updates_does_not_reorder_the_shelf(db):
    """后台跑一轮全量检查不能把书架顺序打乱成「按检查顺序排」。"""
    book_key = storage.upsert_shelf_book({"name": "剑来"}, database_url=db)
    before = storage.get_shelf_book(book_key, database_url=db).updated_at

    storage.record_update_check(book_key, chapter_count=1200, database_url=db)

    assert storage.get_shelf_book(book_key, database_url=db).updated_at == before


def test_checking_a_book_that_left_the_shelf_is_false_not_an_error(db):
    """全量检查是后台任务，中途用户完全可能把某本书下架。"""
    assert storage.record_update_check("deadbeef", chapter_count=10, database_url=db) is False


def test_an_update_check_cannot_reach_another_users_shelf(db):
    storage.upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    book_key = storage.upsert_shelf_book({"name": "剑来"}, user_id=2, database_url=db)

    storage.record_update_check(book_key, user_id=1, chapter_count=1200, database_url=db)

    assert storage.get_shelf_book(book_key, user_id=2, database_url=db).chapter_count == 0


# ------------------------------------------------------------------ 加列迁移


def test_an_existing_shelf_gets_the_new_columns_without_losing_books(tmp_path, monkeypatch):
    """M3d 那版的 `reader_shelf`（有 user_id，没有分组/章节数）就地补列。

    走的是 `ALTER TABLE ADD COLUMN` 而不是重建 —— 顺带验一件容易炸的事：`group`
    在 SQLite 和 MySQL 都是保留字，裸写进 DDL 会直接语法错误。
    """
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'pre-group.db'}"
    storage.init_reader_db(database_url=url)

    session_factory = storage.get_session_factory(url)
    with session_factory() as session:
        session.execute(text("DROP TABLE reader_shelf"))
        session.execute(
            text(
                "CREATE TABLE reader_shelf ("
                " user_id INTEGER NOT NULL,"
                " book_key VARCHAR(32) NOT NULL,"
                " name VARCHAR(255) NOT NULL DEFAULT '',"
                " author VARCHAR(255) NOT NULL DEFAULT '',"
                " cover_url VARCHAR(1024) NOT NULL DEFAULT '',"
                " intro TEXT NOT NULL DEFAULT '',"
                " source_type VARCHAR(32) NOT NULL DEFAULT 'book',"
                " url_id INTEGER NOT NULL DEFAULT 0,"
                " book_url VARCHAR(1024) NOT NULL DEFAULT '',"
                " toc_url VARCHAR(1024) NOT NULL DEFAULT '',"
                " last_chapter VARCHAR(512) NOT NULL DEFAULT '',"
                " created_at DATETIME NOT NULL,"
                " updated_at DATETIME NOT NULL,"
                " PRIMARY KEY (user_id, book_key))"
            )
        )
        session.execute(
            text(
                "INSERT INTO reader_shelf (user_id, book_key, name, created_at, updated_at)"
                " VALUES (7, 'k1', '剑来', '2026-10-01 00:00:00', '2026-10-01 00:00:00')"
            )
        )
        session.commit()

    #  模拟进程重启：缓存清掉，init_reader_db 才会真的再跑一次迁移
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    storage.init_reader_db(database_url=url)

    book = storage.get_shelf_book("k1", user_id=7, database_url=url)
    assert book.name == "剑来"
    assert book.group == ""
    assert book.chapter_count == 0
    assert book.last_checked_at is None
    #  补完列之后就能正常用了，不是只能读
    assert storage.set_shelf_group(["k1"], "玄幻", user_id=7, database_url=url) == 1


def test_adding_columns_twice_is_a_no_op(tmp_path, monkeypatch):
    """每次启动都会跑一遍，第二遍必须啥也不做（`ADD COLUMN` 不是幂等语句）。"""
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'twice.db'}"
    storage.init_reader_db(database_url=url)
    storage.upsert_shelf_book({"name": "剑来", "group": "玄幻"}, database_url=url)

    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    storage.init_reader_db(database_url=url)

    assert storage.list_shelf_groups(database_url=url) == [{"name": "玄幻", "count": 1}]
