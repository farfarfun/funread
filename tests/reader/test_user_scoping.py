"""数据按人隔离、认领无主数据，以及给老表补 `user_id` 的迁移。

账号本身（口令哈希、注册、登录、邀请码）不在这一层 —— 搬到 `funread-api` 的
`accounts` 模块，建在 funauth 上了，对应的测试也跟着搬了过去。这里只认
`user_id` 这个整数，不关心它是谁发的，所以测的是「两个 id 的数据互不可见」。
"""

import pytest
from sqlalchemy import text

from funread.legado.reader import storage
from funread.legado.reader.storage import (
    LOCAL_USER_ID,
    ReaderChapterCache,
    claim_local_data,
    get_article_states,
    get_progress,
    get_session_factory,
    get_shelf_book,
    init_reader_db,
    list_cached_chapter_indexes,
    list_shelf,
    list_subscriptions,
    remove_shelf_book,
    save_cached_chapter,
    save_progress,
    set_article_state,
    upsert_shelf_book,
    upsert_subscription,
)


@pytest.fixture
def db(tmp_path, monkeypatch):
    """每个用例一个全新库。

    `_INITIALIZED_DATABASES` 是按 URL 记的，而 autouse 的 `_isolate_database`
    已经把 URL 指到 tmp_path，所以用例之间天然不串；这里只是把缓存清掉，
    免得同一个 URL 在一次测试会话里被复用时跳过迁移。
    """
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'reader.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    return url


# ---------------------------------------------------------------- 认领无主数据


def test_claim_hands_over_everything_left_by_the_accountless_era(db):
    upsert_shelf_book({"name": "剑来", "author": "烽火戏诸侯"}, database_url=db)
    book_key = storage.compute_book_key("剑来", "烽火戏诸侯")
    save_progress(book_key, chapter_index=7, database_url=db)
    sub_id = upsert_subscription(
        {"kind": "feed", "feed_url": "https://x/atom.xml", "title": "X"}, database_url=db
    )
    set_article_state(sub_id, "a1", read=True, database_url=db)

    moved = claim_local_data(7, database_url=db)

    assert moved == {
        "reader_shelf": 1,
        "reader_progress": 1,
        "reader_rss_subscription": 1,
        "reader_rss_article_state": 1,
    }
    assert list_shelf(LOCAL_USER_ID, database_url=db) == []
    assert [book.name for book in list_shelf(7, database_url=db)] == ["剑来"]
    assert get_progress(book_key, 7, database_url=db).chapter_index == 7
    assert [sub.sub_id for sub in list_subscriptions(7, database_url=db)] == [sub_id]


def test_claim_moves_article_state_with_its_subscription(db):
    """只搬订阅会让已读标记留在 user 0 名下 —— 表现成「订阅还在，文章全变未读」。"""
    sub_id = upsert_subscription(
        {"kind": "feed", "feed_url": "https://x/atom.xml"}, database_url=db
    )
    set_article_state(sub_id, "a1", read=True, database_url=db)

    claim_local_data(7, database_url=db)

    states = get_article_states(sub_id, ["a1"], user_id=7, database_url=db)
    assert states["a1"].read is True
    assert get_article_states(sub_id, ["a1"], user_id=LOCAL_USER_ID, database_url=db) == {}


def test_claim_is_idempotent(db):
    upsert_shelf_book({"name": "剑来"}, database_url=db)
    claim_local_data(7, database_url=db)

    #  第二次已经没有 user 0 的行了，所以一行都搬不走 —— 而且不会把第一个人的
    #  数据再抢给第二个人。
    assert claim_local_data(8, database_url=db) == {
        "reader_shelf": 0,
        "reader_progress": 0,
        "reader_rss_subscription": 0,
        "reader_rss_article_state": 0,
    }
    assert len(list_shelf(7, database_url=db)) == 1
    assert list_shelf(8, database_url=db) == []


# ---------------------------------------------------------------- 按人隔离


def test_shelves_do_not_leak_between_users(db):
    upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    upsert_shelf_book({"name": "雪中悍刀行"}, user_id=2, database_url=db)

    assert [b.name for b in list_shelf(1, database_url=db)] == ["剑来"]
    assert [b.name for b in list_shelf(2, database_url=db)] == ["雪中悍刀行"]


def test_two_users_can_shelve_the_same_book(db):
    key_one = upsert_shelf_book({"name": "剑来", "author": "烽火"}, user_id=1, database_url=db)
    key_two = upsert_shelf_book({"name": "剑来", "author": "烽火"}, user_id=2, database_url=db)
    assert key_one == key_two
    assert get_shelf_book(key_one, 1, database_url=db) is not None
    assert get_shelf_book(key_one, 2, database_url=db) is not None


def test_one_user_cannot_see_another_users_book_by_key(db):
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    assert get_shelf_book(key, 2, database_url=db) is None


def test_progress_is_per_user(db):
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    upsert_shelf_book({"name": "剑来"}, user_id=2, database_url=db)
    save_progress(key, chapter_index=10, user_id=1, database_url=db)
    save_progress(key, chapter_index=99, user_id=2, database_url=db)

    assert get_progress(key, 1, database_url=db).chapter_index == 10
    assert get_progress(key, 2, database_url=db).chapter_index == 99


def test_subscriptions_do_not_leak_between_users(db):
    upsert_subscription({"kind": "feed", "feed_url": "https://a/f"}, user_id=1, database_url=db)
    upsert_subscription({"kind": "feed", "feed_url": "https://b/f"}, user_id=2, database_url=db)

    assert [s.feed_url for s in list_subscriptions(1, database_url=db)] == ["https://a/f"]
    assert [s.feed_url for s in list_subscriptions(2, database_url=db)] == ["https://b/f"]


def test_read_marks_are_per_user(db):
    """同一个源、同一篇文章，一个人读过不代表另一个人读过。"""
    sub_id = upsert_subscription(
        {"kind": "feed", "feed_url": "https://a/f"}, user_id=1, database_url=db
    )
    upsert_subscription({"kind": "feed", "feed_url": "https://a/f"}, user_id=2, database_url=db)
    set_article_state(sub_id, "a1", user_id=1, read=True, database_url=db)

    assert get_article_states(sub_id, ["a1"], user_id=1, database_url=db)["a1"].read is True
    assert get_article_states(sub_id, ["a1"], user_id=2, database_url=db) == {}


def test_payload_cannot_smuggle_a_user_id(db):
    """payload 来自 HTTP 请求体，它说自己是谁不算数。"""
    key = upsert_shelf_book({"name": "剑来", "user_id": 2}, user_id=1, database_url=db)
    assert get_shelf_book(key, 1, database_url=db) is not None
    assert get_shelf_book(key, 2, database_url=db) is None


# ---------------------------------------------------------------- 下架与共享缓存


def test_removing_a_book_only_touches_the_caller(db):
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    upsert_shelf_book({"name": "剑来"}, user_id=2, database_url=db)
    save_progress(key, chapter_index=5, user_id=2, database_url=db)

    assert remove_shelf_book(key, user_id=1, database_url=db) is True

    assert get_shelf_book(key, 1, database_url=db) is None
    assert get_shelf_book(key, 2, database_url=db) is not None
    assert get_progress(key, 2, database_url=db).chapter_index == 5


def test_removing_a_book_someone_else_still_has_keeps_the_shared_cache(db):
    """正文缓存是全局共享的，不能被一个人的下架动作顺手废掉。"""
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    upsert_shelf_book({"name": "剑来"}, user_id=2, database_url=db)
    save_cached_chapter(key, 0, "http://x/1", "第一章", "正文", database_url=db)

    remove_shelf_book(key, user_id=1, database_url=db)
    assert list_cached_chapter_indexes(key, database_url=db) == [0]


def test_the_last_reader_leaving_clears_the_cache(db):
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    save_cached_chapter(key, 0, "http://x/1", "第一章", "正文", database_url=db)

    remove_shelf_book(key, user_id=1, database_url=db)
    assert list_cached_chapter_indexes(key, database_url=db) == []


def test_removing_a_book_you_do_not_have_returns_false(db):
    key = upsert_shelf_book({"name": "剑来"}, user_id=1, database_url=db)
    assert remove_shelf_book(key, user_id=2, database_url=db) is False
    assert get_shelf_book(key, 1, database_url=db) is not None


# ---------------------------------------------------------------- 迁移


def _create_pre_user_tables(url, rows):
    """照 M2 时的形状建 reader_shelf / reader_progress —— 没有 user_id，主键是 book_key。"""
    session_factory = get_session_factory(url)
    with session_factory() as session:
        session.execute(text("DROP TABLE IF EXISTS reader_shelf"))
        session.execute(text("DROP TABLE IF EXISTS reader_progress"))
        session.execute(
            text(
                "CREATE TABLE reader_shelf ("
                " book_key VARCHAR(32) NOT NULL PRIMARY KEY,"
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
                " updated_at DATETIME NOT NULL)"
            )
        )
        session.execute(
            text(
                "CREATE TABLE reader_progress ("
                " book_key VARCHAR(32) NOT NULL PRIMARY KEY,"
                " chapter_index INTEGER NOT NULL DEFAULT 0,"
                " chapter_url VARCHAR(1024) NOT NULL DEFAULT '',"
                " chapter_name VARCHAR(512) NOT NULL DEFAULT '',"
                " char_offset INTEGER NOT NULL DEFAULT 0,"
                " updated_at DATETIME NOT NULL)"
            )
        )
        for key, name in rows:
            session.execute(
                text(
                    "INSERT INTO reader_shelf (book_key, name, created_at, updated_at)"
                    " VALUES (:k, :n, '2026-10-01 00:00:00', '2026-10-01 00:00:00')"
                ),
                {"k": key, "n": name},
            )
            session.execute(
                text(
                    "INSERT INTO reader_progress (book_key, chapter_index, updated_at)"
                    " VALUES (:k, 42, '2026-10-01 00:00:00')"
                ),
                {"k": key},
            )
        session.commit()


def test_migration_moves_existing_rows_to_the_local_user(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'legacy.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    _create_pre_user_tables(url, [("k1", "剑来"), ("k2", "雪中悍刀行")])

    #  模拟进程重启：缓存清掉，init_reader_db 才会真的再跑一次迁移
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    init_reader_db(url)

    books = list_shelf(LOCAL_USER_ID, database_url=url)
    assert sorted(book.name for book in books) == ["剑来", "雪中悍刀行"]
    assert get_progress("k1", LOCAL_USER_ID, database_url=url).chapter_index == 42
    #  旧表搬完就删，不留下两份真相
    session_factory = get_session_factory(url)
    with session_factory() as session:
        names = {
            row[0]
            for row in session.execute(
                text("SELECT name FROM sqlite_master WHERE type='table'")
            ).all()
        }
    assert "reader_shelf__pre_user" not in names
    assert "reader_progress__pre_user" not in names


def test_migration_is_a_no_op_on_an_already_migrated_database(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'fresh.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    upsert_shelf_book({"name": "剑来"}, user_id=3, database_url=url)

    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    init_reader_db(url)

    assert [b.name for b in list_shelf(3, database_url=url)] == ["剑来"]


def test_migration_then_claim_is_the_whole_upgrade_path(tmp_path, monkeypatch):
    """迁移把存量行归给 user 0，认领再把它交给第一个注册的人。两步合起来才完整。"""
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'upgrade.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    _create_pre_user_tables(url, [("k1", "剑来")])
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    init_reader_db(url)

    claim_local_data(1, database_url=url)

    assert [b.name for b in list_shelf(1, database_url=url)] == ["剑来"]
    assert get_progress("k1", 1, database_url=url).chapter_index == 42


def test_chapter_cache_has_no_user_column(db):
    """正文缓存故意保持全局 —— 按人隔离只会让同一章存 N 份。"""
    assert "user_id" not in ReaderChapterCache.__table__.columns
