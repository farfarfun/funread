"""阅读端账号、口令哈希、数据按人隔离，以及给老表补 user_id 的迁移。"""

import pytest
from sqlalchemy import text

from funread.legado.reader import storage
from funread.legado.reader.storage import (
    LOCAL_USER_ID,
    ReaderChapterCache,
    authenticate,
    claim_local_data,
    count_users,
    create_user,
    get_progress,
    get_session_factory,
    get_shelf_book,
    get_user,
    get_user_by_name,
    hash_password,
    init_reader_db,
    list_cached_chapter_indexes,
    list_shelf,
    remove_shelf_book,
    save_cached_chapter,
    save_progress,
    upsert_shelf_book,
    verify_password,
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


# ---------------------------------------------------------------- 口令哈希


def test_hash_and_verify_roundtrip():
    stored = hash_password("correct horse battery")
    assert verify_password("correct horse battery", stored)
    assert not verify_password("wrong", stored)


def test_hash_is_salted_so_two_identical_passwords_differ():
    assert hash_password("same-password") != hash_password("same-password")


def test_hash_records_its_parameters():
    scheme, n, r, p, salt, key = hash_password("x" * 10).split("$")
    assert scheme == "scrypt"
    assert (int(n), int(r), int(p)) == (2**14, 8, 1)
    assert len(bytes.fromhex(salt)) == 16
    assert len(bytes.fromhex(key)) == 32


@pytest.mark.parametrize(
    "stored",
    ["", "not-a-hash", "scrypt$1$2$3", "bcrypt$1$1$1$aa$bb", "scrypt$x$8$1$aa$bb"],
)
def test_verify_never_raises_on_a_broken_hash(stored):
    """哈希串坏了是验证失败，不是 500。"""
    assert verify_password("anything", stored) is False


def test_empty_password_is_refused_at_hash_time():
    with pytest.raises(ValueError):
        hash_password("")


# ---------------------------------------------------------------- 建账号


def test_create_and_fetch_user(db):
    user = create_user("alice", "password123", database_url=db)
    assert user.user_id > 0
    assert user.username == "alice"
    assert user.password_hash.startswith("scrypt$")
    assert get_user_by_name("alice", database_url=db).user_id == user.user_id
    assert get_user(user.user_id, database_url=db).username == "alice"
    assert count_users(database_url=db) == 1


def test_plaintext_password_is_never_stored(db):
    create_user("alice", "password123", database_url=db)
    session_factory = get_session_factory(db)
    with session_factory() as session:
        rows = session.execute(text("SELECT * FROM reader_user")).all()
    assert not any("password123" in str(value) for row in rows for value in row)


def test_duplicate_username_is_rejected(db):
    create_user("alice", "password123", database_url=db)
    with pytest.raises(ValueError, match="已被占用"):
        create_user("alice", "another-password", database_url=db)
    assert count_users(database_url=db) == 1


@pytest.mark.parametrize("username", ["ab", "_leading", "a" * 33, "has space", "", "用户名"])
def test_invalid_usernames_are_rejected(db, username):
    with pytest.raises(ValueError, match="用户名"):
        create_user(username, "password123", database_url=db)


def test_short_password_is_rejected(db):
    with pytest.raises(ValueError, match="口令至少"):
        create_user("alice", "short", database_url=db)


def test_username_is_trimmed(db):
    user = create_user("  alice  ", "password123", database_url=db)
    assert user.username == "alice"


# ---------------------------------------------------------------- 登录


def test_authenticate_accepts_the_right_password(db):
    created = create_user("alice", "password123", database_url=db)
    assert authenticate("alice", "password123", database_url=db).user_id == created.user_id


def test_authenticate_rejects_the_wrong_password(db):
    create_user("alice", "password123", database_url=db)
    assert authenticate("alice", "password124", database_url=db) is None


def test_authenticate_on_an_unknown_user_is_indistinguishable_from_a_bad_password(db):
    create_user("alice", "password123", database_url=db)
    assert authenticate("nobody", "password123", database_url=db) is None
    assert authenticate("alice", "nope", database_url=db) is None


def test_disabled_user_cannot_authenticate(db):
    user = create_user("alice", "password123", database_url=db)
    session_factory = get_session_factory(db)
    with session_factory() as session:
        session.get(storage.ReaderUser, user.user_id).disabled = True
        session.commit()
    assert authenticate("alice", "password123", database_url=db) is None


# ---------------------------------------------------------------- 认领无主数据


def test_first_user_claims_data_left_by_the_accountless_era(db):
    upsert_shelf_book({"name": "剑来", "author": "烽火戏诸侯"}, database_url=db)
    save_progress(storage.compute_book_key("剑来", "烽火戏诸侯"), chapter_index=7, database_url=db)
    assert len(list_shelf(LOCAL_USER_ID, database_url=db)) == 1

    alice = create_user("alice", "password123", database_url=db)

    assert list_shelf(LOCAL_USER_ID, database_url=db) == []
    mine = list_shelf(alice.user_id, database_url=db)
    assert [book.name for book in mine] == ["剑来"]
    assert get_progress(mine[0].book_key, alice.user_id, database_url=db).chapter_index == 7


def test_the_second_user_claims_nothing(db):
    upsert_shelf_book({"name": "剑来"}, database_url=db)
    alice = create_user("alice", "password123", database_url=db)
    bob = create_user("bob", "password123", database_url=db)

    assert len(list_shelf(alice.user_id, database_url=db)) == 1
    assert list_shelf(bob.user_id, database_url=db) == []


def test_claim_is_idempotent(db):
    upsert_shelf_book({"name": "剑来"}, database_url=db)
    alice = create_user("alice", "password123", database_url=db)
    assert claim_local_data(alice.user_id, database_url=db) == {
        "reader_shelf": 0,
        "reader_progress": 0,
    }
    assert len(list_shelf(alice.user_id, database_url=db)) == 1


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


def test_migration_then_first_registration_hands_the_data_over(tmp_path, monkeypatch):
    """迁移 + 认领合起来才是完整的升级路径。"""
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'upgrade.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    _create_pre_user_tables(url, [("k1", "剑来")])
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    init_reader_db(url)

    alice = create_user("alice", "password123", database_url=url)

    assert [b.name for b in list_shelf(alice.user_id, database_url=url)] == ["剑来"]
    assert get_progress("k1", alice.user_id, database_url=url).chapter_index == 42


def test_chapter_cache_has_no_user_column(db):
    """正文缓存故意保持全局 —— 按人隔离只会让同一章存 N 份。"""
    assert "user_id" not in ReaderChapterCache.__table__.columns
