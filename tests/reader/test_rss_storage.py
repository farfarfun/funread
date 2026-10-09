"""订阅表与文章状态表：幂等订阅、按人隔离、已读/收藏、退订连带清理。"""

import pytest

from funread.legado.reader import storage
from funread.legado.reader.storage import (
    LOCAL_USER_ID,
    compute_article_key,
    compute_sub_id,
    count_read,
    get_article_states,
    get_subscription,
    init_reader_db,
    list_favorites,
    list_subscriptions,
    mark_all_read,
    record_subscription_fetch,
    remove_subscription,
    set_article_state,
    upsert_subscription,
)


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "_INITIALIZED_DATABASES", set())
    url = f"sqlite:///{tmp_path / 'rss.db'}"
    monkeypatch.setenv("FUNREAD_DATABASE_URL", url)
    init_reader_db(url)
    return url


def feed_sub(db, user_id=1, url="https://blog.example.com/feed.xml", **extra):
    return upsert_subscription(
        {"kind": "feed", "feed_url": url, "title": "示例博客", **extra},
        user_id=user_id,
        database_url=db,
    )


# ---------------------------------------------------------------- 标识


def test_sub_id_is_stable_for_the_same_source():
    assert compute_sub_id("feed", "https://x/feed") == compute_sub_id("feed", "https://x/feed")


def test_sub_id_differs_by_kind():
    """同一个标识在两种来源下是两个订阅。"""
    assert compute_sub_id("feed", "123") != compute_sub_id("legado", "123")


def test_sub_id_needs_an_identifier():
    with pytest.raises(ValueError):
        compute_sub_id("feed", "  ")


def test_article_key_is_md5_of_the_link():
    assert compute_article_key("https://x/1") == compute_article_key("https://x/1")
    assert compute_article_key("https://x/1") != compute_article_key("https://x/2")


def test_article_key_needs_a_link():
    with pytest.raises(ValueError):
        compute_article_key("")


# ---------------------------------------------------------------- 订阅


def test_subscribe_and_read_back(db):
    sub_id = feed_sub(db)
    row = get_subscription(sub_id, user_id=1, database_url=db)
    assert row.kind == "feed"
    assert row.feed_url == "https://blog.example.com/feed.xml"
    assert row.title == "示例博客"


def test_subscribing_twice_is_idempotent(db):
    """重复订阅同一个源不该变成两行 —— 这也是 sub_id 不用自增的理由。"""
    first = feed_sub(db)
    second = feed_sub(db, title="改了标题")
    assert first == second
    assert len(list_subscriptions(1, database_url=db)) == 1
    assert get_subscription(first, user_id=1, database_url=db).title == "改了标题"


def test_legado_and_feed_subscriptions_coexist(db):
    feed_sub(db)
    upsert_subscription(
        {"kind": "legado", "url_id": 42, "title": "归档源"}, user_id=1, database_url=db
    )
    kinds = sorted(row.kind for row in list_subscriptions(1, database_url=db))
    assert kinds == ["feed", "legado"]


def test_subscriptions_do_not_leak_between_users(db):
    feed_sub(db, user_id=1)
    feed_sub(db, user_id=2, url="https://other.example.com/feed.xml", title="别人的")

    assert [r.title for r in list_subscriptions(1, database_url=db)] == ["示例博客"]
    assert [r.title for r in list_subscriptions(2, database_url=db)] == ["别人的"]


def test_two_users_can_subscribe_to_the_same_source(db):
    """sub_id 一样，所以主键必须带 user_id。"""
    one = feed_sub(db, user_id=1)
    two = feed_sub(db, user_id=2)
    assert one == two
    assert get_subscription(one, user_id=1, database_url=db) is not None
    assert get_subscription(one, user_id=2, database_url=db) is not None


def test_one_user_cannot_see_anothers_subscription_by_id(db):
    sub_id = feed_sub(db, user_id=1)
    assert get_subscription(sub_id, user_id=2, database_url=db) is None


def test_payload_cannot_smuggle_a_user_id(db):
    sub_id = upsert_subscription(
        {"kind": "feed", "feed_url": "https://x/f", "user_id": 2}, user_id=1, database_url=db
    )
    assert get_subscription(sub_id, user_id=1, database_url=db) is not None
    assert get_subscription(sub_id, user_id=2, database_url=db) is None


def test_unsubscribing_only_touches_the_caller(db):
    sub_id = feed_sub(db, user_id=1)
    feed_sub(db, user_id=2)

    assert remove_subscription(sub_id, user_id=1, database_url=db) is True

    assert get_subscription(sub_id, user_id=1, database_url=db) is None
    assert get_subscription(sub_id, user_id=2, database_url=db) is not None


def test_unsubscribing_something_you_do_not_have_returns_false(db):
    sub_id = feed_sub(db, user_id=1)
    assert remove_subscription(sub_id, user_id=2, database_url=db) is False


def test_fetch_result_is_recorded(db):
    sub_id = feed_sub(db)
    record_subscription_fetch(sub_id, user_id=1, error="超时", database_url=db)
    row = get_subscription(sub_id, user_id=1, database_url=db)
    assert row.last_error == "超时"
    assert row.last_fetched_at is not None

    #  成功后要把上次的错误清掉，否则界面会一直显示一个过期的失败原因
    record_subscription_fetch(sub_id, user_id=1, database_url=db)
    assert get_subscription(sub_id, user_id=1, database_url=db).last_error is None


# ---------------------------------------------------------------- 文章状态


def test_mark_read_and_read_back(db):
    sub_id = feed_sub(db)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(sub_id, key, user_id=1, read=True, database_url=db)

    states = get_article_states(sub_id, [key], user_id=1, database_url=db)
    assert states[key].read is True
    assert states[key].favorited is False


def test_none_leaves_the_other_flag_alone(db):
    sub_id = feed_sub(db)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(sub_id, key, user_id=1, read=True, database_url=db)
    set_article_state(sub_id, key, user_id=1, favorited=True, database_url=db)

    state = get_article_states(sub_id, [key], user_id=1, database_url=db)[key]
    assert (state.read, state.favorited) == (True, True)


def test_meta_is_written_once_and_not_overwritten(db):
    """收藏列表要能脱离原始列表单独渲染，所以标题和链接要留一份。"""
    sub_id = feed_sub(db)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(
        sub_id,
        key,
        user_id=1,
        favorited=True,
        meta={"title": "第一篇", "link": "https://blog.example.com/1"},
        database_url=db,
    )
    set_article_state(sub_id, key, user_id=1, read=True, meta={"title": "改了"}, database_url=db)

    state = get_article_states(sub_id, [key], user_id=1, database_url=db)[key]
    assert state.title == "第一篇"


def test_article_states_are_per_user(db):
    sub_id = feed_sub(db, user_id=1)
    feed_sub(db, user_id=2)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(sub_id, key, user_id=1, read=True, database_url=db)

    assert get_article_states(sub_id, [key], user_id=2, database_url=db) == {}


def test_batch_state_lookup_handles_an_empty_list(db):
    assert get_article_states(feed_sub(db), [], user_id=1, database_url=db) == {}


def test_count_read(db):
    sub_id = feed_sub(db)
    for index in range(3):
        key = compute_article_key(f"https://blog.example.com/{index}")
        set_article_state(sub_id, key, user_id=1, read=index < 2, database_url=db)

    assert count_read(sub_id, user_id=1, database_url=db) == 2


def test_mark_all_read_creates_missing_rows_and_counts_changes(db):
    sub_id = feed_sub(db)
    keys = [compute_article_key(f"https://blog.example.com/{i}") for i in range(3)]
    set_article_state(sub_id, keys[0], user_id=1, read=True, database_url=db)

    #  第一条已经读过了，所以只应改动两条
    assert mark_all_read(sub_id, keys, user_id=1, database_url=db) == 2
    assert count_read(sub_id, user_id=1, database_url=db) == 3
    #  再来一次没有任何变化
    assert mark_all_read(sub_id, keys, user_id=1, database_url=db) == 0


def test_mark_all_read_with_no_keys_is_a_no_op(db):
    assert mark_all_read(feed_sub(db), [], user_id=1, database_url=db) == 0


def test_favorites_list(db):
    sub_id = feed_sub(db)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(
        sub_id, key, user_id=1, favorited=True, meta={"title": "收藏的"}, database_url=db
    )
    other = compute_article_key("https://blog.example.com/2")
    set_article_state(sub_id, other, user_id=1, read=True, database_url=db)

    favorites = list_favorites(user_id=1, database_url=db)
    assert [row.title for row in favorites] == ["收藏的"]


def test_favorites_are_per_user(db):
    sub_id = feed_sub(db, user_id=1)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(sub_id, key, user_id=1, favorited=True, database_url=db)

    assert list_favorites(user_id=2, database_url=db) == []


def test_unsubscribing_clears_the_article_states(db):
    """留着就是永远不会被读到的垃圾 —— 而且退订再订回来不该看到旧的已读状态。"""
    sub_id = feed_sub(db)
    key = compute_article_key("https://blog.example.com/1")
    set_article_state(sub_id, key, user_id=1, read=True, database_url=db)

    remove_subscription(sub_id, user_id=1, database_url=db)
    feed_sub(db)

    assert get_article_states(sub_id, [key], user_id=1, database_url=db) == {}


def test_tables_are_user_scoped_from_the_start(db):
    """这两张表是带 user_id 建出来的，不需要迁移。"""
    assert "user_id" in storage.ReaderRssSubscription.__table__.columns
    assert "user_id" in storage.ReaderRssArticleState.__table__.columns
    assert storage.ReaderRssSubscription.__table__.primary_key.columns.keys() == [
        "user_id",
        "sub_id",
    ]


def test_the_local_identity_can_subscribe_too(db):
    """还没有账号时也要能用。"""
    sub_id = feed_sub(db, user_id=LOCAL_USER_ID)
    assert get_subscription(sub_id, user_id=LOCAL_USER_ID, database_url=db) is not None
