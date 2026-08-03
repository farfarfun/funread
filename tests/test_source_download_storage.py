from datetime import UTC, datetime

import requests

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import Session

from funread.legado.manage.source.storage import (
    SOURCE_STATUS_AVAILABLE,
    SOURCE_STATUS_BLACKLISTED,
    Base,
    SOURCE_STATUS_PENDING,
    SourceListRecord,
    SourceDetailRecord,
    add_source_list_url,
    add_source_detail_url,
    load_source_detail_url_map,
    list_source_detail_records,
    iter_source_list_data,
    init_source_db,
    upsert_source_list_record,
)


def test_upsert_source_list_record(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source.db'}"

    record = upsert_source_list_record(
        url="https://example.com/source.json",
        source_type="rss",
        source_count=12,
        queried_at=datetime(2024, 1, 1, 0, 0, 0),
        database_url=db_url,
    )

    assert record.url == "https://example.com/source.json"

    updated = upsert_source_list_record(
        url="https://example.com/source.json",
        source_type="book",
        source_count=34,
        queried_at=datetime(2024, 1, 2, 0, 0, 0),
        database_url=db_url,
    )

    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        rows = session.execute(select(SourceListRecord)).scalars().all()

    assert len(rows) == 1
    assert updated.source_type == "book"
    assert rows[0].source_count == 34
    assert rows[0].last_queried_at == datetime(2024, 1, 2, 0, 0, 0)


def test_add_source_list_url_defaults_count_to_negative_one(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_default.db'}"

    record = add_source_list_url(
        url="https://example.com/default.json",
        source_type="rss",
        database_url=db_url,
    )

    assert record.source_count == -1


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_iter_source_list_data_orders_by_last_queried_at_desc(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'source_iter.db'}"
    add_source_list_url(
        url="https://example.com/older.json",
        source_type="rss",
        source_count=-1,
        queried_at=datetime(2024, 1, 1, 0, 0, 0),
        database_url=db_url,
    )
    add_source_list_url(
        url="https://example.com/newer.json",
        source_type="rss",
        source_count=-1,
        queried_at=datetime(2024, 1, 2, 0, 0, 0),
        database_url=db_url,
    )

    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        if url.endswith("newer.json"):
            return _FakeResponse([{"id": 1}, {"id": 2}])
        return _FakeResponse({"list": [{"id": 3}]})

    monkeypatch.setattr(requests, "get", fake_get)

    items = list(iter_source_list_data(source_type="rss", database_url=db_url))

    assert calls == [
        "https://example.com/newer.json",
        "https://example.com/older.json",
    ]
    assert [item[1] for item in items] == [[{"id": 1}, {"id": 2}], {"list": [{"id": 3}]}]
    assert [item[0].url for item in items] == [
        "https://example.com/newer.json",
        "https://example.com/older.json",
    ]
    assert [item[0].source_count for item in items] == [2, 1]

    engine = create_engine(db_url, future=True)
    with Session(engine) as session:
        rows = (
            session.execute(select(SourceListRecord).order_by(SourceListRecord.url)).scalars().all()
        )

    assert rows[0].source_count == 2
    assert rows[1].source_count == 1


def test_upsert_source_detail_record(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_record.db'}"

    record = add_source_detail_url(
        url="https://example.com/source-a",
        source_type="book",
        source_id=1001,
        database_url=db_url,
    )

    assert record.url == "https://example.com/source-a"
    assert record.id == 1001

    updated = add_source_detail_url(
        url="https://example.com/source-a-updated",
        source_type="rss",
        source_id=1001,
        database_url=db_url,
    )

    engine = create_engine(db_url, future=True)
    with Session(engine) as session:
        rows = session.execute(select(SourceDetailRecord)).scalars().all()

    assert len(rows) == 1
    assert updated.id == record.id
    assert rows[0].url == "https://example.com/source-a-updated"
    assert rows[0].id == 1001
    assert rows[0].source_type == "rss"
    assert rows[0].status == SOURCE_STATUS_PENDING


def test_add_source_detail_url_assigns_ids_from_10000000(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_start.db'}"

    first = add_source_detail_url(
        url="https://example.com/source-first",
        source_type="book",
        database_url=db_url,
    )
    second = add_source_detail_url(
        url="https://example.com/source-second",
        source_type="rss",
        database_url=db_url,
    )

    assert first.id == 10000000
    assert second.id == 10000001
    assert load_source_detail_url_map(database_url=db_url) == {
        "https://example.com/source-first": 10000000,
        "https://example.com/source-second": 10000001,
    }
    assert [record.id for record in list_source_detail_records(database_url=db_url)] == [
        10000000,
        10000001,
    ]


def test_list_source_detail_records_filters_by_source_type(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_type.db'}"

    add_source_detail_url(url="https://example.com/book", source_type="book", database_url=db_url)
    add_source_detail_url(url="https://example.com/rss", source_type="rss", database_url=db_url)

    assert [
        record.url for record in list_source_detail_records(source_type="book", database_url=db_url)
    ] == ["https://example.com/book"]
    assert load_source_detail_url_map(source_type="rss", database_url=db_url) == {
        "https://example.com/rss": 10000001
    }


def test_list_source_detail_records_filters_by_status(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_status.db'}"

    add_source_detail_url(
        url="https://example.com/pending",
        source_type="book",
        status=SOURCE_STATUS_PENDING,
        database_url=db_url,
    )
    add_source_detail_url(
        url="https://example.com/available",
        source_type="book",
        status=SOURCE_STATUS_AVAILABLE,
        database_url=db_url,
    )
    add_source_detail_url(
        url="https://example.com/blacklisted",
        source_type="book",
        status=SOURCE_STATUS_BLACKLISTED,
        database_url=db_url,
    )

    records = list_source_detail_records(
        source_type="book",
        statuses=[SOURCE_STATUS_PENDING, SOURCE_STATUS_AVAILABLE],
        database_url=db_url,
    )

    assert [record.url for record in records] == [
        "https://example.com/pending",
        "https://example.com/available",
    ]


def test_iter_source_list_data_skips_recent_records_by_default(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'source_iter_stale.db'}"
    add_source_list_url(
        url="https://example.com/recent.json",
        source_type="rss",
        source_count=-1,
        queried_at=datetime.now(UTC).replace(tzinfo=None),
        database_url=db_url,
    )

    monkeypatch.setattr(
        requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not fetch recent record")
        ),
    )

    items = list(iter_source_list_data(source_type="rss", database_url=db_url))

    assert items == []


def test_init_source_db_migrates_old_source_detail_table(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_migrate.db'}"
    engine = create_engine(db_url, future=True)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE source_detail_records (
                    source_type VARCHAR(32) NOT NULL,
                    id INTEGER NOT NULL,
                    url VARCHAR(1024) NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    PRIMARY KEY (source_type, id)
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO source_detail_records
                    (source_type, id, url, version, created_at, updated_at)
                VALUES
                    ('book', 10000001, 'books.example.com', 2, '2024-01-01 00:00:00', '2024-01-01 00:00:00')
                """
            )
        )

    init_source_db(database_url=db_url)

    inspector = inspect(engine)
    pk = tuple(inspector.get_pk_constraint("source_detail_records")["constrained_columns"])
    uniques = {
        tuple(constraint.get("column_names") or [])
        for constraint in inspector.get_unique_constraints("source_detail_records")
    }

    with Session(engine) as session:
        row = session.execute(select(SourceDetailRecord)).scalar_one()

    assert pk == ("source_type", "url_md5")
    assert ("id",) in uniques
    assert row.id == 10000001
    assert row.url == "books.example.com"
    assert row.version == 2
    assert row.status == SOURCE_STATUS_PENDING
