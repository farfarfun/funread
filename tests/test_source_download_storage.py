from datetime import datetime

import requests

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from funread.legado.manage.source_download import (
    Base,
    SourceDownloadRecord,
    SourceRecord,
    add_source_download_url,
    add_source_url,
    load_source_url_map,
    list_source_records,
    iter_source_download_data,
    upsert_source_download_record,
)


def test_upsert_source_download_record(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source.db'}"

    record = upsert_source_download_record(
        download_url="https://example.com/source.json",
        source_type="rss",
        source_count=12,
        queried_at=datetime(2024, 1, 1, 0, 0, 0),
        database_url=db_url,
    )

    assert record.download_url == "https://example.com/source.json"

    updated = upsert_source_download_record(
        download_url="https://example.com/source.json",
        source_type="book",
        source_count=34,
        queried_at=datetime(2024, 1, 2, 0, 0, 0),
        database_url=db_url,
    )

    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        rows = session.execute(select(SourceDownloadRecord)).scalars().all()

    assert len(rows) == 1
    assert updated.source_type == "book"
    assert rows[0].source_count == 34
    assert rows[0].last_queried_at == datetime(2024, 1, 2, 0, 0, 0)


def test_add_source_download_url_defaults_count_to_negative_one(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_default.db'}"

    record = add_source_download_url(
        download_url="https://example.com/default.json",
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


def test_iter_source_download_data_orders_by_last_queried_at_desc(tmp_path, monkeypatch):
    db_url = f"sqlite:///{tmp_path / 'source_iter.db'}"
    add_source_download_url(
        download_url="https://example.com/older.json",
        source_type="rss",
        source_count=-1,
        queried_at=datetime(2024, 1, 1, 0, 0, 0),
        database_url=db_url,
    )
    add_source_download_url(
        download_url="https://example.com/newer.json",
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

    items = list(iter_source_download_data(source_type="rss", database_url=db_url))

    assert calls == [
        "https://example.com/newer.json",
        "https://example.com/older.json",
    ]
    assert [item[1] for item in items] == [[{"id": 1}, {"id": 2}], {"list": [{"id": 3}]}]
    assert [item[0].download_url for item in items] == [
        "https://example.com/newer.json",
        "https://example.com/older.json",
    ]
    assert [item[0].source_count for item in items] == [2, 1]

    engine = create_engine(db_url, future=True)
    with Session(engine) as session:
        rows = (
            session.execute(
                select(SourceDownloadRecord).order_by(SourceDownloadRecord.download_url)
            )
            .scalars()
            .all()
        )

    assert rows[0].source_count == 2
    assert rows[1].source_count == 1


def test_upsert_source_record(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_record.db'}"

    record = add_source_url(
        url="https://example.com/source-a",
        source_id=1001,
        database_url=db_url,
    )

    assert record.url == "https://example.com/source-a"
    assert record.id == 1001

    updated = add_source_url(
        url="https://example.com/source-a-updated",
        source_id=1001,
        database_url=db_url,
    )

    engine = create_engine(db_url, future=True)
    with Session(engine) as session:
        rows = session.execute(select(SourceRecord)).scalars().all()

    assert len(rows) == 1
    assert updated.id == record.id
    assert rows[0].url == "https://example.com/source-a-updated"
    assert rows[0].id == 1001


def test_add_source_url_assigns_ids_from_10000000(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source_start.db'}"

    first = add_source_url(
        url="https://example.com/source-first",
        database_url=db_url,
    )
    second = add_source_url(
        url="https://example.com/source-second",
        database_url=db_url,
    )

    assert first.id == 10000000
    assert second.id == 10000001
    assert load_source_url_map(database_url=db_url) == {
        "https://example.com/source-first": 10000000,
        "https://example.com/source-second": 10000001,
    }
    assert [record.id for record in list_source_records(database_url=db_url)] == [
        10000000,
        10000001,
    ]
