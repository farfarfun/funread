from datetime import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from funread.legado.manage.source_download import (
    Base,
    SourceDownloadRecord,
    upsert_source_download_record,
)


def test_upsert_source_download_record(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'source.db'}"

    record = upsert_source_download_record(
        download_url="https://example.com/source.json",
        source_type="rss",
        source_data={"name": "first"},
        queried_at=datetime(2024, 1, 1, 0, 0, 0),
        database_url=db_url,
    )

    assert record.download_url == "https://example.com/source.json"

    updated = upsert_source_download_record(
        download_url="https://example.com/source.json",
        source_type="book",
        source_data={"name": "second"},
        queried_at=datetime(2024, 1, 2, 0, 0, 0),
        database_url=db_url,
    )

    engine = create_engine(db_url, future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        rows = session.execute(select(SourceDownloadRecord)).scalars().all()

    assert len(rows) == 1
    assert updated.source_type == "book"
    assert rows[0].source_data == {"name": "second"}
    assert rows[0].last_queried_at == datetime(2024, 1, 2, 0, 0, 0)
