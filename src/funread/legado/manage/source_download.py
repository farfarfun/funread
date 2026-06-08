"""Source download record persistence."""

from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from nltlog import getLogger
from nltsecret import read_secret
from sqlalchemy import DateTime, Integer, String, create_engine, desc, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


logger = getLogger("funread")


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base."""


def utcnow() -> datetime:
    """Return a naive UTC datetime for database timestamps."""
    return datetime.utcnow()


class SourceDownloadRecord(Base):
    """Persisted metadata for a downloaded source URL."""

    __tablename__ = "source_download_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    download_url: Mapped[str] = mapped_column(String(1024), unique=True, index=True, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    last_queried_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class SourceRecord(Base):
    """Persisted source URL mapping metadata."""

    __tablename__ = "source_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    url: Mapped[str] = mapped_column(String(1024), unique=True, index=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


_ENGINE_CACHE: Dict[str, Any] = {}
_SESSION_FACTORY_CACHE: Dict[str, sessionmaker] = {}
_INITIALIZED_DATABASES = set()
SOURCE_RECORD_ID_START = 10_000_000


def _get_database_url(database_url: Optional[str] = None) -> Optional[str]:
    if database_url:
        return database_url

    try:
        secret_url = read_secret(
            cate1="funread",
            cate2="cache",
            cate3="source",
            cate4="db_url",
        )
        if secret_url:
            return secret_url
    except Exception:
        pass

    return None


def _get_engine(database_url: Optional[str] = None):
    resolved_url = _get_database_url(database_url)
    if not resolved_url:
        raise ValueError(
            "Database URL is not configured in read_secret(funread/cache/source/db_url)."
        )
    engine = _ENGINE_CACHE.get(resolved_url)
    if engine is None:
        engine = create_engine(resolved_url, future=True)
        _ENGINE_CACHE[resolved_url] = engine
    return engine


def _get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    resolved_url = _get_database_url(database_url)
    if not resolved_url:
        raise ValueError(
            "Database URL is not configured in read_secret(funread/cache/source/db_url)."
        )
    factory = _SESSION_FACTORY_CACHE.get(resolved_url)
    if factory is None:
        factory = sessionmaker(bind=_get_engine(resolved_url), expire_on_commit=False, future=True)
        _SESSION_FACTORY_CACHE[resolved_url] = factory
    return factory


def init_source_download_db(database_url: Optional[str] = None) -> None:
    resolved_url = _get_database_url(database_url)
    if not resolved_url or resolved_url in _INITIALIZED_DATABASES:
        return
    Base.metadata.create_all(_get_engine(resolved_url))
    _INITIALIZED_DATABASES.add(resolved_url)


def _count_source_items(source_data: Any) -> int:
    if isinstance(source_data, list):
        return len(source_data)
    if isinstance(source_data, dict):
        if isinstance(source_data.get("list"), list):
            return len(source_data["list"])
        if isinstance(source_data.get("data"), list):
            return len(source_data["data"])
        return 1
    return -1


def _build_source_download_query(
    source_type: Optional[str] = None,
    min_source_count: Optional[int] = None,
    max_source_count: Optional[int] = None,
):
    stmt = select(SourceDownloadRecord)
    if source_type:
        stmt = stmt.where(SourceDownloadRecord.source_type == source_type)
    if min_source_count is not None:
        stmt = stmt.where(SourceDownloadRecord.source_count >= min_source_count)
    if max_source_count is not None:
        stmt = stmt.where(SourceDownloadRecord.source_count <= max_source_count)
    return stmt.order_by(desc(SourceDownloadRecord.last_queried_at), desc(SourceDownloadRecord.id))


def iter_source_download_data(
    source_type: Optional[str] = None,
    min_source_count: Optional[int] = None,
    max_source_count: Optional[int] = None,
    limit: Optional[int] = None,
    timeout: int = 30,
    database_url: Optional[str] = None,
) -> Iterator[Tuple[SourceDownloadRecord, Any]]:
    """Yield updated record info and source payloads ordered by last query time descending."""
    init_source_download_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = _build_source_download_query(
            source_type=source_type,
            min_source_count=min_source_count,
            max_source_count=max_source_count,
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        records: List[SourceDownloadRecord] = session.execute(stmt).scalars().all()

    for record in records:
        queried_at = utcnow()
        try:
            response = requests.get(record.download_url, timeout=timeout)
            response.raise_for_status()
            source_data = response.json()
            source_count = _count_source_items(source_data)
            updated_record = upsert_source_download_record(
                download_url=record.download_url,
                source_type=record.source_type,
                source_count=source_count,
                queried_at=queried_at,
                database_url=database_url,
            )
            yield updated_record, source_data
        except Exception as e:
            logger.warning(f"Failed to fetch source data from {record.download_url}: {e}")
            upsert_source_download_record(
                download_url=record.download_url,
                source_type=record.source_type,
                source_count=-1,
                queried_at=queried_at,
                database_url=database_url,
            )


def list_source_records(database_url: Optional[str] = None) -> List[SourceRecord]:
    """List source records ordered by id."""
    init_source_download_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        return session.execute(select(SourceRecord).order_by(SourceRecord.id)).scalars().all()


def load_source_url_map(database_url: Optional[str] = None) -> Dict[str, int]:
    """Load URL to id mapping from the source_records table."""
    return {record.url: record.id for record in list_source_records(database_url=database_url)}


def _next_source_record_id(session: Session) -> int:
    current_max = session.execute(select(func.max(SourceRecord.id))).scalar_one_or_none()
    if current_max is None:
        return SOURCE_RECORD_ID_START
    return max(int(current_max) + 1, SOURCE_RECORD_ID_START)


def add_source_url(
    url: str,
    source_id: Optional[int] = None,
    database_url: Optional[str] = None,
) -> SourceRecord:
    """Add or update a source URL record."""
    return upsert_source_record(url=url, source_id=source_id, database_url=database_url)


def upsert_source_record(
    url: str,
    source_id: Optional[int] = None,
    database_url: Optional[str] = None,
) -> SourceRecord:
    if not url:
        raise ValueError("url is required")

    normalized_source_id = int(source_id) if source_id is not None else None

    init_source_download_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceRecord).where(SourceRecord.url == url)
        record = session.execute(stmt).scalar_one_or_none()

        if record is None:
            record_id = (
                normalized_source_id
                if normalized_source_id is not None
                else _next_source_record_id(session)
            )
            record = SourceRecord(id=record_id, url=url)
            session.add(record)
        else:
            if normalized_source_id is not None and record.id != normalized_source_id:
                record.id = normalized_source_id
            record.url = url

        session.commit()
        session.refresh(record)
        return record


def add_source_download_url(
    download_url: str,
    source_type: str,
    source_count: int = -1,
    queried_at: Optional[datetime] = None,
    database_url: Optional[str] = None,
) -> SourceDownloadRecord:
    """Add or update a source download URL record with a default unknown count."""
    return upsert_source_download_record(
        download_url=download_url,
        source_type=source_type,
        source_count=source_count,
        queried_at=queried_at,
        database_url=database_url,
    )


def upsert_source_download_record(
    download_url: str,
    source_type: str,
    source_count: int,
    queried_at: Optional[datetime] = None,
    database_url: Optional[str] = None,
) -> SourceDownloadRecord:
    if not download_url:
        raise ValueError("download_url is required")
    if not source_type:
        raise ValueError("source_type is required")

    queried_at = queried_at or utcnow()
    normalized_count = int(source_count)

    init_source_download_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        record = session.execute(
            select(SourceDownloadRecord).where(SourceDownloadRecord.download_url == download_url)
        ).scalar_one_or_none()

        if record is None:
            record = SourceDownloadRecord(
                download_url=download_url,
                source_type=source_type,
                source_count=normalized_count,
                last_queried_at=queried_at,
            )
            session.add(record)
        else:
            record.source_type = source_type
            record.source_count = normalized_count
            record.last_queried_at = queried_at

        session.commit()
        session.refresh(record)
        return record
