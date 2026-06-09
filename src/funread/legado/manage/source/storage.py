"""Source list and source detail persistence."""

from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from nltlog import getLogger
from nltsecret import read_secret
from sqlalchemy import DateTime, Integer, String, create_engine, delete, desc, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


logger = getLogger("funread")


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base."""


def utcnow() -> datetime:
    """Return a naive UTC datetime for database timestamps."""
    return datetime.utcnow()


class SourceListRecord(Base):
    """Persisted metadata for a source-list URL."""

    __tablename__ = "source_list_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(1024), unique=True, index=True, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    last_queried_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class SourceDetailRecord(Base):
    """Persisted source-detail URL mapping metadata."""

    __tablename__ = "source_detail_records"

    source_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    url: Mapped[str] = mapped_column(String(1024), index=True, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class SourceIndexRecord(Base):
    """Persisted source-content index metadata keyed by md5."""

    __tablename__ = "source_index_records"

    md5: Mapped[str] = mapped_column(String(64), primary_key=True)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    url_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    cate1: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


_ENGINE_CACHE: Dict[str, Any] = {}
_SESSION_FACTORY_CACHE: Dict[str, sessionmaker] = {}
_INITIALIZED_DATABASES = set()
SOURCE_DETAIL_ID_START = 10_000_000


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


def init_source_db(database_url: Optional[str] = None) -> None:
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


def _build_source_list_query(
    source_type: Optional[str] = None,
    min_source_count: Optional[int] = None,
    max_source_count: Optional[int] = None,
    queried_before: Optional[datetime] = None,
):
    stmt = select(SourceListRecord)
    if source_type:
        stmt = stmt.where(SourceListRecord.source_type == source_type)
    if min_source_count is not None:
        stmt = stmt.where(SourceListRecord.source_count >= min_source_count)
    if max_source_count is not None:
        stmt = stmt.where(SourceListRecord.source_count <= max_source_count)
    if queried_before is not None:
        stmt = stmt.where(SourceListRecord.last_queried_at <= queried_before)
    return stmt.order_by(desc(SourceListRecord.last_queried_at), desc(SourceListRecord.id))


def iter_source_list_data(
    source_type: Optional[str] = None,
    min_source_count: Optional[int] = None,
    max_source_count: Optional[int] = None,
    stale_seconds: int = 86400,
    limit: Optional[int] = None,
    timeout: int = 30,
    database_url: Optional[str] = None,
) -> Iterator[Tuple[SourceListRecord, Any]]:
    """Yield updated source-list records and payloads for stale URLs ordered by last query time."""
    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)
    queried_before = utcnow() if stale_seconds <= 0 else utcnow() - timedelta(seconds=stale_seconds)

    with session_factory() as session:
        stmt = _build_source_list_query(
            source_type=source_type,
            min_source_count=min_source_count,
            max_source_count=max_source_count,
            queried_before=queried_before,
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        records: List[SourceListRecord] = session.execute(stmt).scalars().all()

    for record in records:
        queried_at = utcnow()
        try:
            response = requests.get(record.url, timeout=timeout)
            response.raise_for_status()
            source_data = response.json()
            source_count = _count_source_items(source_data)
            updated_record = upsert_source_list_record(
                url=record.url,
                source_type=record.source_type,
                source_count=source_count,
                queried_at=queried_at,
                database_url=database_url,
            )
            yield updated_record, source_data
        except Exception as e:
            logger.warning(f"Failed to fetch source list from {record.url}: {e}")
            upsert_source_list_record(
                url=record.url,
                source_type=record.source_type,
                source_count=-1,
                queried_at=queried_at,
                database_url=database_url,
            )


def list_source_detail_records(
    source_type: Optional[str] = None,
    database_url: Optional[str] = None,
) -> List[SourceDetailRecord]:
    """List source-detail records ordered by id."""
    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceDetailRecord)
        if source_type:
            stmt = stmt.where(SourceDetailRecord.source_type == source_type)
        return session.execute(stmt.order_by(SourceDetailRecord.id)).scalars().all()


def load_source_index_map(
    source_type: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, Dict[str, Any]]:
    """Load md5 index metadata from source-index records."""
    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceIndexRecord)
        if source_type:
            stmt = stmt.where(SourceIndexRecord.source_type == source_type)
        records = session.execute(stmt).scalars().all()

    return {
        record.md5: {
            "md5": record.md5,
            "source_type": record.source_type,
            "url_id": record.url_id,
            "hostname": record.hostname,
            "cate1": record.cate1,
        }
        for record in records
    }


def upsert_source_index_records(
    records: List[Dict[str, Any]],
    source_type: str,
    database_url: Optional[str] = None,
) -> None:
    """Bulk upsert source-content index metadata."""
    if not source_type:
        raise ValueError("source_type is required")
    if not records:
        return

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)
    md5_list = [str(record["md5"]) for record in records if record.get("md5")]
    if not md5_list:
        return

    with session_factory() as session:
        existing_records = {
            record.md5: record
            for record in session.execute(
                select(SourceIndexRecord).where(SourceIndexRecord.md5.in_(md5_list))
            )
            .scalars()
            .all()
        }

        for payload in records:
            md5 = str(payload.get("md5") or "")
            hostname = str(payload.get("hostname") or "")
            url_id = payload.get("url_id")
            cate1 = payload.get("cate1")
            if not md5 or not hostname or url_id is None or cate1 is None:
                continue

            record = existing_records.get(md5)
            if record is None:
                session.add(
                    SourceIndexRecord(
                        md5=md5,
                        source_type=source_type,
                        url_id=int(url_id),
                        hostname=hostname,
                        cate1=int(cate1),
                    )
                )
                continue

            record.source_type = source_type
            record.url_id = int(url_id)
            record.hostname = hostname
            record.cate1 = int(cate1)

        session.commit()


def replace_source_index_records(
    records: List[Dict[str, Any]],
    source_type: str,
    database_url: Optional[str] = None,
) -> None:
    """Replace all source-index rows for a source type with the provided records."""
    if not source_type:
        raise ValueError("source_type is required")

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        session.execute(
            delete(SourceIndexRecord).where(SourceIndexRecord.source_type == source_type)
        )
        for payload in records:
            md5 = str(payload.get("md5") or "")
            hostname = str(payload.get("hostname") or "")
            url_id = payload.get("url_id")
            cate1 = payload.get("cate1")
            if not md5 or not hostname or url_id is None or cate1 is None:
                continue
            session.add(
                SourceIndexRecord(
                    md5=md5,
                    source_type=source_type,
                    url_id=int(url_id),
                    hostname=hostname,
                    cate1=int(cate1),
                )
            )
        session.commit()


def replace_source_detail_records(
    records: List[Dict[str, Any]],
    source_type: str,
    database_url: Optional[str] = None,
) -> None:
    """Replace all source-detail rows for a source type with the provided records."""
    if not source_type:
        raise ValueError("source_type is required")

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        session.execute(
            delete(SourceDetailRecord).where(SourceDetailRecord.source_type == source_type)
        )
        for payload in records:
            record_id = payload.get("id")
            url = str(payload.get("url") or "")
            version = payload.get("version", 0)
            if record_id is None or not url:
                continue
            session.add(
                SourceDetailRecord(
                    id=int(record_id),
                    url=url,
                    source_type=source_type,
                    version=int(version),
                )
            )
        session.commit()


def load_source_detail_url_map(
    source_type: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[str, int]:
    """Load URL to id mapping from source-detail records."""
    return {
        record.url: record.id
        for record in list_source_detail_records(source_type=source_type, database_url=database_url)
    }


def _next_source_detail_id(session: Session, source_type: str) -> int:
    current_max = session.execute(
        select(func.max(SourceDetailRecord.id)).where(SourceDetailRecord.source_type == source_type)
    ).scalar_one_or_none()
    if current_max is None:
        return SOURCE_DETAIL_ID_START
    return max(int(current_max) + 1, SOURCE_DETAIL_ID_START)


def add_source_detail_url(
    url: str,
    source_type: str,
    source_id: Optional[int] = None,
    version: int = 0,
    database_url: Optional[str] = None,
) -> SourceDetailRecord:
    """Add or update a source-detail URL record."""
    return upsert_source_detail_record(
        url=url,
        source_type=source_type,
        source_id=source_id,
        version=version,
        database_url=database_url,
    )


def upsert_source_detail_record(
    url: str,
    source_type: str,
    source_id: Optional[int] = None,
    version: int = 0,
    database_url: Optional[str] = None,
) -> SourceDetailRecord:
    if not url:
        raise ValueError("url is required")
    if not source_type:
        raise ValueError("source_type is required")

    normalized_source_id = int(source_id) if source_id is not None else None

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceDetailRecord).where(
            SourceDetailRecord.source_type == source_type,
            SourceDetailRecord.url == url,
        )
        record = session.execute(stmt).scalar_one_or_none()

        if record is None:
            record_id = (
                normalized_source_id
                if normalized_source_id is not None
                else _next_source_detail_id(session, source_type=source_type)
            )
            record = SourceDetailRecord(id=record_id, url=url, source_type=source_type)
            session.add(record)
        else:
            if normalized_source_id is not None and record.id != normalized_source_id:
                record.id = normalized_source_id
            record.url = url
            record.source_type = source_type
        record.version = int(version)

        session.commit()
        session.refresh(record)
        return record


def add_source_list_url(
    url: str,
    source_type: str,
    source_count: int = -1,
    queried_at: Optional[datetime] = None,
    database_url: Optional[str] = None,
) -> SourceListRecord:
    """Add or update a source-list URL record with a default unknown count."""
    return upsert_source_list_record(
        url=url,
        source_type=source_type,
        source_count=source_count,
        queried_at=queried_at,
        database_url=database_url,
    )


def upsert_source_list_record(
    url: str,
    source_type: str,
    source_count: int,
    queried_at: Optional[datetime] = None,
    database_url: Optional[str] = None,
) -> SourceListRecord:
    if not url:
        raise ValueError("url is required")
    if not source_type:
        raise ValueError("source_type is required")

    queried_at = queried_at or utcnow()
    normalized_count = int(source_count)

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        record = session.execute(
            select(SourceListRecord).where(SourceListRecord.url == url)
        ).scalar_one_or_none()

        if record is None:
            record = SourceListRecord(
                url=url,
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
