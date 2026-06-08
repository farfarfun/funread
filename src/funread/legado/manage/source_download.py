"""Source download record persistence."""

from datetime import datetime
from typing import Any, Dict, Optional

from nltsecret import read_secret
from sqlalchemy import JSON, DateTime, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker


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
    source_data: Mapped[Any] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    last_queried_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


_ENGINE_CACHE: Dict[str, Any] = {}
_SESSION_FACTORY_CACHE: Dict[str, sessionmaker] = {}
_INITIALIZED_DATABASES = set()


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


def _normalize_source_data(source_data: Any) -> Any:
    if source_data is None:
        return {}
    if isinstance(source_data, (dict, list, str, int, float, bool)):
        return source_data
    return {"value": repr(source_data)}


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


def upsert_source_download_record(
    download_url: str,
    source_type: str,
    source_data: Any,
    queried_at: Optional[datetime] = None,
    database_url: Optional[str] = None,
) -> SourceDownloadRecord:
    if not download_url:
        raise ValueError("download_url is required")
    if not source_type:
        raise ValueError("source_type is required")

    queried_at = queried_at or utcnow()
    normalized_data = _normalize_source_data(source_data)

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
                source_data=normalized_data,
                last_queried_at=queried_at,
            )
            session.add(record)
        else:
            record.source_type = source_type
            record.source_data = normalized_data
            record.last_queried_at = queried_at

        session.commit()
        session.refresh(record)
        return record
