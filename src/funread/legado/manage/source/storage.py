"""Source list and source detail persistence."""

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from farlog import getLogger
from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    delete,
    desc,
    event,
    func,
    inspect,
    select,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from funread.base.config import is_sqlite_url, resolve_database_url

logger = getLogger("funread")

SOURCE_STATUS_PENDING = 1
SOURCE_STATUS_AVAILABLE = 2
SOURCE_STATUS_UNAVAILABLE = 3
SOURCE_STATUS_BLACKLISTED = 4
VALID_SOURCE_STATUSES = {
    SOURCE_STATUS_PENDING,
    SOURCE_STATUS_AVAILABLE,
    SOURCE_STATUS_UNAVAILABLE,
    SOURCE_STATUS_BLACKLISTED,
}


class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base."""


def utcnow() -> datetime:
    """Return a naive UTC datetime for database timestamps."""
    return datetime.now(UTC).replace(tzinfo=None)


class SourceListRecord(Base):
    """Persisted metadata for a source-list URL."""

    __tablename__ = "source_list_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String(1024), unique=True, index=True, nullable=False)
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[Optional[str]] = mapped_column(String(2048), nullable=True)
    last_success_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    increment_start: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    increment_stop: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )
    last_queried_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


class SourceDetailRecord(Base):
    """Persisted source-detail URL mapping metadata."""

    __tablename__ = "source_detail_records"
    __table_args__ = (UniqueConstraint("id", name="uq_source_detail_records_id"),)

    source_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    url_md5: Mapped[str] = mapped_column(String(32), primary_key=True)
    url: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    id: Mapped[int] = mapped_column(Integer, nullable=False, unique=True, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[int] = mapped_column(
        Integer, nullable=False, default=SOURCE_STATUS_PENDING, index=True
    )
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


def normalize_source_status(status: Optional[int]) -> int:
    normalized = int(status or SOURCE_STATUS_PENDING)
    if normalized not in VALID_SOURCE_STATUSES:
        raise ValueError(f"Invalid source status: {status}")
    return normalized


def compute_url_md5(url: str) -> str:
    if not url:
        raise ValueError("url is required")
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def _get_database_url(database_url: Optional[str] = None) -> Optional[str]:
    return database_url or resolve_database_url()


def _tune_sqlite(dbapi_conn: Any, _record: Any) -> None:
    """Connection-level PRAGMAs for SQLite.

    ``foreign_keys``: SQLite doesn't enforce it by default. ``WAL`` lets the
    CLI pipeline write while something else reads. ``busy_timeout`` waits out
    lock contention instead of raising "database is locked" immediately.
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()


def _get_engine(database_url: Optional[str] = None):
    resolved_url = _get_database_url(database_url)
    engine = _ENGINE_CACHE.get(resolved_url)
    if engine is None:
        engine = create_engine(resolved_url, future=True)
        if is_sqlite_url(resolved_url):
            event.listen(engine, "connect", _tune_sqlite)
        _ENGINE_CACHE[resolved_url] = engine
    return engine


def _get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    resolved_url = _get_database_url(database_url)
    factory = _SESSION_FACTORY_CACHE.get(resolved_url)
    if factory is None:
        factory = sessionmaker(bind=_get_engine(resolved_url), expire_on_commit=False, future=True)
        _SESSION_FACTORY_CACHE[resolved_url] = factory
    return factory


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    """Public accessor for the cached session factory (e.g. for the API layer)."""
    init_source_db(database_url=database_url)
    return _get_session_factory(database_url=database_url)


def init_source_db(database_url: Optional[str] = None) -> None:
    resolved_url = _get_database_url(database_url)
    if not resolved_url or resolved_url in _INITIALIZED_DATABASES:
        return
    engine = _get_engine(resolved_url)
    _migrate_source_detail_records_table(engine)
    _migrate_source_list_records_table(engine)
    Base.metadata.create_all(engine)
    _INITIALIZED_DATABASES.add(resolved_url)


def _migrate_source_list_records_table(engine: Any) -> None:
    inspector = inspect(engine)
    if "source_list_records" not in inspector.get_table_names():
        return

    columns = {column["name"] for column in inspector.get_columns("source_list_records")}
    additions = {
        "enabled": "BOOLEAN NOT NULL DEFAULT TRUE",
        "consecutive_failures": "INTEGER NOT NULL DEFAULT 0",
        "last_error": "VARCHAR(2048) NULL",
        "last_success_at": "TIMESTAMP NULL",
        "increment_start": "INTEGER NULL",
        "increment_stop": "INTEGER NULL",
    }
    with engine.begin() as conn:
        for name, definition in additions.items():
            if name not in columns:
                conn.execute(
                    text(f"ALTER TABLE source_list_records ADD COLUMN {name} {definition}")
                )


def _migrate_source_detail_records_table(engine: Any) -> None:
    inspector = inspect(engine)
    table_names = set(inspector.get_table_names())
    if "source_detail_records" not in table_names:
        Base.metadata.create_all(engine)
        return

    columns = {column["name"] for column in inspector.get_columns("source_detail_records")}
    pk_columns = tuple(
        inspector.get_pk_constraint("source_detail_records").get("constrained_columns") or []
    )
    unique_constraints = inspector.get_unique_constraints("source_detail_records")
    has_id_unique = any(
        tuple(constraint.get("column_names") or []) == ("id",) for constraint in unique_constraints
    )
    target_pk = ("source_type", "url_md5")
    needs_migration = (
        pk_columns != target_pk
        or "url_md5" not in columns
        or "status" not in columns
        or not has_id_unique
    )
    if not needs_migration:
        return

    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS source_detail_records_migrating"))
        conn.execute(
            text(
                """
                CREATE TABLE source_detail_records_migrating (
                    source_type VARCHAR(32) NOT NULL,
                    url_md5 VARCHAR(32) NOT NULL,
                    url VARCHAR(1024) NOT NULL,
                    id INTEGER NOT NULL,
                    version INTEGER NOT NULL DEFAULT 0,
                    status INTEGER NOT NULL DEFAULT 1,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    PRIMARY KEY (source_type, url_md5),
                    UNIQUE (id)
                )
                """
            )
        )
        rows = conn.execute(text("SELECT * FROM source_detail_records")).mappings().all()
        for row in rows:
            url = str(row["url"])
            conn.execute(
                text(
                    """
                    INSERT INTO source_detail_records_migrating
                        (source_type, url_md5, url, id, version, status, created_at, updated_at)
                    VALUES
                        (:source_type, :url_md5, :url, :id, :version, :status, :created_at, :updated_at)
                    """
                ),
                {
                    "source_type": row["source_type"],
                    "url_md5": row.get("url_md5") or compute_url_md5(url),
                    "url": url,
                    "id": int(row["id"]),
                    "version": int(row.get("version") or 0),
                    "status": int(row.get("status") or SOURCE_STATUS_PENDING),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                },
            )
        conn.execute(text("DROP TABLE source_detail_records"))
        conn.execute(
            text("ALTER TABLE source_detail_records_migrating RENAME TO source_detail_records")
        )


def count_source_items(source_data: Any) -> int:
    if isinstance(source_data, list):
        return len(source_data)
    if isinstance(source_data, dict):
        if isinstance(source_data.get("list"), list):
            return len(source_data["list"])
        if isinstance(source_data.get("data"), list):
            return len(source_data["data"])
        return 1
    return -1


def _iter_source_urls(record: SourceListRecord) -> Iterator[str]:
    start, stop = record.increment_start, record.increment_stop
    if start is None and stop is None:
        yield record.url
        return
    if start is None or stop is None or start >= stop or "{id}" not in record.url:
        raise ValueError("incrementing sources require {id} and a valid start/stop range")
    for source_id in range(start, stop):
        yield record.url.replace("{id}", str(source_id))


def fetch_source_list_data(record: SourceListRecord, timeout: int = 30) -> List[Any]:
    """Fetch one list URL, or internally expand one incrementing URL template."""
    payloads: List[Any] = []
    failures: List[Exception] = []
    incremental = record.increment_start is not None or record.increment_stop is not None
    for url in _iter_source_urls(record):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            payloads.append(response.json())
        except Exception as exc:
            if not incremental:
                raise
            failures.append(exc)

    if not payloads:
        detail = str(failures[-1]) if failures else "no payload returned"
        raise RuntimeError(f"all incrementing source URLs failed: {detail}")
    if failures:
        logger.warning(f"Skipped {len(failures)} failed URLs for {record.url}")
    return payloads


def count_source_payloads(payloads: List[Any]) -> int:
    return sum(max(0, count_source_items(payload)) for payload in payloads)


def _build_source_list_query(
    source_type: Optional[str] = None,
    min_source_count: Optional[int] = None,
    max_source_count: Optional[int] = None,
    queried_before: Optional[datetime] = None,
):
    stmt = select(SourceListRecord).where(SourceListRecord.enabled.is_(True))
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
            payloads = fetch_source_list_data(record, timeout=timeout)
            source_count = count_source_payloads(payloads)
            updated_record = upsert_source_list_record(
                url=record.url,
                source_type=record.source_type,
                source_count=source_count,
                queried_at=queried_at,
                fetch_succeeded=True,
                database_url=database_url,
            )
            for source_data in payloads:
                yield updated_record, source_data
        except Exception as e:
            logger.warning(f"Failed to fetch source list from {record.url}: {e}")
            upsert_source_list_record(
                url=record.url,
                source_type=record.source_type,
                source_count=-1,
                queried_at=queried_at,
                fetch_succeeded=False,
                error=str(e),
                database_url=database_url,
            )


def list_source_detail_records(
    source_type: Optional[str] = None,
    statuses: Optional[List[int]] = None,
    database_url: Optional[str] = None,
) -> List[SourceDetailRecord]:
    """List source-detail records ordered by id."""
    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceDetailRecord)
        if source_type:
            stmt = stmt.where(SourceDetailRecord.source_type == source_type)
        if statuses:
            stmt = stmt.where(
                SourceDetailRecord.status.in_(
                    [normalize_source_status(status) for status in statuses]
                )
            )
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


def replace_source_index_records_for_url(
    records: List[Dict[str, Any]],
    source_type: str,
    url_id: int,
    database_url: Optional[str] = None,
) -> None:
    """Replace source-index rows for one source type/url_id pair."""
    if not source_type:
        raise ValueError("source_type is required")

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        session.execute(
            delete(SourceIndexRecord).where(
                SourceIndexRecord.source_type == source_type,
                SourceIndexRecord.url_id == int(url_id),
            )
        )
        for payload in records:
            md5 = str(payload.get("md5") or "")
            hostname = str(payload.get("hostname") or "")
            record_url_id = payload.get("url_id")
            cate1 = payload.get("cate1")
            if not md5 or not hostname or record_url_id is None or cate1 is None:
                continue
            session.add(
                SourceIndexRecord(
                    md5=md5,
                    source_type=source_type,
                    url_id=int(record_url_id),
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
            status = normalize_source_status(payload.get("status"))
            if record_id is None or not url:
                continue
            session.add(
                SourceDetailRecord(
                    id=int(record_id),
                    url=url,
                    url_md5=compute_url_md5(url),
                    source_type=source_type,
                    version=int(version),
                    status=status,
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


def load_source_detail_status_map(
    source_type: Optional[str] = None,
    database_url: Optional[str] = None,
) -> Dict[int, int]:
    """Load source-detail status keyed by source id."""
    return {
        record.id: record.status
        for record in list_source_detail_records(source_type=source_type, database_url=database_url)
    }


def _next_source_detail_id(session: Session) -> int:
    current_max = session.execute(select(func.max(SourceDetailRecord.id))).scalar_one_or_none()
    if current_max is None:
        return SOURCE_DETAIL_ID_START
    return max(int(current_max) + 1, SOURCE_DETAIL_ID_START)


def add_source_detail_url(
    url: str,
    source_type: str,
    source_id: Optional[int] = None,
    version: int = 0,
    status: int = SOURCE_STATUS_PENDING,
    database_url: Optional[str] = None,
) -> SourceDetailRecord:
    """Add or update a source-detail URL record."""
    return upsert_source_detail_record(
        url=url,
        source_type=source_type,
        source_id=source_id,
        version=version,
        status=status,
        database_url=database_url,
    )


def upsert_source_detail_record(
    url: str,
    source_type: str,
    source_id: Optional[int] = None,
    version: int = 0,
    status: int = SOURCE_STATUS_PENDING,
    database_url: Optional[str] = None,
) -> SourceDetailRecord:
    if not url:
        raise ValueError("url is required")
    if not source_type:
        raise ValueError("source_type is required")

    normalized_source_id = int(source_id) if source_id is not None else None
    normalized_status = normalize_source_status(status)
    url_md5 = compute_url_md5(url)

    init_source_db(database_url=database_url)
    session_factory = _get_session_factory(database_url=database_url)

    with session_factory() as session:
        stmt = select(SourceDetailRecord).where(
            SourceDetailRecord.source_type == source_type,
            SourceDetailRecord.url_md5 == url_md5,
        )
        record = session.execute(stmt).scalar_one_or_none()

        if record is None:
            if normalized_source_id is not None:
                conflict = session.execute(
                    select(SourceDetailRecord).where(SourceDetailRecord.id == normalized_source_id)
                ).scalar_one_or_none()
                if conflict is not None:
                    conflict.source_type = source_type
                    conflict.url = url
                    conflict.url_md5 = url_md5
                    conflict.version = int(version)
                    conflict.status = normalized_status
                    session.commit()
                    session.refresh(conflict)
                    return conflict
            record_id = (
                normalized_source_id
                if normalized_source_id is not None
                else _next_source_detail_id(session)
            )
            record = SourceDetailRecord(
                id=record_id,
                url=url,
                url_md5=url_md5,
                source_type=source_type,
            )
            session.add(record)
        else:
            if normalized_source_id is not None and record.id != normalized_source_id:
                record.id = normalized_source_id
            record.url = url
            record.url_md5 = url_md5
            record.source_type = source_type
        record.version = int(version)
        record.status = normalized_status

        session.commit()
        session.refresh(record)
        return record


def add_source_list_url(
    url: str,
    source_type: str,
    source_count: int = -1,
    queried_at: Optional[datetime] = None,
    increment_start: Optional[int] = None,
    increment_stop: Optional[int] = None,
    database_url: Optional[str] = None,
) -> SourceListRecord:
    """Add or update a source-list URL record with a default unknown count."""
    return upsert_source_list_record(
        url=url,
        source_type=source_type,
        source_count=source_count,
        queried_at=queried_at,
        increment_start=increment_start,
        increment_stop=increment_stop,
        database_url=database_url,
    )


def upsert_source_list_record(
    url: str,
    source_type: str,
    source_count: int,
    queried_at: Optional[datetime] = None,
    fetch_succeeded: Optional[bool] = None,
    error: Optional[str] = None,
    increment_start: Optional[int] = None,
    increment_stop: Optional[int] = None,
    database_url: Optional[str] = None,
) -> SourceListRecord:
    if not url:
        raise ValueError("url is required")
    if not source_type:
        raise ValueError("source_type is required")
    if increment_start is not None or increment_stop is not None:
        if (
            increment_start is None
            or increment_stop is None
            or increment_start >= increment_stop
            or "{id}" not in url
        ):
            raise ValueError("incrementing sources require {id} and a valid start/stop range")

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
                increment_start=increment_start,
                increment_stop=increment_stop,
            )
            session.add(record)
        else:
            record.source_type = source_type
            record.source_count = normalized_count
            record.last_queried_at = queried_at
            if increment_start is not None:
                record.increment_start = increment_start
                record.increment_stop = increment_stop

        if fetch_succeeded is True:
            record.last_success_at = queried_at
            record.consecutive_failures = 0
            record.last_error = None
        elif fetch_succeeded is False:
            record.consecutive_failures += 1
            record.last_error = (error or "采集失败")[:2048]

        session.commit()
        session.refresh(record)
        return record
