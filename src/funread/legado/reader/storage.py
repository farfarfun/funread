"""阅读端的持久化：源偏好、书架、阅读进度、章节缓存。

和 `manage/source/storage.py` 同一套路子（SQLAlchemy declarative + `create_all`
+ 手写迁移，不引 Alembic），但表是独立的一组，engine / session 工厂复用那边的缓存
—— 同一个库开两个连接池没有意义。
"""

import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from farlog import getLogger
from sqlalchemy import Boolean, DateTime, Integer, String, Text, delete, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from ..manage.source.storage import _get_engine, _get_session_factory, utcnow

logger = getLogger("funread")


class ReaderBase(DeclarativeBase):
    """阅读端表的 declarative base。

    和采集侧的 `Base` 分开：`init_reader_db()` 不该顺手把采集侧的表也建出来，
    反过来也一样。
    """


class ReaderSourcePref(ReaderBase):
    """一个源在阅读端的可用性与排序权重。

    `is_complete` / `needs_js` 是**静态**扫描结果（源 JSON 里的八个核心字段齐不齐、
    有没有 JS 规则），一次算好存下来，不必每次搜索都重新解析 13k 个文件。
    `fail_count` / `last_ok_at` 是**动态**的实跑结果 —— 语料里 `status==2` 的源
    绝大多数其实已经死了，真正能用哪些只能靠跑出来。
    """

    __tablename__ = "reader_source_prefs"

    source_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    url_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True, index=True)
    weight: Mapped[int] = mapped_column(Integer, nullable=False, default=0, index=True)
    is_complete: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    needs_js: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    fail_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_ok_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[Optional[str]] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderShelfBook(ReaderBase):
    """书架上的一本书。

    主键是 `book_key`（书名+作者的 md5）而不是 (源, bookUrl)：同一本书在不同源下
    的 URL 完全不同，换源时如果主键跟着源走，书架上就会冒出两条同名记录、进度也
    各记各的。源相关的字段（`source_type`/`url_id`/`book_url`）是「当前读的是哪个源」，
    换源时原地改写。
    """

    __tablename__ = "reader_shelf"

    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    author: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    cover_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    intro: Mapped[str] = mapped_column(Text, nullable=False, default="")
    source_type: Mapped[str] = mapped_column(String(32), nullable=False, default="book")
    url_id: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    book_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    toc_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    last_chapter: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderProgress(ReaderBase):
    """阅读进度。一本书一条，跟着 `book_key` 走，换源不丢。

    `char_offset` 存的是正文里的字符偏移而不是滚动像素 —— 换了字号/字体/设备之后
    像素值毫无意义，字符偏移还能换算回大致位置。
    """

    __tablename__ = "reader_progress"

    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    chapter_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    chapter_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    chapter_name: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    char_offset: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=utcnow, onupdate=utcnow, nullable=False
    )


class ReaderChapterCache(ReaderBase):
    """已抓取的章节正文。既是缓存也是离线下载的落点。"""

    __tablename__ = "reader_chapter_cache"

    book_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    chapter_index: Mapped[int] = mapped_column(Integer, primary_key=True)
    chapter_url: Mapped[str] = mapped_column(String(1024), nullable=False, default="")
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, nullable=False)


_INITIALIZED_DATABASES: set = set()


def compute_book_key(name: str, author: str = "") -> str:
    """书名 + 作者的稳定标识。

    作者可能缺失（搜索结果里很常见），所以归一化成 `书名\\n作者` 再取 md5 —— 空作者
    照样得到一个稳定的 key，而不是和另一本同名书撞上。
    """
    normalized = f"{(name or '').strip()}\n{(author or '').strip()}"
    if not normalized.strip():
        raise ValueError("name is required")
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def init_reader_db(database_url: Optional[str] = None) -> None:
    engine = _get_engine(database_url)
    key = str(engine.url)
    if key in _INITIALIZED_DATABASES:
        return
    ReaderBase.metadata.create_all(engine)
    _INITIALIZED_DATABASES.add(key)


def get_session_factory(database_url: Optional[str] = None) -> sessionmaker:
    init_reader_db(database_url=database_url)
    return _get_session_factory(database_url=database_url)


# ------------------------------------------------------------------ 源偏好


def upsert_source_prefs(
    records: List[Dict[str, Any]],
    database_url: Optional[str] = None,
) -> int:
    """批量写入源偏好。已存在的按字段覆盖，没给的字段保持原值。

    注册表扫描一次会写几千条，所以走一个事务、一次性 `merge`。
    """
    if not records:
        return 0
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        existing = {
            (row.source_type, row.url_id): row
            for row in session.execute(select(ReaderSourcePref)).scalars().all()
        }
        for payload in records:
            key = (str(payload["source_type"]), int(payload["url_id"]))
            row = existing.get(key)
            if row is None:
                row = ReaderSourcePref(source_type=key[0], url_id=key[1])
                session.add(row)
                existing[key] = row
            for field, value in payload.items():
                if field in ("source_type", "url_id"):
                    continue
                if hasattr(row, field):
                    setattr(row, field, value)
        session.commit()
    return len(records)


def list_source_prefs(
    source_type: str = "book",
    enabled_only: bool = True,
    limit: Optional[int] = None,
    database_url: Optional[str] = None,
) -> List[ReaderSourcePref]:
    """按「真跑通过的优先、失败少优先、权重高优先」列出源。

    排序顺序就是选源策略的全部 —— 聚合搜索只取前 N 个，排错了等于没过滤。
    第一项是 `last_ok_at IS NULL` 升序：**实跑成功过**的源排在从没试过的前面。
    采集侧的 `status==2` 只说明某次 GET 过站点首页，拿它当「能读」的依据会得到
    个位数百分比的命中率；真正可信的信号只有自己跑出来的那一次成功。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderSourcePref).where(ReaderSourcePref.source_type == source_type)
        if enabled_only:
            stmt = stmt.where(ReaderSourcePref.enabled.is_(True))
        stmt = stmt.order_by(
            ReaderSourcePref.last_ok_at.is_(None).asc(),
            ReaderSourcePref.fail_count.asc(),
            ReaderSourcePref.weight.desc(),
            ReaderSourcePref.url_id.asc(),
        )
        if limit is not None:
            stmt = stmt.limit(int(limit))
        return list(session.execute(stmt).scalars().all())


def record_source_result(
    source_type: str,
    url_id: int,
    ok: bool,
    error: str = "",
    disable_after: Optional[int] = None,
    database_url: Optional[str] = None,
) -> None:
    """记一次实跑结果。成功清零失败计数，失败累加。

    `disable_after` 给的是「连续失败多少次就停用」。默认不停用：站点临时抽风很常见，
    自动停用太激进会把能用的源一点点耗光。真正该立刻停用的是
    `JsRequiredError` 那种结构性不支持 —— 调用方显式传 `disable_after=1`。
    """
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderSourcePref, (source_type, int(url_id)))
        if row is None:
            row = ReaderSourcePref(source_type=source_type, url_id=int(url_id))
            session.add(row)
        if ok:
            row.fail_count = 0
            row.last_ok_at = utcnow()
            row.last_error = None
        else:
            row.fail_count = int(row.fail_count or 0) + 1
            row.last_error = (error or "")[:1024] or None
            if disable_after is not None and row.fail_count >= int(disable_after):
                row.enabled = False
        session.commit()


# ------------------------------------------------------------------ 书架


def list_shelf(database_url: Optional[str] = None) -> List[ReaderShelfBook]:
    """书架列表，最近读过的排前面。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = select(ReaderShelfBook).order_by(ReaderShelfBook.updated_at.desc())
        return list(session.execute(stmt).scalars().all())


def get_shelf_book(book_key: str, database_url: Optional[str] = None) -> Optional[ReaderShelfBook]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderShelfBook, book_key)


def upsert_shelf_book(payload: Dict[str, Any], database_url: Optional[str] = None) -> str:
    """加书入架 / 更新书架条目，返回 `book_key`。

    调用方可以不给 `book_key`，由书名+作者算出来 —— 前端拿到的搜索结果里本来就
    只有这两样。
    """
    book_key = payload.get("book_key") or compute_book_key(
        str(payload.get("name") or ""), str(payload.get("author") or "")
    )
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderShelfBook, book_key)
        if row is None:
            row = ReaderShelfBook(book_key=book_key)
            session.add(row)
        for field, value in payload.items():
            if field == "book_key":
                continue
            if hasattr(row, field) and value is not None:
                setattr(row, field, value)
        row.updated_at = utcnow()
        session.commit()
    return book_key


def remove_shelf_book(book_key: str, database_url: Optional[str] = None) -> bool:
    """下架。连带清掉进度和章节缓存 —— 留着就是永远不会被读到的垃圾。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderShelfBook, book_key)
        if row is None:
            return False
        session.delete(row)
        session.execute(delete(ReaderProgress).where(ReaderProgress.book_key == book_key))
        session.execute(delete(ReaderChapterCache).where(ReaderChapterCache.book_key == book_key))
        session.commit()
        return True


# ------------------------------------------------------------------ 进度


def get_progress(book_key: str, database_url: Optional[str] = None) -> Optional[ReaderProgress]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderProgress, book_key)


def save_progress(
    book_key: str,
    chapter_index: int,
    chapter_url: str = "",
    chapter_name: str = "",
    char_offset: int = 0,
    database_url: Optional[str] = None,
) -> None:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderProgress, book_key)
        if row is None:
            row = ReaderProgress(book_key=book_key)
            session.add(row)
        row.chapter_index = int(chapter_index)
        row.chapter_url = chapter_url or ""
        row.chapter_name = chapter_name or ""
        row.char_offset = max(0, int(char_offset))
        row.updated_at = utcnow()
        #  书架上的 updated_at 跟着动，书架排序才会把在读的书顶上去
        book = session.get(ReaderShelfBook, book_key)
        if book is not None:
            book.updated_at = row.updated_at
        session.commit()


# ------------------------------------------------------------------ 章节缓存


def get_cached_chapter(
    book_key: str,
    chapter_index: int,
    database_url: Optional[str] = None,
) -> Optional[ReaderChapterCache]:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        return session.get(ReaderChapterCache, (book_key, int(chapter_index)))


def save_cached_chapter(
    book_key: str,
    chapter_index: int,
    chapter_url: str,
    title: str,
    content: str,
    database_url: Optional[str] = None,
) -> None:
    """写章节缓存。空正文不写 —— 把一次失败的抓取缓存下来等于永久坏掉这一章。"""
    if not (content or "").strip():
        return
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        row = session.get(ReaderChapterCache, (book_key, int(chapter_index)))
        if row is None:
            row = ReaderChapterCache(book_key=book_key, chapter_index=int(chapter_index))
            session.add(row)
        row.chapter_url = chapter_url or ""
        row.title = title or ""
        row.content = content
        session.commit()


def list_cached_chapter_indexes(
    book_key: str,
    database_url: Optional[str] = None,
) -> List[int]:
    """已缓存的章节序号。前端据此画「已下载」标记，不必逐章问。"""
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        stmt = (
            select(ReaderChapterCache.chapter_index)
            .where(ReaderChapterCache.book_key == book_key)
            .order_by(ReaderChapterCache.chapter_index)
        )
        return [int(value) for value in session.execute(stmt).scalars().all()]


def clear_chapter_cache(book_key: str, database_url: Optional[str] = None) -> int:
    session_factory = get_session_factory(database_url)
    with session_factory() as session:
        result = session.execute(
            delete(ReaderChapterCache).where(ReaderChapterCache.book_key == book_key)
        )
        session.commit()
        return int(result.rowcount or 0)


__all__ = [
    "ReaderBase",
    "ReaderChapterCache",
    "ReaderProgress",
    "ReaderShelfBook",
    "ReaderSourcePref",
    "clear_chapter_cache",
    "compute_book_key",
    "get_cached_chapter",
    "get_progress",
    "get_session_factory",
    "get_shelf_book",
    "init_reader_db",
    "list_cached_chapter_indexes",
    "list_shelf",
    "list_source_prefs",
    "record_source_result",
    "remove_shelf_book",
    "save_cached_chapter",
    "save_progress",
    "upsert_shelf_book",
    "upsert_source_prefs",
]
