"""阅读端服务层：选源、聚合搜索、书架、进度、章节缓存。

和 `engine/` 的分工：`engine/` 是纯求值、不碰 IO 也不碰 DB；`reader/` 负责把它
接到真实的归档文件、网络和数据库上。所以**这一层可以** import requests /
sqlalchemy / funread.legado.manage，`engine/` 不行。
"""

from .registry import SourceRegistry
from .service import (
    DEFAULT_SEARCH_SOURCES,
    DEFAULT_SEARCH_WORKERS,
    AggregatedBook,
    ReaderService,
)
from .storage import (
    ReaderChapterCache,
    ReaderProgress,
    ReaderShelfBook,
    ReaderSourcePref,
    clear_chapter_cache,
    compute_book_key,
    get_cached_chapter,
    get_progress,
    get_shelf_book,
    init_reader_db,
    list_cached_chapter_indexes,
    list_shelf,
    list_source_prefs,
    record_source_result,
    remove_shelf_book,
    save_cached_chapter,
    save_progress,
    upsert_shelf_book,
    upsert_source_prefs,
)

__all__ = [
    "DEFAULT_SEARCH_SOURCES",
    "DEFAULT_SEARCH_WORKERS",
    "AggregatedBook",
    "ReaderChapterCache",
    "ReaderProgress",
    "ReaderService",
    "ReaderShelfBook",
    "ReaderSourcePref",
    "SourceRegistry",
    "clear_chapter_cache",
    "compute_book_key",
    "get_cached_chapter",
    "get_progress",
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
