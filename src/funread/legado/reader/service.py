"""阅读服务：聚合搜索、四段流程、书架、进度、章节缓存。

这一层把「无状态的规则引擎」和「有状态的 DB」缝起来，是 API 路由唯一该调的东西。

两条贯穿全文的约定：

- **单源失败只记账，不往上抛。** 候选池里绝大多数源其实已经死了（实测 150 个里
  只有 6 个能跑通四段），聚合搜索必须假定多数分支会炸；一个源抛异常就 500 的话
  这个功能等于不存在。只有「一个源都没成功」才算整体失败。
- **`JsRequiredError` 立刻停用那个源。** 它不是网络抖动，是结构性不支持 ——
  phase 1 下次再选它结果完全一样，白白占掉一个并发位。
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

from farlog import getLogger

from ..engine import (
    BookInfo,
    BookSourceEngine,
    Chapter,
    ChapterContent,
    JsNotSupportedError,
    SearchBook,
    SourceSpec,
    UnsupportedFeatureError,
)
from ..net import RequestsFetcher
from . import storage
from .registry import SourceRegistry

logger = getLogger("funread")

#  一次聚合搜索最多打几个源 / 几个并发。并发别调太高：这些都是小站，
#  打太狠既容易被限流，也会让慢源拖住整批的收尾。
DEFAULT_SEARCH_SOURCES = 12
DEFAULT_SEARCH_WORKERS = 6

#  单个源的抓取超时。聚合搜索是「谁先回谁算」，慢源不值得等。
SEARCH_TIMEOUT = (6, 12)
DETAIL_TIMEOUT = (10, 20)

#  批量下载的节流间隔（秒）。串行 + 停顿，别把人家站点打挂。
DOWNLOAD_INTERVAL = 0.5


def _default_fetcher(timeout=DETAIL_TIMEOUT) -> RequestsFetcher:
    """默认抓取器。每次都是新实例 —— cookie jar 必须按源隔离。"""
    return RequestsFetcher(timeout=timeout, max_retry=1)


class AggregatedBook:
    """同一本书在多个源下的聚合结果。"""

    def __init__(self, book: SearchBook, url_id: int):
        self.book_key = storage.compute_book_key(book.name, book.author)
        self.name = book.name
        self.author = book.author
        self.cover_url = book.cover_url
        self.intro = book.intro
        self.kind = book.kind
        self.word_count = book.word_count
        self.last_chapter = book.last_chapter
        #  每个来源一条：(url_id, 源名, 这本书在该源下的 URL)
        self.sources: List[Tuple[int, str, str]] = [(url_id, book.source_name, book.book_url)]

    def merge(self, book: SearchBook, url_id: int) -> None:
        if not any(existing[0] == url_id for existing in self.sources):
            self.sources.append((url_id, book.source_name, book.book_url))
        #  先到的不一定最全：缺的字段用后到的补上，已有的不覆盖
        for field in ("cover_url", "intro", "kind", "word_count", "last_chapter"):
            if not getattr(self, field) and getattr(book, field):
                setattr(self, field, getattr(book, field))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "book_key": self.book_key,
            "name": self.name,
            "author": self.author,
            "cover_url": self.cover_url,
            "intro": self.intro,
            "kind": self.kind,
            "word_count": self.word_count,
            "last_chapter": self.last_chapter,
            "sources": [
                {"url_id": url_id, "source_name": name, "book_url": book_url}
                for url_id, name, book_url in self.sources
            ],
        }


class ReaderService:
    """阅读端的门面。一个进程一个实例就够。"""

    def __init__(
        self,
        cache_root: Optional[str] = None,
        database_url: Optional[str] = None,
        registry: Optional[SourceRegistry] = None,
        allow_web_view: bool = False,
        fetcher_factory: Optional[Callable[..., Any]] = None,
    ):
        self.database_url = database_url
        self.registry = registry or SourceRegistry(
            cache_root=cache_root, source_type="book", database_url=database_url
        )
        self.allow_web_view = allow_web_view
        #  可替换，好让单测注入 `StaticFetcher` 而不打网
        self.fetcher_factory = fetcher_factory or _default_fetcher
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 执行

    def _run(
        self,
        url_id: int,
        spec: SourceSpec,
        action: Callable[[BookSourceEngine], Any],
        timeout=DETAIL_TIMEOUT,
    ) -> Any:
        """对一个源跑一次操作，并把成败记进 `reader_source_prefs`。

        每次调用开一个新的 `RequestsFetcher`（也就是一个新 session）：Legado 的
        很多站点靠「首页下发 cookie、搜索接口校验 cookie」工作，cookie jar 必须
        按源隔离，跨源复用反而会把 A 站的 cookie 发给 B 站。
        """
        try:
            with self.fetcher_factory(timeout=timeout) as fetcher:
                engine = BookSourceEngine(spec, fetcher, allow_web_view=self.allow_web_view)
                result = action(engine)
        except (JsNotSupportedError, UnsupportedFeatureError) as exc:
            #  结构性不支持：下次选它结果一样，直接停用
            storage.record_source_result(
                "book",
                url_id,
                ok=False,
                error=str(exc),
                disable_after=1,
                database_url=self.database_url,
            )
            raise
        except Exception as exc:
            storage.record_source_result(
                "book", url_id, ok=False, error=str(exc), database_url=self.database_url
            )
            raise
        storage.record_source_result("book", url_id, ok=True, database_url=self.database_url)
        return result

    # ------------------------------------------------------------------ 搜索

    def search(
        self,
        keyword: str,
        max_sources: int = DEFAULT_SEARCH_SOURCES,
        workers: int = DEFAULT_SEARCH_WORKERS,
    ) -> List[Dict[str, Any]]:
        """跨源聚合搜索。按 `书名+作者` 合并同一本书的多个来源。

        返回顺序是「命中的源最多的排前面」：被多个站点同时收录的书，通常既是
        用户要找的那本，也更有换源余地。
        """
        keyword = (keyword or "").strip()
        if not keyword:
            return []

        candidates = self.registry.candidates(limit=max_sources)
        if not candidates:
            return []

        aggregated: Dict[str, AggregatedBook] = {}
        order: List[str] = []

        def _search_one(item: Tuple[int, SourceSpec]) -> Tuple[int, List[SearchBook]]:
            url_id, spec = item
            books = self._run(
                url_id, spec, lambda engine: engine.search(keyword), timeout=SEARCH_TIMEOUT
            )
            return url_id, books

        with ThreadPoolExecutor(max_workers=max(1, min(workers, len(candidates)))) as pool:
            futures = [pool.submit(_search_one, item) for item in candidates]
            for future in as_completed(futures):
                try:
                    url_id, books = future.result()
                except Exception as exc:
                    #  单源失败是常态，已经记进 fail_count 了，这里只留个 debug
                    logger.debug(f"Search failed on one source: {exc}")
                    continue
                for book in books:
                    if not book.name.strip():
                        continue
                    key = storage.compute_book_key(book.name, book.author)
                    with self._lock:
                        if key in aggregated:
                            aggregated[key].merge(book, url_id)
                        else:
                            aggregated[key] = AggregatedBook(book, url_id)
                            order.append(key)

        results = [aggregated[key] for key in order]
        results.sort(key=lambda item: len(item.sources), reverse=True)
        return [item.to_dict() for item in results]

    def sources_for(
        self,
        book_key: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> List[Dict[str, Any]]:
        """换源列表：这本书还能在哪些源下读。

        书架上只记了「当前在读的源」，别的来源不会落库 —— 真实的换源列表要靠
        重新搜一次得到，所以这里按书名反查。
        """
        book = storage.get_shelf_book(book_key, user_id=user_id, database_url=self.database_url)
        if book is None:
            return []
        for item in self.search(book.name):
            if item["book_key"] == book_key:
                return item["sources"]
        return []

    # ------------------------------------------------------------------ 四段

    def _spec(self, url_id: int) -> SourceSpec:
        spec = self.registry.load_spec(url_id)
        if spec is None:
            raise LookupError(f"源不存在或无法解析：url_id={url_id}")
        return spec

    def book_info(self, url_id: int, book_url: str, name: str = "", author: str = "") -> BookInfo:
        spec = self._spec(url_id)
        book = SearchBook(name=name, author=author, book_url=book_url)
        return self._run(url_id, spec, lambda engine: engine.book_info(book))

    def toc(self, url_id: int, info: BookInfo) -> List[Chapter]:
        spec = self._spec(url_id)
        return self._run(url_id, spec, lambda engine: engine.toc(info))

    def content(
        self,
        url_id: int,
        chapter: Chapter,
        info: Optional[BookInfo] = None,
        book_key: str = "",
    ) -> ChapterContent:
        """取章节正文。有缓存就直接返回缓存，不发请求。"""
        if book_key:
            cached = storage.get_cached_chapter(
                book_key, chapter.index, database_url=self.database_url
            )
            if cached is not None:
                return ChapterContent(
                    text=cached.content, title=cached.title, url=cached.chapter_url
                )

        spec = self._spec(url_id)
        result = self._run(url_id, spec, lambda engine: engine.content(chapter, info=info))
        if book_key:
            storage.save_cached_chapter(
                book_key=book_key,
                chapter_index=chapter.index,
                chapter_url=chapter.url,
                title=result.title or chapter.name,
                content=result.text,
                database_url=self.database_url,
            )
        return result

    # ------------------------------------------------------------------ 离线

    def download_chapters(
        self,
        book_key: str,
        url_id: int,
        chapters: List[Chapter],
        info: Optional[BookInfo] = None,
        interval: float = DOWNLOAD_INTERVAL,
    ) -> Dict[str, int]:
        """批量预取章节正文到缓存。

        串行 + 停顿，不并发：这是后台任务，没人在等它，而把一个小站打挂的代价
        是整个源以后都用不了。已缓存的直接跳过，所以中断后重跑是幂等的。
        """
        import time

        stats = {"total": len(chapters), "downloaded": 0, "cached": 0, "failed": 0}
        for chapter in chapters:
            existing = storage.get_cached_chapter(
                book_key, chapter.index, database_url=self.database_url
            )
            if existing is not None:
                stats["cached"] += 1
                continue
            try:
                self.content(url_id, chapter, info=info, book_key=book_key)
                stats["downloaded"] += 1
            except Exception as exc:
                stats["failed"] += 1
                logger.debug(f"Download failed for chapter {chapter.index}: {exc}")
            if interval > 0:
                time.sleep(interval)
        return stats

    # ------------------------------------------------------------------ 书架

    def shelf(self, user_id: int = storage.LOCAL_USER_ID) -> List[Dict[str, Any]]:
        """某个人的书架，带上每本书的阅读进度 —— 前端要在封面上画进度条。"""
        items = []
        for book in storage.list_shelf(user_id=user_id, database_url=self.database_url):
            progress = storage.get_progress(
                book.book_key, user_id=user_id, database_url=self.database_url
            )
            items.append(
                {
                    "book_key": book.book_key,
                    "name": book.name,
                    "author": book.author,
                    "cover_url": book.cover_url,
                    "intro": book.intro,
                    "url_id": book.url_id,
                    "book_url": book.book_url,
                    "toc_url": book.toc_url,
                    "last_chapter": book.last_chapter,
                    "updated_at": book.updated_at.isoformat() if book.updated_at else "",
                    "progress": (
                        {
                            "chapter_index": progress.chapter_index,
                            "chapter_name": progress.chapter_name,
                            "char_offset": progress.char_offset,
                        }
                        if progress is not None
                        else None
                    ),
                }
            )
        return items

    def add_to_shelf(
        self,
        payload: Dict[str, Any],
        user_id: int = storage.LOCAL_USER_ID,
    ) -> str:
        return storage.upsert_shelf_book(
            payload, user_id=user_id, database_url=self.database_url
        )

    def remove_from_shelf(
        self,
        book_key: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> bool:
        return storage.remove_shelf_book(
            book_key, user_id=user_id, database_url=self.database_url
        )

    def save_progress(
        self,
        book_key: str,
        chapter_index: int,
        chapter_url: str = "",
        chapter_name: str = "",
        char_offset: int = 0,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> None:
        storage.save_progress(
            book_key=book_key,
            chapter_index=chapter_index,
            chapter_url=chapter_url,
            chapter_name=chapter_name,
            char_offset=char_offset,
            user_id=user_id,
            database_url=self.database_url,
        )

    def switch_source(
        self,
        book_key: str,
        url_id: int,
        book_url: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> None:
        """换源。缓存必须清掉 —— 不同源的章节切分方式不同，序号对不上。

        缓存是全局共享的，所以这里清掉的是所有人的 —— 但换源本身就意味着原来那套
        序号不再可信，留着比清掉更糟。
        """
        storage.upsert_shelf_book(
            {"book_key": book_key, "url_id": url_id, "book_url": book_url, "toc_url": ""},
            user_id=user_id,
            database_url=self.database_url,
        )
        storage.clear_chapter_cache(book_key, database_url=self.database_url)


__all__ = [
    "DEFAULT_SEARCH_SOURCES",
    "DEFAULT_SEARCH_WORKERS",
    "AggregatedBook",
    "ReaderService",
]
