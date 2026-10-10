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
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import islice
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
from . import matching, storage
from .registry import SourceRegistry

logger = getLogger("funread")

#  一次聚合搜索最多试几个源。
#
#  这个数字曾经是 12，而书源池实测有 **5,606 个可用源** —— 只打 12 个等于 0.2%，
#  而源语料大面积失效（实测 150 个标着可用的源里只有 6 个真能跑通四段流程），
#  所以 12 个里很可能一个都答不上来，表现成「这本书搜不到」。
#
#  但也不能全试：5,606 个 × 最长 12 秒超时，就算 24 并发也要几十分钟。所以是
#  「分波搜 + 三条终止线」，见 `search_report`。
DEFAULT_SEARCH_SOURCES = 400

#  并发数。比原来的 6 高一截 —— 瓶颈在等待响应而不在本机带宽，而且每个源是
#  不同的站点，并发打它们不会集中压到某一家。再往上调收益递减：慢源的尾巴
#  会越来越长，而 `SEARCH_BUDGET_SECONDS` 本来就会把它们截掉。
DEFAULT_SEARCH_WORKERS = 24

#  一波的大小。等于并发数，所以每一波都是一次满并发、一起收口。
SEARCH_WAVE_SIZE = DEFAULT_SEARCH_WORKERS

#  整轮搜索的墙钟预算（秒）。超了就把已有结果返回 —— **不是报错**。
#  热门书通常第一波就够，冷门书会一直搜到这条线为止。
SEARCH_BUDGET_SECONDS = 25.0

#  有这么多个源答出了结果就停。再往下搜只是把同一本书的来源列表堆长 ——
#  对「找到这本书」没有增量，对换源才有（所以那条路径用更大的值）。
SEARCH_ENOUGH_HITS = 8

#  换源要的是尽可能多的来源，所以目标高一些。
SWITCH_ENOUGH_HITS = 20

#  单个源的抓取超时。聚合搜索是「谁先回谁算」，慢源不值得等。
SEARCH_TIMEOUT = (6, 12)
DETAIL_TIMEOUT = (10, 20)

#  批量下载的节流间隔（秒）。串行 + 停顿，别把人家站点打挂。
DOWNLOAD_INTERVAL = 0.5

#  批量检查更新的节流间隔（秒）。比下载慢一倍 —— 下载是连着打同一个源，而检查
#  更新是挨个打不同的源，节奏快了更像爬虫。
CHECK_INTERVAL = 1.0


def _default_fetcher(timeout=DETAIL_TIMEOUT) -> RequestsFetcher:
    """默认抓取器。每次都是新实例 —— cookie jar 必须按源隔离。"""
    return RequestsFetcher(timeout=timeout, max_retry=1)


def unread_chapters(chapter_count: int, progress: Optional[Any]) -> int:
    """还剩几章没读。书架角标和「有新章节」标记都用这一个数。

    不单独存一列 `unread`：它是 `chapter_count` 和阅读进度的差，存了就要在两边
    任意一个变化时同步，而漏同步的表现是角标永远停在某个数字上。

    `chapter_count = 0` 意味着**还没查过更新**，不是「这本书没有章节」，所以返回
    0 而不是一个凭空的数。没有进度的书整本都没读，所以未读 = 章节总数。
    """
    total = max(0, int(chapter_count or 0))
    if not total:
        return 0
    if progress is None:
        return total
    #  `chapter_index` 是 0 基的，读到第 0 章意味着后面还有 total - 1 章
    return max(0, total - 1 - int(progress.chapter_index or 0))


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

    def search_report(
        self,
        keyword: str,
        max_sources: int = DEFAULT_SEARCH_SOURCES,
        workers: int = DEFAULT_SEARCH_WORKERS,
        enough_hits: int = SEARCH_ENOUGH_HITS,
        budget: float = SEARCH_BUDGET_SECONDS,
    ) -> Dict[str, Any]:
        """跨源聚合搜索，分波进行，连带返回这一轮的源统计。

        按 `书名+作者` 合并同一本书的多个来源。返回顺序是「命中的源最多的排前面」：
        被多个站点同时收录的书，通常既是用户要找的那本，也更有换源余地。

        ## 为什么分波

        书源池实测有 5,606 个可用源，而实跑可用率只有个位数百分比 —— 只打十几个源
        很可能一个都答不上来，表现成「这本书搜不到」。但全试也不行：5,606 × 最长
        12 秒，24 并发也要几十分钟。

        所以按 `SEARCH_WAVE_SIZE` 一波一波搜，**三条终止线任一触发就停**：

        1. `enough_hits` 个源答出了结果 —— 够用了，再搜只是把来源列表堆长；
        2. 墙钟超过 `budget` —— 把已有结果返回，不报错；
        3. 试过的源达到 `max_sources`，或候选池本来就搜完了。

        候选池的顺序是「实跑成功过的排最前」（见 `list_source_prefs` 的 ORDER BY），
        所以用得越久第一波的命中率越高 —— 这是分波能快的前提。

        ## 统计字段

        给前端用的。聚合搜索慢且会有大量单源失败，没有这组数字时「搜不到」和
        「试的源全挂了」在界面上长得一模一样，用户只会以为是 bug。
        `js_skipped` 单独计：那是结构性不支持，换关键词也没用。
        `exhausted` 为真表示候选池真的搜完了 —— 此时「没搜到」是确定的结论，
        而不是「还没搜到那么深」。
        """
        keyword = (keyword or "").strip()
        stats = {
            "sources_tried": 0,
            "sources_ok": 0,
            "hits": 0,
            "js_skipped": 0,
            "failed": 0,
            "waves": 0,
            "elapsed": 0.0,
            "exhausted": False,
            "stopped_by": "empty",
        }
        if not keyword:
            return {"items": [], "total": 0, **stats}

        aggregated: Dict[str, AggregatedBook] = {}
        order: List[str] = []
        started = time.monotonic()
        pool = self.registry.iter_candidates()
        wave_size = max(1, min(workers, SEARCH_WAVE_SIZE))
        stopped_by = "exhausted"

        def _search_one(item: Tuple[int, SourceSpec]) -> Tuple[int, List[SearchBook]]:
            url_id, spec = item
            return url_id, self._run(
                url_id, spec, lambda engine: engine.search(keyword), timeout=SEARCH_TIMEOUT
            )

        while stats["sources_tried"] < max_sources:
            remaining = max_sources - stats["sources_tried"]
            wave = list(islice(pool, min(wave_size, remaining)))
            if not wave:
                stats["exhausted"] = True
                stopped_by = "exhausted"
                break

            stats["waves"] += 1
            stats["sources_tried"] += len(wave)
            with ThreadPoolExecutor(max_workers=max(1, min(workers, len(wave)))) as executor:
                futures = [executor.submit(_search_one, item) for item in wave]
                for future in as_completed(futures):
                    try:
                        url_id, books = future.result()
                    except (JsNotSupportedError, UnsupportedFeatureError) as exc:
                        #  结构性不支持，和「站点这次抽风」是两回事，分开报给前端
                        stats["js_skipped"] += 1
                        logger.debug(f"Search skipped a source needing JS: {exc}")
                        continue
                    except Exception as exc:
                        #  单源失败是常态，已经记进 fail_count 了，这里只留个 debug
                        stats["failed"] += 1
                        logger.debug(f"Search failed on one source: {exc}")
                        continue
                    stats["sources_ok"] += 1
                    usable = [book for book in books if book.name.strip()]
                    if usable:
                        stats["hits"] += 1
                    for book in usable:
                        key = storage.compute_book_key(book.name, book.author)
                        with self._lock:
                            if key in aggregated:
                                aggregated[key].merge(book, url_id)
                            else:
                                aggregated[key] = AggregatedBook(book, url_id)
                                order.append(key)

            #  终止线按「代价从小到大」判：够了最省，超预算次之，源用完最后。
            if stats["hits"] >= enough_hits:
                stopped_by = "enough"
                break
            if time.monotonic() - started >= budget:
                stopped_by = "budget"
                break
        else:
            stopped_by = "max_sources"

        stats["elapsed"] = round(time.monotonic() - started, 2)
        stats["stopped_by"] = stopped_by
        results = [aggregated[key] for key in order]
        results.sort(key=lambda item: len(item.sources), reverse=True)
        items = [item.to_dict() for item in results]
        return {"items": items, "total": len(items), **stats}

    def search(
        self,
        keyword: str,
        max_sources: int = DEFAULT_SEARCH_SOURCES,
        workers: int = DEFAULT_SEARCH_WORKERS,
    ) -> List[Dict[str, Any]]:
        """只要结果、不要统计时的薄包装。"""
        return self.search_report(keyword, max_sources=max_sources, workers=workers)["items"]

    # ------------------------------------------------------------------ 发现

    def explore_sources(self, limit: int = 50, offset: int = 0, q: str = "") -> Dict[str, Any]:
        """能浏览分类的源。

        引擎的 `explore()` 从 M1 就实现了，但一直没有服务层入口 —— 这是接上它的
        第一步。抽样实测 **54.9% 的书源带发现页规则**，其中纯 Python 可跑的占
        19.3%（全量折算约 2,500 个源）。

        只查表（`has_explore` 列在扫描时算好），**不读源文件** —— 否则列个源表就要
        现场解析几千个 JSON。分类名要读文件，所以只给当前这一页补。
        """
        keyword = (q or "").strip()
        rows = self.registry.prefs(limit=None, explore_only=True)
        items = [
            {"url_id": int(pref.url_id), "name": pref.name or f"源 {pref.url_id}"}
            for pref in rows
            if not keyword or keyword in (pref.name or "")
        ]
        total = len(items)
        window = items[offset : offset + limit]
        for item in window:
            spec = self.registry.load_spec(item["url_id"])
            item["kinds"] = (
                [kind.name for kind in spec.explore_kinds() if kind.name] if spec else []
            )
        return {"items": window, "total": total, "limit": limit, "offset": offset}

    def explore_kinds(self, url_id: int) -> List[Dict[str, str]]:
        """一个源的发现页分类。零网络 —— `exploreUrl` 是源 JSON 里的静态字段。

        `url` 是源里**原样声明**的那个串，不是绝对地址，调用方应当把它当**不透明
        令牌**原样回传给 `explore()`。不在这里拼 base_url 是因为它可能带 URL 选项
        （`/list/1,{"method":"POST","body":"..."}`）—— 引擎会先拆选项再拼 base_url，
        顺序反过来就可能把选项拼坏。
        """
        spec = self._spec(url_id)
        kinds = [{"name": kind.name, "url": kind.url} for kind in spec.explore_kinds() if kind.url]
        if not kinds:
            raise LookupError("这个源没有可浏览的分类")
        return kinds

    def explore(self, url_id: int, kind_url: str, page: int = 1) -> List[Dict[str, Any]]:
        """浏览某个分类下的书。

        返回形状和搜索结果一致（每本书带 `sources`），这样详情页那条链不必分两种
        情况处理 —— 只是这里的 `sources` 恒为一项，因为浏览是单源行为。
        """
        spec = self._spec(url_id)
        books = self._run(
            url_id,
            spec,
            lambda engine: engine.explore(kind_url, page=page),
            timeout=SEARCH_TIMEOUT,
        )
        items: List[Dict[str, Any]] = []
        for book in books:
            if not book.name.strip():
                continue
            items.append(AggregatedBook(book, url_id).to_dict())
        return items

    def sources_for(
        self,
        book_key: str,
        user_id: int = storage.LOCAL_USER_ID,
        enough_hits: int = SWITCH_ENOUGH_HITS,
        budget: float = SEARCH_BUDGET_SECONDS,
    ) -> Dict[str, Any]:
        """换源列表：这本书还能在哪些源下读。

        书架上只记了「当前在读的源」，别的来源不会落库 —— 真实的换源列表要靠
        **重新搜一次**得到，所以这里按书名跑一轮聚合搜索再筛。

        ## 为什么不按 `book_key` 精确筛

        `book_key = md5(书名\n作者)`。按它精确筛会**静默丢掉**一批真的有这本书的
        源 —— 作者名写法稍有差异就是另一个 key：空作者、繁简不同、「烽火戏诸侯」
        写成「烽火戏诸候」、或者带了「（著）」。实测这类差异很常见，而被丢掉的源
        往往恰恰是还活着的那些。

        所以改成**按书名宽松匹配**，把候选都列出来、标注作者和最新章节，让用户
        自己判断哪个是同一本。`exact` 字段标明是不是 `book_key` 完全一致 ——
        界面可以把精确的排前面，但不该把其余的藏起来。

        返回的是一个报告（含搜索统计），不是裸列表：界面要能解释「为什么换源列表
        是空的」—— 是真没有别的源，还是这一轮试的源都没答上来。
        """
        book = storage.get_shelf_book(book_key, user_id=user_id, database_url=self.database_url)
        if book is None:
            raise LookupError("书架里没有这本书")

        report = self.search_report(book.name, enough_hits=enough_hits, budget=budget)
        wanted = matching.normalize_chapter_name(book.name)
        current_url_id = int(book.url_id or 0)

        items: List[Dict[str, Any]] = []
        for candidate in report["items"]:
            #  书名归一化后不同就不是同一本 —— 搜索是模糊的，「仙逆」会搜出
            #  「仙逆之再生」这类同前缀的书，不能当成换源选项。
            if matching.normalize_chapter_name(candidate["name"]) != wanted:
                continue
            exact = candidate["book_key"] == book_key
            for source in candidate["sources"]:
                items.append(
                    {
                        "url_id": source["url_id"],
                        "source_name": source["source_name"],
                        "book_url": source["book_url"],
                        "name": candidate["name"],
                        "author": candidate["author"],
                        "last_chapter": candidate["last_chapter"],
                        #  book_key 完全一致 = 书名与作者都对得上
                        "exact": exact,
                        "current": source["url_id"] == current_url_id,
                    }
                )

        #  精确匹配排前面；其中当前在读的那个再往前 —— 用户要先看到自己现在在哪
        items.sort(key=lambda item: (not item["exact"], not item["current"]))
        return {
            "items": items,
            "total": len(items),
            "book_key": book_key,
            "name": book.name,
            **{
                key: report[key]
                for key in (
                    "sources_tried",
                    "sources_ok",
                    "hits",
                    "js_skipped",
                    "failed",
                    "waves",
                    "elapsed",
                    "exhausted",
                    "stopped_by",
                )
            },
        }

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

    def shelf(
        self,
        user_id: int = storage.LOCAL_USER_ID,
        group: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """某个人的书架，带上每本书的阅读进度 —— 前端要在封面上画进度条。

        `group=None` 是整个书架，`group=""` 是只看未分组的那些。
        """
        items = []
        for book in storage.list_shelf(
            user_id=user_id, group=group, database_url=self.database_url
        ):
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
                    "group": book.group,
                    "chapter_count": book.chapter_count,
                    "unread": unread_chapters(book.chapter_count, progress),
                    "last_checked_at": (
                        book.last_checked_at.isoformat() if book.last_checked_at else ""
                    ),
                    "last_check_error": book.last_check_error or "",
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

    def shelf_groups(self, user_id: int = storage.LOCAL_USER_ID) -> List[Dict[str, Any]]:
        return storage.list_shelf_groups(user_id=user_id, database_url=self.database_url)

    def set_shelf_group(
        self,
        book_keys: List[str],
        group: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> int:
        return storage.set_shelf_group(
            book_keys, group, user_id=user_id, database_url=self.database_url
        )

    def rename_shelf_group(
        self,
        old: str,
        new: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> int:
        return storage.rename_shelf_group(old, new, user_id=user_id, database_url=self.database_url)

    # ------------------------------------------------------------------ 检查更新

    def check_updates(
        self,
        user_id: int = storage.LOCAL_USER_ID,
        book_keys: Optional[List[str]] = None,
        interval: float = CHECK_INTERVAL,
        on_progress: Optional[Callable[[Dict[str, int]], None]] = None,
    ) -> Dict[str, int]:
        """逐本重取目录，记下章节总数 —— 未读角标就是从这里来的。

        串行 + 停顿，和 `download_chapters` 同一个理由：这是后台任务，没人在等它，
        而把一个小站打挂的代价是整个源以后都用不了。一本书要两个请求（详情页 +
        目录页），所以一个 50 本的书架大约 100 次请求。

        单本失败只记在那一行的 `last_check_error` 上，不中断整轮 —— 书架上十本书
        来自十个源，其中一个挂了不该让另外九本也查不到。

        `book_keys=None` 查整个书架；给了就只查这几本（界面上的「单本检查更新」）。
        """
        books = storage.list_shelf(user_id=user_id, database_url=self.database_url)
        if book_keys is not None:
            wanted = {key for key in book_keys if key}
            books = [book for book in books if book.book_key in wanted]

        stats = {"total": len(books), "checked": 0, "updated": 0, "failed": 0}
        for position, book in enumerate(books):
            try:
                count, last_chapter = self._count_chapters(book)
            except Exception as exc:
                stats["failed"] += 1
                storage.record_update_check(
                    book.book_key,
                    user_id=user_id,
                    error=str(exc)[:500],
                    database_url=self.database_url,
                )
                logger.debug(f"Update check failed for {book.name}: {exc}")
            else:
                stats["checked"] += 1
                if count > book.chapter_count:
                    stats["updated"] += 1
                storage.record_update_check(
                    book.book_key,
                    user_id=user_id,
                    chapter_count=count,
                    last_chapter=last_chapter,
                    database_url=self.database_url,
                )
            if on_progress is not None:
                on_progress(dict(stats))
            #  最后一本查完就不用再等了
            if interval > 0 and position + 1 < len(books):
                time.sleep(interval)
        return stats

    def _count_chapters(self, book: storage.ReaderShelfBook) -> Tuple[int, str]:
        """数一下这本书现在有多少章，顺便带回最新章节名。

        书架上没有 `url_id`/`book_url` 的条目（手动加的、或者源已经删了）直接报错
        而不是返回 0 —— 返回 0 会被记成「这本书没有章节」，未读角标跟着清零。
        """
        if not book.url_id or not book.book_url:
            raise LookupError("这本书没有记住来源，先换一次源")
        info = self.book_info(book.url_id, book.book_url, name=book.name, author=book.author)
        chapters = self.toc(book.url_id, info)
        return len(chapters), chapters[-1].name if chapters else ""

    def add_to_shelf(
        self,
        payload: Dict[str, Any],
        user_id: int = storage.LOCAL_USER_ID,
    ) -> str:
        return storage.upsert_shelf_book(payload, user_id=user_id, database_url=self.database_url)

    def remove_from_shelf(
        self,
        book_key: str,
        user_id: int = storage.LOCAL_USER_ID,
    ) -> bool:
        return storage.remove_shelf_book(book_key, user_id=user_id, database_url=self.database_url)

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
        remap_progress: bool = True,
    ) -> Dict[str, Any]:
        """换源，并把阅读进度重新定位到新源的目录里。

        缓存必须清掉 —— 不同源的章节切分方式不同，序号对不上。缓存是全局共享的，
        所以这里清掉的是所有人的，但换源本身就意味着原来那套序号不再可信，留着
        比清掉更糟。

        **进度必须重新定位，这是数据正确性问题而不只是体验问题。** 不重定位的话
        书架上仍写着「读到第 500 章」，而那个序号在新源下指向别的内容 —— 用户点
        「续读」会跳到一个陌生位置，而且看不出哪里错了。

        重定位要联网取新源的目录。取不到就保留原进度并在返回值里说明
        （`method="none"`），**不**假装成功 —— 让调用方去提示用户手动选章。

        `remap_progress=False` 用于「还没开始读就换源」：没有进度可搬，省掉一次
        目录抓取。
        """
        before = storage.get_progress(book_key, user_id=user_id, database_url=self.database_url)
        storage.upsert_shelf_book(
            {"book_key": book_key, "url_id": url_id, "book_url": book_url, "toc_url": ""},
            user_id=user_id,
            database_url=self.database_url,
        )
        storage.clear_chapter_cache(book_key, database_url=self.database_url)

        if not remap_progress or before is None:
            return {
                "method": matching.MATCH_NONE if before is not None else "skipped",
                "chapter_index": before.chapter_index if before else 0,
                "chapter_name": before.chapter_name if before else "",
                "total": 0,
            }

        book = storage.get_shelf_book(book_key, user_id=user_id, database_url=self.database_url)
        try:
            info = self.book_info(
                url_id,
                book_url,
                name=book.name if book else "",
                author=book.author if book else "",
            )
            chapters = self.toc(url_id, info)
        except Exception as exc:
            #  新源的目录取不到。源已经换了（那一步是本地的、已经落库），只是
            #  没法重定位 —— 如实返回，别把一次抓取失败变成整个换源失败。
            logger.debug(f"Could not remap progress after switching source: {exc}")
            return {
                "method": matching.MATCH_NONE,
                "chapter_index": before.chapter_index,
                "chapter_name": before.chapter_name,
                "total": 0,
            }

        match = matching.match_chapter_in(
            chapter_name=before.chapter_name,
            chapter_index=before.chapter_index,
            old_total=max(before.chapter_index + 1, len(chapters)),
            new_chapters=chapters,
        )
        if match.method != matching.MATCH_NONE:
            target = chapters[match.index]
            storage.save_progress(
                book_key=book_key,
                chapter_index=match.index,
                chapter_url=target.url,
                chapter_name=target.name,
                #  字符偏移不能跨章沿用 —— 新源这一章的长度和分段都不一样，
                #  沿用会把人扔到一个随机位置。退回章首是唯一诚实的选择。
                char_offset=0,
                user_id=user_id,
                database_url=self.database_url,
            )
        return {
            "method": match.method,
            "chapter_index": match.index,
            "chapter_name": match.name,
            "total": match.total,
            "is_approximate": match.is_approximate,
        }


__all__ = [
    "DEFAULT_SEARCH_SOURCES",
    "DEFAULT_SEARCH_WORKERS",
    "SEARCH_BUDGET_SECONDS",
    "SEARCH_ENOUGH_HITS",
    "SEARCH_WAVE_SIZE",
    "SWITCH_ENOUGH_HITS",
    "AggregatedBook",
    "ReaderService",
]
