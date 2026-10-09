"""候选源池：从本地归档里挑出「阅读端真能用」的源。

全量源 JSON 只存在文件里（`<cache_root>/<book|rss>/source/<bucket>/<url_id>.json`），
库里没有 JSON。所以这一层负责：扫描归档 → 静态判定（字段齐不齐、要不要 JS）→
把结论写进 `reader_source_prefs`，之后选源只查表，不再碰 13k 个文件。

**不要把采集侧的 `status==2` 当作可用性依据。** 那个标记只代表某次 GET 过站点首页
（`source/check/task.py` 从头到尾不碰 `searchUrl`），实测 150 个 `status==2` 的源
里只有 6 个能从搜索走到正文。这里只拿它当一个很弱的排序加分项，真正的筛选靠
`record_source_result()` 积累出来的实跑结果。
"""

import os
from typing import Any, Dict, Iterator, List, Optional, Tuple

from farlog import getLogger

from ..engine import SourceSpec, load_source
from ..manage.download.core.store import LocalSourceStore
from .storage import ReaderSourcePref, list_source_prefs, upsert_source_prefs

logger = getLogger("funread")

#  采集侧「首页可达」的标记值。只当排序加分，不当过滤条件。
SOURCE_STATUS_AVAILABLE = 2

#  一次写库的批大小。全量扫描有上万条，攒批比逐条 commit 快一个数量级。
SCAN_BATCH_SIZE = 500


class SourceRegistry:
    """按 url_id 装载源，并维护候选池。

    `source_type` 是 `"book"` 或 `"rss"` —— 它同时决定归档子目录和
    `reader_source_prefs` 里的分区。
    """

    def __init__(
        self,
        cache_root: Optional[str] = None,
        source_type: str = "book",
        database_url: Optional[str] = None,
    ):
        if cache_root is None:
            from funread.base.config import resolve_cache_root

            cache_root = resolve_cache_root()
        self.cache_root = cache_root
        self.source_type = source_type
        self.database_url = database_url
        self.store = LocalSourceStore(path=cache_root, cate1=source_type)
        self._specs: Dict[int, Optional[SourceSpec]] = {}

    # ------------------------------------------------------------------ 装载

    def load_spec(self, url_id: int) -> Optional[SourceSpec]:
        """按 url_id 取归一化后的源。取不到 / 解析不了都返回 None。

        结果（包括 None）进内存缓存：一次聚合搜索会反复问同一批 url_id，
        而且「这个 url_id 没有文件」本身也值得缓存下来，省掉重复的 stat。
        """
        key = int(url_id)
        if key in self._specs:
            return self._specs[key]
        spec = self._load_spec_uncached(key)
        self._specs[key] = spec
        return spec

    def _load_spec_uncached(self, url_id: int) -> Optional[SourceSpec]:
        data = self.store.load_by_url_id(url_id)
        if data is None:
            return None
        try:
            return load_source(data, source_type=self.source_type)
        except Exception as exc:
            logger.debug(f"Failed to load source {self.source_type}/{url_id}: {exc}")
            return None

    def invalidate(self, url_id: Optional[int] = None) -> None:
        """丢掉内存缓存。`url_id` 为空时整体丢。"""
        if url_id is None:
            self._specs.clear()
        else:
            self._specs.pop(int(url_id), None)

    # ------------------------------------------------------------------ 扫描

    def iter_archive(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """遍历归档，产出 `(url_id, 包装对象)`。坏文件跳过。"""
        root = self.store.path_bok
        if not os.path.isdir(root):
            return
        for dir_path, _, files in os.walk(root):
            for name in sorted(files):
                if not name.endswith(".json"):
                    continue
                stem = name[:-5]
                if not stem.isdigit():
                    continue
                url_id = int(stem)
                data = self.store.load_by_url_id(url_id)
                if data is None:
                    continue
                yield url_id, data

    def scan(self, limit: Optional[int] = None) -> Dict[str, int]:
        """全量扫描归档，把静态判定结果写进 `reader_source_prefs`。

        只写静态字段（`name`/`enabled`/`weight`/`is_complete`/`needs_js`），
        **不碰** `fail_count`/`last_ok_at`/`last_error` —— 那些是实跑积累下来的，
        重扫一次不该把它们抹掉。
        """
        stats = {"scanned": 0, "complete": 0, "needs_js": 0, "web_view": 0, "enabled": 0}
        batch: List[Dict[str, Any]] = []

        for url_id, data in self.iter_archive():
            if limit is not None and stats["scanned"] >= limit:
                break
            stats["scanned"] += 1
            spec = self._load_spec_uncached(url_id)
            if spec is None:
                continue
            is_complete = spec.is_complete
            needs_js = spec.needs_js()
            #  `singleUrl` 型订阅源（归档的 21.7%）规则可能齐、也可能不含 JS，
            #  但工作方式是把页面塞进 WebView —— 纯 Python 跑不了，不能进候选池。
            #  书源没有这个形态，`is_web_view` 对它恒为 False。
            web_view = spec.is_web_view
            enabled = bool(is_complete and not needs_js and not web_view)
            if is_complete:
                stats["complete"] += 1
            if needs_js:
                stats["needs_js"] += 1
            if web_view:
                stats["web_view"] += 1
            if enabled:
                stats["enabled"] += 1
            batch.append(
                {
                    "source_type": self.source_type,
                    "url_id": url_id,
                    "name": (spec.name or "")[:255],
                    "enabled": enabled,
                    #  首页可达只值一分 —— 它几乎预测不了搜索是否能用
                    "weight": 1 if data.get("status") == SOURCE_STATUS_AVAILABLE else 0,
                    "is_complete": is_complete,
                    "needs_js": needs_js,
                }
            )
            if len(batch) >= SCAN_BATCH_SIZE:
                upsert_source_prefs(batch, database_url=self.database_url)
                batch = []

        if batch:
            upsert_source_prefs(batch, database_url=self.database_url)
        logger.info(f"Reader registry scan finished: {stats}")
        return stats

    # ------------------------------------------------------------------ 选源

    def prefs(self, limit: Optional[int] = None) -> List[ReaderSourcePref]:
        return list_source_prefs(
            source_type=self.source_type,
            enabled_only=True,
            limit=limit,
            database_url=self.database_url,
        )

    def candidates(self, limit: int = 20) -> List[Tuple[int, SourceSpec]]:
        """取前 `limit` 个真能装载的源。

        表里标着 enabled 但文件已经不见了的情况是存在的（归档被清理 / 重算过
        url_id），所以多取一些再按装载结果截断，而不是直接拿表里的前 N 条。
        """
        picked: List[Tuple[int, SourceSpec]] = []
        for pref in self.prefs(limit=max(limit * 3, limit + 10)):
            spec = self.load_spec(pref.url_id)
            if spec is None:
                continue
            picked.append((int(pref.url_id), spec))
            if len(picked) >= limit:
                break
        return picked


__all__ = ["SCAN_BATCH_SIZE", "SOURCE_STATUS_AVAILABLE", "SourceRegistry"]
