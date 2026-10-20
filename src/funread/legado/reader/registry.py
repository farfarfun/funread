"""候选源池：运行时只从数据库加载规则，本地归档仅供显式迁移。

**不要把采集侧的 `status==2` 当作可用性依据。** 那个标记只代表某次 GET 过站点首页
（`source/check/task.py` 从头到尾不碰 `searchUrl`），实测 150 个 `status==2` 的源
里只有 6 个能从搜索走到正文。这里只拿它当一个很弱的排序加分项，真正的筛选靠
`record_source_result()` 积累出来的实跑结果。
"""

import os
import threading
import time
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from farlog import getLogger

from ..engine import SourceSpec, load_source
from ..engine.js import default_js_runtime
from ..manage.download.core.store import LocalSourceStore
from ..manage.source.storage import add_source_detail_url, load_source_detail_url_map
from ..manage.utils import url_to_hostname
from .storage import (
    ReaderSourcePref,
    get_source_json,
    iter_source_json,
    list_source_prefs,
    upsert_source_prefs,
)

logger = getLogger("funread")

#  采集侧「首页可达」的标记值。只当排序加分，不当过滤条件。
SOURCE_STATUS_AVAILABLE = 2

#  一次写库的批大小。全量扫描有上万条，攒批比逐条 commit 快一个数量级。
SCAN_BATCH_SIZE = 500
_INGEST_LOCK = threading.Lock()
SPEC_CACHE_SECONDS = 60


def _rss_identity(url: str) -> str:
    parsed = urlsplit(url.strip())
    return urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path.rstrip("/"), parsed.query, "")
    )


class SourceRegistry:
    """按 url_id 装载源，并维护候选池。

    `source_type` 是 `"book"` 或 `"rss"`，决定数据库分区及旧归档子目录。
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
        self._store: Optional[LocalSourceStore] = None
        self._specs: Dict[int, Tuple[float, SourceSpec]] = {}

    @property
    def store(self) -> LocalSourceStore:
        if self._store is None:
            self._store = LocalSourceStore(path=self.cache_root, cate1=self.source_type)
        return self._store

    # ------------------------------------------------------------------ 装载

    def load_spec(self, url_id: int) -> Optional[SourceSpec]:
        """按 url_id 取归一化后的源。取不到 / 解析不了都返回 None。

        成功结果短时缓存；缺失不缓存，其他服务器入库后即可被看见。
        """
        key = int(url_id)
        cached = self._specs.get(key)
        if cached and time.monotonic() - cached[0] < SPEC_CACHE_SECONDS:
            return cached[1]
        spec = self._load_spec_uncached(key)
        if spec is not None:
            self._specs[key] = (time.monotonic(), spec)
        else:
            self._specs.pop(key, None)
        return spec

    def _load_spec_uncached(self, url_id: int) -> Optional[SourceSpec]:
        data = get_source_json(self.source_type, url_id, self.database_url)
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
        root = os.path.join(self.cache_root, self.source_type, "source")
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

    def _verdict(
        self, url_id: int, data: Dict[str, Any], stats: Dict[str, int]
    ) -> Optional[Dict[str, Any]]:
        try:
            spec = load_source(data, source_type=self.source_type)
        except Exception as exc:
            logger.debug(f"Failed to load source {self.source_type}/{url_id}: {exc}")
            return None
        is_complete = spec.is_complete
        needs_js = spec.needs_js()
        web_view = spec.is_web_view
        has_explore = bool(
            any(kind.url for kind in spec.explore_kinds())
            and (
                spec.has_rule("ruleExplore", "bookList") or spec.has_rule("ruleSearch", "bookList")
            )
        )
        js_available = self.source_type == "book" and default_js_runtime().available()
        enabled = bool(is_complete and (not needs_js or js_available) and not web_view)
        for field, present in (
            ("complete", is_complete),
            ("needs_js", needs_js),
            ("web_view", web_view),
            ("has_explore", has_explore and enabled),
            ("enabled", enabled),
        ):
            stats[field] += int(present)
        return {
            "source_type": self.source_type,
            "url_id": url_id,
            "name": (spec.name or "")[:255],
            # 归档可能含数百份重复候选；只保存解析器选中的那份规则。
            "source_json": {
                key: value
                for key, value in spec.raw.items()
                if key not in ("bookSourceComment", "sourceComment")
            },
            "enabled": enabled,
            "weight": 1 if data.get("status") == SOURCE_STATUS_AVAILABLE else 0,
            "is_complete": is_complete,
            "needs_js": needs_js,
            "has_explore": has_explore,
        }

    def ingest(self, payloads: List[Any]) -> int:
        """Persist rules from downloaded Legado lists using existing source IDs."""
        with _INGEST_LOCK:
            return self._ingest_locked(payloads)

    def _ingest_locked(self, payloads: List[Any]) -> int:
        url_key = "bookSourceUrl" if self.source_type == "book" else "sourceUrl"
        url_map = load_source_detail_url_map(self.source_type, self.database_url)
        if self.source_type == "rss":
            for url_id, saved in iter_source_json("rss", self.database_url):
                raw_url = saved.get("sourceUrl")
                if isinstance(raw_url, str):
                    url_map.setdefault(_rss_identity(raw_url), url_id)
        rows: Dict[int, Dict[str, Any]] = {}
        unique: Dict[str, Tuple[Tuple[bool, bool, bool, bool, int], Dict[str, Any]]] = {}
        stats = dict.fromkeys(("complete", "needs_js", "web_view", "has_explore", "enabled"), 0)
        for payload in payloads:
            if isinstance(payload, list):
                items = payload
            elif isinstance(payload, dict):
                items = next(
                    (
                        payload[key]
                        for key in ("list", "data")
                        if isinstance(payload.get(key), list)
                    ),
                    [payload],
                )
            else:
                continue
            for source in items:
                if not isinstance(source, dict):
                    continue
                raw_url = source.get(url_key)
                hostname = url_to_hostname(raw_url) if isinstance(raw_url, str) else None
                if not hostname:
                    continue
                identity = _rss_identity(raw_url) if self.source_type == "rss" else hostname
                try:
                    spec = load_source(source, source_type=self.source_type)
                except Exception:
                    continue
                quality = (
                    spec.is_complete,
                    not spec.is_web_view,
                    not spec.needs_js(),
                    bool(
                        any(kind.url for kind in spec.explore_kinds())
                        and spec.has_rule("ruleExplore", "bookList")
                    ),
                    len(spec.raw),
                )
                if identity not in unique or quality > unique[identity][0]:
                    unique[identity] = (quality, source)
        for identity, (_, source) in unique.items():
            url_id = url_map.get(identity)
            if url_id is None:
                url_id = add_source_detail_url(
                    identity, self.source_type, database_url=self.database_url
                ).id
                url_map[identity] = url_id
            row = self._verdict(url_id, source, stats)
            if row is not None:
                rows[url_id] = row
        values = list(rows.values())
        for start in range(0, len(values), SCAN_BATCH_SIZE):
            upsert_source_prefs(
                values[start : start + SCAN_BATCH_SIZE], database_url=self.database_url
            )
        self.invalidate()
        return len(values)

    def scan(self, limit: Optional[int] = None) -> Dict[str, int]:
        """只重算数据库里已有的规则，不读取本机归档。

        只写静态字段（`name`/`enabled`/`weight`/`is_complete`/`needs_js`），
        **不碰** `fail_count`/`last_ok_at`/`last_error` —— 那些是实跑积累下来的，
        重扫一次不该把它们抹掉。
        """
        with _INGEST_LOCK:
            return self._scan_locked(limit, archive=False)

    def import_archive(self, limit: Optional[int] = None) -> Dict[str, int]:
        """一次性迁移旧归档；服务接口不会调用。"""
        with _INGEST_LOCK:
            return self._scan_locked(limit, archive=True)

    def _scan_locked(self, limit: Optional[int], *, archive: bool) -> Dict[str, int]:
        stats = {
            "scanned": 0,
            "complete": 0,
            "needs_js": 0,
            "web_view": 0,
            "has_explore": 0,
            "enabled": 0,
        }
        batch: List[Dict[str, Any]] = []
        seen: set[int] = set()
        source_rows = (
            self.iter_archive()
            if archive
            else iter_source_json(self.source_type, self.database_url)
        )
        for url_id, data in source_rows:
            if limit is not None and stats["scanned"] >= limit:
                break
            if url_id in seen:
                continue
            seen.add(url_id)
            stats["scanned"] += 1
            row = self._verdict(url_id, data, stats)
            if row is None:
                continue
            batch.append(row)
            if len(batch) >= SCAN_BATCH_SIZE:
                upsert_source_prefs(batch, database_url=self.database_url)
                batch = []

        if batch:
            upsert_source_prefs(batch, database_url=self.database_url)
        self.invalidate()
        logger.info(f"Reader registry scan finished: {stats}")
        return stats

    # ------------------------------------------------------------------ 选源

    def prefs(
        self,
        limit: Optional[int] = None,
        explore_only: bool = False,
    ) -> List[ReaderSourcePref]:
        return list_source_prefs(
            source_type=self.source_type,
            enabled_only=True,
            limit=limit,
            explore_only=explore_only,
            with_rules_only=True,
            database_url=self.database_url,
        )

    def candidates(self, limit: int = 20) -> List[Tuple[int, SourceSpec]]:
        """取前 `limit` 个真能装载的源。"""
        picked: List[Tuple[int, SourceSpec]] = []
        for item in self.iter_candidates():
            picked.append(item)
            if len(picked) >= limit:
                break
        return picked

    def iter_candidates(self) -> Iterator[Tuple[int, SourceSpec]]:
        """按候选池顺序惰性产出 `(url_id, spec)`，跳过装载不出来的。

        生成器避免一次把所有规则 JSON 读进内存。

        顺序来自 `list_source_prefs` 的 ORDER BY：**实跑成功过的排最前**。所以用得
        越久，搜索命中得越快 —— 能用的源会浮到池子顶部。
        """
        for pref in self.prefs(limit=None):
            spec = self.load_spec(pref.url_id)
            if spec is None:
                continue
            yield int(pref.url_id), spec


__all__ = ["SCAN_BATCH_SIZE", "SOURCE_STATUS_AVAILABLE", "SourceRegistry"]
