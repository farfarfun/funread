"""Sync local source files into database records."""

import os
from typing import Any, Dict, List, Optional, Set

from nltlog import getLogger
from nltsecret import read_secret
from nlttask import Task
from tqdm import tqdm

from ...download.core.store import LocalSourceStore
from ...download.sources.book import BookSourceProcessor
from ...download.sources.rss import RSSSourceProcessor
from ..storage import replace_source_detail_records, replace_source_index_records


logger = getLogger("funread")


class SyncLocalSourceRecordsTask(Task):
    """Rebuild source detail/index records from local source files."""

    def __init__(self, path: Optional[str] = None, *args, **kwargs):
        self.path = path or self._read_cache_root()
        super(SyncLocalSourceRecordsTask, self).__init__(*args, **kwargs)

    @staticmethod
    def _read_cache_root() -> str:
        return read_secret(cate1="funread", cate2="cache", cate3="path", cate4="root")

    @staticmethod
    def _create_store(path: str, source_type: str):
        if source_type == "book":
            return BookSourceProcessor(path=path, cate1="book")
        if source_type == "rss":
            return RSSSourceProcessor(path=path, cate1="rss")
        raise ValueError(f"Unsupported source type: {source_type}")

    @staticmethod
    def _iter_md5_values(items: Any) -> List[str]:
        values: List[str] = []
        if not isinstance(items, list):
            return values
        for item in items:
            if not isinstance(item, dict):
                continue
            md5_list = item.get("md5_list", [])
            if not isinstance(md5_list, list):
                continue
            for value in md5_list:
                if isinstance(value, str) and value:
                    values.append(value)
        return values

    @classmethod
    def _count_unmerged_versions(cls, data: Dict[str, Any]) -> int:
        return len(cls._iter_md5_values(data.get("candidate", [])))

    @staticmethod
    def _iter_source_files(store: LocalSourceStore) -> List[str]:
        file_list: List[str] = []
        if not os.path.exists(store.path_bok):
            return file_list
        for root, _, files in os.walk(store.path_bok):
            for name in files:
                if name.endswith(".json"):
                    file_list.append(os.path.join(root, name))
        file_list.sort()
        return file_list

    def _build_records(self, store: LocalSourceStore) -> Dict[str, Any]:
        detail_records: List[Dict[str, Any]] = []
        index_records: List[Dict[str, Any]] = []
        seen_md5: Set[str] = set()

        for file_path in tqdm(self._iter_source_files(store), desc=f"sync-{store.cate1}"):
            try:
                data = store._load_json_safely(file_path)
            except Exception as e:
                logger.warning(f"Skip invalid source file {file_path}: {e}")
                continue

            url_id = data.get("url_id")
            hostname = str(data.get("hostname") or "")
            if url_id is None or not hostname:
                continue

            detail_records.append(
                {
                    "id": int(url_id),
                    "url": hostname,
                    "version": self._count_unmerged_versions(data),
                }
            )

            cate1 = (int(url_id) // 100) * 100
            for key in ("merged", "candidate"):
                items = data.get(key, [])
                if not isinstance(items, list):
                    continue
                for md5 in self._iter_md5_values(items):
                    if md5 in seen_md5:
                        continue
                    seen_md5.add(md5)
                    index_records.append(
                        {
                            "md5": md5,
                            "source_type": store.cate1,
                            "url_id": int(url_id),
                            "hostname": hostname,
                            "cate1": cate1,
                        }
                    )

        return {
            "detail_records": detail_records,
            "index_records": index_records,
        }

    def run_source(self, source_type: str, database_url: Optional[str] = None) -> Dict[str, int]:
        with self._create_store(self.path, source_type=source_type) as store:
            payload = self._build_records(store)
            replace_source_detail_records(
                records=payload["detail_records"],
                source_type=store.cate1,
                database_url=database_url or store.database_url,
            )
            replace_source_index_records(
                records=payload["index_records"],
                source_type=store.cate1,
                database_url=database_url or store.database_url,
            )
            return {
                "details": len(payload["detail_records"]),
                "indexes": len(payload["index_records"]),
            }

    def run_book(self, database_url: Optional[str] = None) -> Dict[str, int]:
        return self.run_source(source_type="book", database_url=database_url)

    def run_rss(self, database_url: Optional[str] = None) -> Dict[str, int]:
        return self.run_source(source_type="rss", database_url=database_url)
