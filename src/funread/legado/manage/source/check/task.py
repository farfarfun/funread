"""Source availability checking tasks."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

import requests
from nltlog import getLogger
from nltsecret import read_secret
from nlttask import Task
from tqdm import tqdm

from ...download.core.processor import SourceProcessor
from ...download.sources.book import BookSourceProcessor
from ...download.sources.rss import RSSSourceProcessor
from ..storage import (
    SOURCE_STATUS_AVAILABLE,
    SOURCE_STATUS_PENDING,
    SOURCE_STATUS_UNAVAILABLE,
    load_source_detail_status_map,
)
from ..sync.task import SyncLocalSourceRecordsTask


logger = getLogger("funread")

DEFAULT_CHECK_TIMEOUT = 15
DEFAULT_CHECK_WORKERS = 8
DEFAULT_ALLOWED_STATUSES = {
    SOURCE_STATUS_PENDING,
    SOURCE_STATUS_AVAILABLE,
    SOURCE_STATUS_UNAVAILABLE,
}
DEFAULT_STATUS_PRIORITY = {
    SOURCE_STATUS_PENDING: 0,
    SOURCE_STATUS_UNAVAILABLE: 0,
    SOURCE_STATUS_AVAILABLE: 1,
}


class SourceStatusCheckRunner:
    """Check local source files and update availability status."""

    def __init__(
        self,
        store: SourceProcessor,
        timeout: int = DEFAULT_CHECK_TIMEOUT,
        max_workers: int = DEFAULT_CHECK_WORKERS,
    ):
        self.store = store
        self.timeout = timeout
        self.max_workers = max(1, int(max_workers))

    def run(self, limit: Optional[int] = None) -> Dict[str, int]:
        stats = {"processed": 0, "available": 0, "unavailable": 0, "failed": 0}
        file_paths = self.iter_source_files()
        if limit is not None:
            file_paths = file_paths[:limit]
        total = len(file_paths)
        if total == 0:
            return stats

        with ThreadPoolExecutor(max_workers=min(self.max_workers, total)) as executor:
            futures = [executor.submit(self.check_file, file_path) for file_path in file_paths]
            for future in tqdm(
                as_completed(futures), total=total, desc=f"check-{self.store.cate1}"
            ):
                stats["processed"] += 1
                try:
                    result = future.result()
                except Exception as e:
                    logger.warning(f"Failed to collect source check result: {e}")
                    result = "failed"
                stats[result] += 1
        return stats

    def iter_source_files(self) -> List[str]:
        file_list: List[tuple[int, int, str]] = []
        status_map = self._load_status_map()
        if not os.path.exists(self.store.path_bok):
            return []
        for root, _, files in os.walk(self.store.path_bok):
            for name in files:
                if not name.endswith(".json"):
                    continue
                file_path = os.path.join(root, name)
                url_id = self._extract_url_id_from_path(file_path)
                if url_id is not None:
                    status = status_map.get(url_id, self._read_file_status(file_path))
                    if status not in DEFAULT_ALLOWED_STATUSES:
                        continue
                else:
                    status = self._read_file_status(file_path)
                    if status not in DEFAULT_ALLOWED_STATUSES:
                        continue
                version = self._read_version(file_path)
                file_list.append((DEFAULT_STATUS_PRIORITY.get(status, 99), -version, file_path))
        file_list.sort(key=lambda item: (item[0], item[1], item[2]))
        return [file_path for _, _, file_path in file_list]

    def check_file(self, file_path: str) -> str:
        try:
            data = self.store._load_json_safely(file_path)
            url = self._pick_source_url(data)
            if not url:
                self._update_file_status(
                    file_path=file_path,
                    data=data,
                    status=SOURCE_STATUS_UNAVAILABLE,
                    available=False,
                )
                return "unavailable"
            available = self._check_url_available(url)
            self._update_file_status(
                file_path=file_path,
                data=data,
                status=SOURCE_STATUS_AVAILABLE if available else SOURCE_STATUS_UNAVAILABLE,
                available=available,
            )
            return "available" if available else "unavailable"
        except Exception as e:
            logger.warning(f"Failed to check source file {file_path}: {e}")
            return "failed"

    def _load_status_map(self) -> Dict[int, int]:
        database_url = getattr(self.store, "database_url", None)
        if not database_url:
            return {}
        try:
            return load_source_detail_status_map(
                source_type=self.store.cate1,
                database_url=database_url,
            )
        except Exception as e:
            logger.warning(f"Failed to load source status map for check: {e}")
            return {}

    @staticmethod
    def _extract_url_id_from_path(file_path: str) -> Optional[int]:
        name = os.path.splitext(os.path.basename(file_path))[0]
        return int(name) if name.isdigit() else None

    def _read_file_status(self, file_path: str) -> int:
        try:
            data = self.store._load_json_safely(file_path)
        except Exception:
            return SOURCE_STATUS_PENDING
        status = data.get("status")
        if isinstance(status, int):
            return status
        if data.get("available", True) is False:
            return SOURCE_STATUS_UNAVAILABLE
        return SOURCE_STATUS_PENDING

    def _read_version(self, file_path: str) -> int:
        try:
            data = self.store._load_json_safely(file_path)
        except Exception:
            return 0
        items = data.get("candidate", [])
        if not isinstance(items, list):
            return 0
        version = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            md5_list = item.get("md5_list", [])
            if not isinstance(md5_list, list):
                continue
            version += sum(1 for value in md5_list if isinstance(value, str) and value)
        return version

    def _pick_source_url(self, data: Dict[str, object]) -> str:
        source_url_key = self.store.get_source_url_key()
        for key in ("merged", "candidate"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                source = item.get("source")
                if not isinstance(source, dict):
                    continue
                source_url = source.get(source_url_key)
                if isinstance(source_url, str) and source_url:
                    return source_url
        return ""

    def _check_url_available(self, url: str) -> bool:
        try:
            response = requests.get(
                url,
                timeout=self.timeout,
                allow_redirects=True,
                stream=True,
            )
            response.raise_for_status()
            return True
        except requests.RequestException:
            return False

    def _update_file_status(
        self,
        file_path: str,
        data: Dict[str, object],
        status: int,
        available: bool,
    ) -> None:
        data["status"] = status
        data["available"] = available
        self.store._save_json_safely(file_path, data)
        self._sync_database_record(file_path=file_path, status=status)

    def _sync_database_record(self, file_path: str, status: int) -> None:
        database_url = getattr(self.store, "database_url", None)
        if not database_url:
            return
        SyncLocalSourceRecordsTask(path=self.store.path_rot).sync_file(
            store=self.store,
            file_path=file_path,
            status=status,
            database_url=database_url,
        )


class CheckSourceStatusTask(Task):
    """Run source status checks for a local source store."""

    def __init__(self, path: Optional[str] = None, *args, **kwargs):
        self.path = path or self._read_cache_root()
        super(CheckSourceStatusTask, self).__init__(*args, **kwargs)

    @staticmethod
    def _read_cache_root() -> str:
        return read_secret(cate1="funread", cate2="cache", cate3="path", cate4="root")

    @staticmethod
    def _create_store(
        path: str, source_type: str, database_url: Optional[str] = None
    ) -> SourceProcessor:
        if source_type == "book":
            return BookSourceProcessor(path=path, cate1="book", database_url=database_url)
        if source_type == "rss":
            return RSSSourceProcessor(path=path, cate1="rss", database_url=database_url)
        raise ValueError(f"Unsupported source type: {source_type}")

    def run_source(
        self,
        source_type: str,
        timeout: int = DEFAULT_CHECK_TIMEOUT,
        limit: Optional[int] = None,
        max_workers: int = DEFAULT_CHECK_WORKERS,
    ) -> Dict[str, int]:
        try:
            database_url = read_secret(
                cate1="funread", cate2="cache", cate3="source", cate4="db_url"
            )
        except Exception:
            database_url = None
        with self._create_store(
            self.path,
            source_type=source_type,
            database_url=database_url,
        ) as store:
            runner = SourceStatusCheckRunner(
                store=store,
                timeout=timeout,
                max_workers=max_workers,
            )
            return runner.run(limit=limit)

    def run_book(
        self,
        timeout: int = DEFAULT_CHECK_TIMEOUT,
        limit: Optional[int] = None,
        max_workers: int = DEFAULT_CHECK_WORKERS,
    ):
        return self.run_source(
            source_type="book",
            timeout=timeout,
            limit=limit,
            max_workers=max_workers,
        )

    def run_rss(
        self,
        timeout: int = DEFAULT_CHECK_TIMEOUT,
        limit: Optional[int] = None,
        max_workers: int = DEFAULT_CHECK_WORKERS,
    ):
        return self.run_source(
            source_type="rss",
            timeout=timeout,
            limit=limit,
            max_workers=max_workers,
        )
