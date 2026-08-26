"""Sync local source files into database records."""

import os
import shutil
from typing import Any, Dict, List, Optional, Set

from nltlog import getLogger
from funsecret import read_secret
from nlttask import Task
from tqdm import tqdm

from ...download.core.store import LocalSourceStore
from ...download.sources.book import BookSourceProcessor
from ...download.sources.rss import RSSSourceProcessor
from ..storage import (
    SOURCE_STATUS_PENDING,
    SOURCE_STATUS_UNAVAILABLE,
    VALID_SOURCE_STATUSES,
    load_source_detail_url_map,
    list_source_detail_records,
    replace_source_detail_records,
    replace_source_index_records,
    replace_source_index_records_for_url,
    upsert_source_detail_record,
)


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
    def _create_store(path: str, source_type: str, database_url: Optional[str] = None):
        if source_type == "book":
            return BookSourceProcessor(path=path, cate1="book", database_url=database_url)
        if source_type == "rss":
            return RSSSourceProcessor(path=path, cate1="rss", database_url=database_url)
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
    def _resolve_status(data: Dict[str, Any]) -> int:
        status = data.get("status")
        if isinstance(status, int) and status in VALID_SOURCE_STATUSES:
            return status
        if data.get("available", True) is False:
            return SOURCE_STATUS_UNAVAILABLE
        return SOURCE_STATUS_PENDING

    @classmethod
    def _build_index_records_for_data(
        cls, store: LocalSourceStore, data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        index_records: List[Dict[str, Any]] = []
        seen_md5: Set[str] = set()
        url_id = data.get("url_id")
        hostname = str(data.get("hostname") or "")
        if url_id is None or not hostname:
            return index_records
        cate1 = (int(url_id) // 100) * 100
        for key in ("merged", "candidate"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for md5 in cls._iter_md5_values(items):
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
        return index_records

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

    @staticmethod
    def _target_file_path(store: LocalSourceStore, url_id: int) -> str:
        cate1 = (int(url_id) // 100) * 100
        return os.path.join(store.path_bok, f"{cate1}-{cate1 + 100}", f"{int(url_id)}.json")

    def _build_canonical_url_id_map(
        self,
        store: LocalSourceStore,
        file_paths: List[str],
        database_url: Optional[str] = None,
    ) -> Dict[str, int]:
        database_url = database_url or store.database_url
        if not database_url:
            return {}

        current_map = load_source_detail_url_map(
            source_type=store.cate1,
            database_url=database_url,
        )
        all_records = list_source_detail_records(database_url=database_url)
        used_ids = {int(record.id) for record in all_records}
        next_id = max(used_ids) if used_ids else 9_999_999

        for file_path in file_paths:
            try:
                data = store._load_json_safely(file_path)
            except Exception:
                continue
            hostname = str(data.get("hostname") or "")
            if not hostname or hostname in current_map:
                continue
            current_url_id = data.get("url_id")
            if isinstance(current_url_id, int) and current_url_id not in used_ids:
                current_map[hostname] = current_url_id
                used_ids.add(current_url_id)
                continue
            next_id += 1
            while next_id in used_ids:
                next_id += 1
            current_map[hostname] = next_id
            used_ids.add(next_id)
        return current_map

    def _reconcile_file_identity(
        self,
        store: LocalSourceStore,
        file_path: str,
        data: Dict[str, Any],
        database_url: Optional[str] = None,
        url_id_map: Optional[Dict[str, int]] = None,
    ) -> tuple[str, Dict[str, Any]]:
        hostname = str(data.get("hostname") or "")
        current_url_id = data.get("url_id")
        if not hostname or current_url_id is None:
            return file_path, data

        if url_id_map is None:
            database_url = database_url or store.database_url
            if not database_url:
                return file_path, data
            url_id_map = load_source_detail_url_map(
                source_type=store.cate1,
                database_url=database_url,
            )

        target_url_id = url_id_map.get(hostname)
        if target_url_id is None or int(current_url_id) == int(target_url_id):
            return file_path, data

        updated_data = dict(data)
        updated_data["url_id"] = int(target_url_id)
        target_file_path = self._target_file_path(store, int(target_url_id))

        if os.path.abspath(target_file_path) != os.path.abspath(file_path):
            os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
            store._save_json_safely(target_file_path, updated_data)
            if os.path.exists(file_path):
                os.remove(file_path)
                current_dir = os.path.dirname(file_path)
                if os.path.isdir(current_dir) and not os.listdir(current_dir):
                    shutil.rmtree(current_dir)
            logger.info(
                "Reconciled local source file id: "
                f"{hostname} {current_url_id} -> {target_url_id}, moved to {target_file_path}"
            )
            return target_file_path, updated_data

        store._save_json_safely(file_path, updated_data)
        logger.info(
            "Reconciled local source file id in place: "
            f"{hostname} {current_url_id} -> {target_url_id}"
        )
        return file_path, updated_data

    def _build_records(self, store: LocalSourceStore) -> Dict[str, Any]:
        detail_records: List[Dict[str, Any]] = []
        index_records: List[Dict[str, Any]] = []
        seen_md5: Set[str] = set()
        database_url = getattr(store, "database_url", None)
        file_paths = self._iter_source_files(store)
        url_id_map = self._build_canonical_url_id_map(
            store=store,
            file_paths=file_paths,
            database_url=database_url,
        )

        for file_path in tqdm(file_paths, desc=f"sync-{store.cate1}"):
            try:
                data = store._load_json_safely(file_path)
            except Exception as e:
                logger.warning(f"Skip invalid source file {file_path}: {e}")
                continue

            try:
                file_path, data = self._reconcile_file_identity(
                    store=store,
                    file_path=file_path,
                    data=data,
                    database_url=database_url,
                    url_id_map=url_id_map,
                )
            except Exception as e:
                logger.warning(f"Failed to reconcile source file id {file_path}: {e}")

            url_id = data.get("url_id")
            hostname = str(data.get("hostname") or "")
            if url_id is None or not hostname:
                continue

            detail_records.append(
                {
                    "id": int(url_id),
                    "url": hostname,
                    "version": self._count_unmerged_versions(data),
                    "status": self._resolve_status(data),
                }
            )

            for payload in self._build_index_records_for_data(store=store, data=data):
                md5 = payload["md5"]
                if md5 in seen_md5:
                    continue
                seen_md5.add(md5)
                index_records.append(payload)

        return {
            "detail_records": detail_records,
            "index_records": index_records,
        }

    def run_source(self, source_type: str, database_url: Optional[str] = None) -> Dict[str, int]:
        with self._create_store(
            self.path,
            source_type=source_type,
            database_url=database_url,
        ) as store:
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

    def sync_file(
        self,
        store: LocalSourceStore,
        file_path: str,
        status: Optional[int] = None,
        database_url: Optional[str] = None,
    ) -> Dict[str, int]:
        data = store._load_json_safely(file_path)
        url_id_map = self._build_canonical_url_id_map(
            store=store,
            file_paths=[file_path],
            database_url=database_url or store.database_url,
        )
        file_path, data = self._reconcile_file_identity(
            store=store,
            file_path=file_path,
            data=data,
            database_url=database_url or store.database_url,
            url_id_map=url_id_map,
        )
        url_id = data.get("url_id")
        hostname = str(data.get("hostname") or "")
        if url_id is None or not hostname:
            return {"details": 0, "indexes": 0}

        upsert_source_detail_record(
            url=hostname,
            source_type=store.cate1,
            source_id=int(url_id),
            version=self._count_unmerged_versions(data),
            status=self._resolve_status(data) if status is None else int(status),
            database_url=database_url or store.database_url,
        )
        index_records = self._build_index_records_for_data(store=store, data=data)
        replace_source_index_records_for_url(
            records=index_records,
            source_type=store.cate1,
            url_id=int(url_id),
            database_url=database_url or store.database_url,
        )
        return {"details": 1, "indexes": len(index_records)}

    def run_book(self, database_url: Optional[str] = None) -> Dict[str, int]:
        return self.run_source(source_type="book", database_url=database_url)

    def run_rss(self, database_url: Optional[str] = None) -> Dict[str, int]:
        return self.run_source(source_type="rss", database_url=database_url)
