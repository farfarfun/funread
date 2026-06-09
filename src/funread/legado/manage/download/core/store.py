"""Local source storage primitives."""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from nltfile import funos
from nltfile.compress import tarfile
from nltlog import getLogger
from tqdm import tqdm

from .constants import DEFAULT_BACKUP_ID


logger = getLogger("funread")


class LocalSourceStore:
    """Filesystem-backed local source store."""

    def __init__(self, path: str = "./funread-hub", cate1: str = "rss", *args, **kwargs):
        self.cate1 = cate1
        base_path = Path(path) / cate1
        self.path_rot = str(base_path)
        self.path_bak = str(base_path / "bak")
        self.path_pkl = str(base_path / "pkl")
        self.path_bok = str(base_path / "source")
        self.pkl_url = str(Path(self.path_pkl) / "url_info.json")
        self.database_url = kwargs.get("database_url")
        self.pkl_md5 = str(Path(self.path_pkl) / "source_info.json")

        self.url_map: Dict[str, int] = {}
        self.md5_set: Dict[str, Dict[str, Any]] = {}
        self.current_id = 1
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        for path in [self.path_bak, self.path_bok, self.path_pkl]:
            funos.makedirs(path)

    @staticmethod
    def _validate_path(path: str, base_path: str) -> bool:
        try:
            real_path = Path(path).resolve()
            real_base = Path(base_path).resolve()
            return str(real_path).startswith(str(real_base))
        except (OSError, ValueError):
            return False

    @staticmethod
    def _load_json_safely(file_path: str) -> Dict[str, Any]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            raise
        except IOError as e:
            logger.error(f"Failed to read {file_path}: {e}")
            raise

    @staticmethod
    def _save_json_safely(file_path: str, data: Dict[str, Any]) -> None:
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, sort_keys=True, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Failed to write {file_path}: {e}")
            raise

    @staticmethod
    def _extract_persisted_items(data: Any) -> Any:
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return int(value)
            except ValueError:
                return None
        if isinstance(value, (list, tuple)) and len(value) == 1:
            return LocalSourceStore._coerce_int(value[0])
        return None

    @classmethod
    def _normalize_url_map(cls, data: Any) -> Dict[str, int]:
        normalized: Dict[str, int] = {}
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and isinstance(data.get("url"), list):
            url_ids = data.get("url_id", [])
            if not isinstance(url_ids, list):
                raise ValueError("Invalid URL map data: url_id must be a list")
            items = [{"url": url, "url_id": url_id} for url, url_id in zip(data["url"], url_ids)]
        elif isinstance(data, dict):
            items = [{"url": url, "url_id": url_id} for url, url_id in data.items()]
        else:
            raise ValueError(f"Unsupported URL map data type: {type(data).__name__}")

        for item in items:
            if not isinstance(item, dict):
                logger.warning(f"Skipping invalid URL map item: {item!r}")
                continue
            url = item.get("url")
            url_id = cls._coerce_int(item.get("url_id"))
            if not isinstance(url, str) or not url:
                logger.warning(f"Skipping URL map item with invalid url: {item!r}")
                continue
            if url_id is None:
                logger.warning(f"Skipping URL map item with invalid url_id: {item!r}")
                continue
            normalized[url] = url_id

        return normalized

    def get_source_url_key(self) -> str:
        return "sourceUrl"

    @staticmethod
    def add_source_to_candidate(
        md5: str,
        fpath: str,
        source: Dict[str, Any],
        url_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        url_info = url_info or {}
        if os.path.exists(fpath):
            try:
                data = LocalSourceStore._load_json_safely(fpath)
            except (json.JSONDecodeError, IOError):
                data = LocalSourceStore._create_default_data(url_info)
        else:
            data = LocalSourceStore._create_default_data(url_info)

        if data.get("final", False) or not data.get("available", True):
            return

        existing_md5s = LocalSourceStore._collect_existing_md5s(data)
        if md5 not in existing_md5s:
            data["candidate"].append({"md5_list": [md5], "source": source})
            LocalSourceStore._save_json_safely(fpath, data)

    @staticmethod
    def _create_default_data(url_info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "available": True,
            "merged": [],
            "candidate": [],
            "final": False,
            "url_id": url_info.get("url_id", 0),
            "hostname": url_info.get("hostname", ""),
        }

    @staticmethod
    def _collect_existing_md5s(data: Dict[str, Any]) -> List[str]:
        md5_list = []
        for key in ("merged", "candidate"):
            if key in data:
                for item in data[key]:
                    if "md5_list" in item:
                        md5_list.extend(item["md5_list"])
        return md5_list

    def export_sources(self, size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        file_list: List[str] = []
        if os.path.exists(self.path_bok):
            for root, _, files in os.walk(self.path_bok):
                for file in files:
                    if file.endswith(".json"):
                        file_list.append(os.path.join(root, file))

        dd: List[Dict[str, Any]] = []
        for file_path in tqdm(file_list, desc="Exporting sources"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if not data.get("available", True):
                    continue

                for key in ("merged", "candidate"):
                    if key not in data:
                        continue
                    current = data[key]
                    if len(current) == 0:
                        continue
                    current = current[:3] if key == "candidate" else current[:5]
                    for item in current:
                        if "source" not in item or "md5_list" not in item:
                            continue
                        source = item["source"].copy()
                        source_url_key = self.get_source_url_key()
                        if source_url_key in source and item["md5_list"]:
                            source[source_url_key] = (
                                f"{source[source_url_key]}#{item['md5_list'][0][:10]}"
                            )
                        source["customOrder"] = data.get("customOrder", 999999999)
                        dd.append(source)
                        if len(dd) >= size:
                            yield dd
                            dd = []
                    break
            except (IOError, json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to process {file_path}: {e}")
                continue
        if dd:
            yield dd

    def loads(self) -> None:
        logger.info("Loading persisted data")
        try:
            from funread.legado.manage import load_source_detail_url_map

            self.url_map = load_source_detail_url_map(
                source_type=self.cate1, database_url=self.database_url
            )
        except ValueError:
            self.url_map = {}
        except Exception as e:
            logger.warning(f"Failed to load URL map from database: {e}")
            self.url_map = {}

        self.current_id = max(self.url_map.values()) if self.url_map else DEFAULT_BACKUP_ID - 1

        if os.path.exists(self.pkl_md5):
            try:
                data = self._extract_persisted_items(self._load_json_safely(self.pkl_md5))
                if isinstance(data, list):
                    self.md5_set = {item["md5"]: item for item in data}
                elif isinstance(data, dict):
                    self.md5_set = data
                else:
                    raise ValueError(f"Unsupported MD5 index data type: {type(data).__name__}")
            except (IOError, json.JSONDecodeError):
                logger.warning("Failed to load MD5 index, starting fresh")
                self.md5_set = {}
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"Invalid MD5 index data, starting fresh: {e}")
                self.md5_set = {}
        else:
            self.md5_set = {}

    def dumps(self) -> None:
        logger.info("Saving data to persistent storage")
        self._ensure_directories()
        try:
            if self.md5_set:
                self._save_json_safely(self.pkl_md5, {"data": list(self.md5_set.values())})
        except IOError as e:
            logger.error(f"Failed to save data: {e}")
            raise

    def loads_zip(self, zip_file: Optional[str] = None) -> None:
        if os.path.exists(self.path_pkl):
            funos.delete(self.path_pkl)
        if os.path.exists(self.path_bok):
            funos.delete(self.path_bok)

        if zip_file is None:
            if not os.path.exists(self.path_bak):
                logger.warning("Backup directory does not exist")
                return
            files = [
                f for f in os.listdir(self.path_bak) if f.endswith((".tar", ".tar.xz", ".tar.gz"))
            ]
            if len(files) == 0:
                logger.warning("No backup files found")
                return
            files.sort()
            zip_file = os.path.join(self.path_bak, files[-1])

        if not os.path.exists(zip_file):
            logger.error(f"Backup file not found: {zip_file}")
            return

        logger.info(f"Loading backup from {zip_file}")
        try:
            with tarfile.open(zip_file, "r:*") as tar:
                tar.extractall(self.path_rot)
            self.loads()
        except Exception as e:
            logger.error(f"Failed to extract backup: {e}")
            raise

    def dumps_zip(self) -> str:
        self.dumps()
        funos.makedirs(self.path_pkl)
        funos.makedirs(self.path_bok)
        funos.makedirs(self.path_bak)

        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        zip_file = f"{self.path_bak}/{self.cate1}-{timestamp}.tar.xz"

        logger.info(f"Creating backup: {zip_file}")
        try:
            with tarfile.open(zip_file, "w|xz") as tar:
                if os.path.exists(self.path_pkl):
                    tar.add(self.path_pkl, arcname=os.path.basename(self.path_pkl))
                if os.path.exists(self.path_bok):
                    tar.add(self.path_bok, arcname=os.path.basename(self.path_bok))
            logger.info(f"Backup created successfully: {zip_file}")
            return zip_file
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise

    def __enter__(self):
        self.loads()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dumps()
