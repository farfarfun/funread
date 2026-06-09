"""Source processing primitives."""

import json
import os
import traceback
from typing import Any, Dict, List, Optional, Union

import requests
from nltfile import pickle
from nltlog import getLogger
from funsecret import get_md5_str

from funread.legado.manage.utils import url_to_hostname

from .constants import REQUEST_TIMEOUT
from .store import LocalSourceStore


logger = getLogger("funread")


class SourceProcessor(LocalSourceStore):
    """Fetch, normalize and write source items into local storage."""

    def loader(self) -> None:
        raise NotImplementedError("Subclass must implement loader() method")

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError("Subclass must implement source_format() method")

    def persist_download_record(self, url: str, source_data: Any) -> None:
        try:
            from funread.legado.manage import upsert_source_list_record

            if isinstance(source_data, list):
                source_count = len(source_data)
            elif isinstance(source_data, dict):
                if isinstance(source_data.get("list"), list):
                    source_count = len(source_data["list"])
                elif isinstance(source_data.get("data"), list):
                    source_count = len(source_data["data"])
                elif "error" in source_data:
                    source_count = 0
                else:
                    source_count = 1
            else:
                source_count = 0

            upsert_source_list_record(url=url, source_type=self.cate1, source_count=source_count)
        except ValueError:
            return
        except Exception as e:
            logger.warning(f"Failed to persist download record for {url}: {e}")

    def url_index(self, url: str) -> int:
        if url in self.url_map:
            return self.url_map[url]

        from funread.legado.manage import add_source_detail_url

        record = add_source_detail_url(
            url=url, source_type=self.cate1, database_url=self.database_url
        )
        self.url_map[url] = record.id
        self.current_id = max(self.current_id, record.id)
        return record.id

    def add_source(self, source: Dict[str, Any], *args, **kwargs) -> bool:
        source_url_key = self.get_source_url_key()
        if source is None or len(source) == 0 or source_url_key not in source:
            return False
        try:
            md5 = get_md5_str(json.dumps(source, sort_keys=True))
            source = self.source_format(source)
            if source_url_key not in source:
                logger.warning(f"Source missing '{source_url_key}' field, skipping")
                return False

            source_url = source[source_url_key]
            hostname = url_to_hostname(source_url)
            if hostname is None:
                logger.warning(f"Failed to parse hostname from URL: {source_url}")
                return False

            url_id = self.url_index(hostname)
            cate1 = (url_id // 100) * 100
            fdir = f"{self.path_bok}/{cate1}-{cate1 + 100}/"
            os.makedirs(fdir, exist_ok=True)
            fpath = f"{fdir}/{url_id}.json"

            url_info = {"url_id": url_id, "hostname": hostname, "cate1": cate1}
            self.add_source_to_candidate(md5, fpath, source, url_info=url_info)
            self.md5_set[md5] = {
                "md5": md5,
                "url_id": url_id,
                "hostname": hostname,
                "cate1": cate1,
            }
            return True
        except Exception as e:
            logger.error(f"Error adding source: {e}, traceback: {traceback.format_exc()}")
            return False

    def add_sources(
        self, data: Union[str, List[Dict[str, Any]], Dict[str, Any]], *args, **kwargs
    ) -> int:
        parsed_data = self._parse_input_data(data)
        if parsed_data is None:
            return 0
        if isinstance(parsed_data, dict):
            parsed_data = [parsed_data]
        elif not isinstance(parsed_data, list):
            logger.error(f"Unsupported data type: {type(parsed_data)}")
            return 0
        return sum(1 for source in parsed_data if self.add_source(source, *args, **kwargs))

    def _parse_input_data(self, data: Union[str, Dict, List]) -> Optional[Union[Dict, List]]:
        if isinstance(data, str):
            return self._parse_string_data(data)
        if isinstance(data, (dict, list)):
            return data
        logger.error(f"Invalid data type: {type(data)}")
        return None

    def _parse_string_data(self, data: str) -> Optional[Union[Dict, List]]:
        if data.startswith(("http://", "https://")):
            return self._fetch_from_url(data)
        if os.path.exists(data):
            return self._load_from_file(data)
        if data.strip().startswith(("[", "{")):
            try:
                return json.loads(data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON string: {e}")
                return None
        logger.error(f"Invalid data format: {data[:100]}")
        return None

    def _fetch_from_url(self, url: str) -> Optional[Union[Dict, List]]:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            self.persist_download_record(url, data)
            return data
        except requests.RequestException as e:
            self.persist_download_record(url, {"error": str(e)})
            logger.error(f"Failed to fetch URL {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.persist_download_record(url, {"error": f"Invalid JSON: {e}"})
            logger.error(f"Failed to parse JSON from URL {url}: {e}")
            return None

    def _load_from_file(self, file_path: str) -> Optional[Union[Dict, List]]:
        try:
            if file_path.endswith(".pkl") or file_path.endswith(".pkl.bz2"):
                logger.warning("Pickle format is deprecated, use JSON instead")
                return pickle.load(file_path)
            return self._load_json_safely(file_path)
        except (IOError, Exception) as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            return None
