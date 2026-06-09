"""LLM-driven source merge tasks."""

import copy
import json
import os
import re
import time
from typing import Any, Dict, List, Optional, Protocol

import requests
from nltlog import getLogger
from nltsecret import read_secret
from nlttask import Task

from ...download.core.processor import SourceProcessor
from ...download.sources.book import BookSourceProcessor
from ...download.sources.rss import RSSSourceProcessor
from ...utils import url_to_hostname


logger = getLogger("funread")

DEFAULT_LLM_BASE_URL = "https://api.openai.com/v1"
DEFAULT_LLM_MODEL = "gpt-4.1-mini"
DEFAULT_LLM_TIMEOUT = 1200
DEFAULT_MAX_VERSIONS_PER_MERGE = 8
DEFAULT_MAX_PROMPT_CHARS = 50000
DEFAULT_LLM_MAX_RETRIES = 3
DEFAULT_LLM_RETRY_SLEEP_SECONDS = 2


class SourceMerger(Protocol):
    def merge_sources(
        self,
        source_type: str,
        hostname: str,
        versions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge multiple versions into one final source object."""


VersionItem = Dict[str, Any]


class OpenAICompatibleSourceMerger:
    """Default LLM-based merger using an OpenAI-compatible API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = DEFAULT_LLM_TIMEOUT,
        max_retries: int = DEFAULT_LLM_MAX_RETRIES,
        retry_sleep_seconds: int = DEFAULT_LLM_RETRY_SLEEP_SECONDS,
    ):
        self.base_url = base_url or read_secret(
            cate1="funread", cate2="source", cate3="merge", cate4="base_url"
        )
        self.api_key = api_key or read_secret(
            cate1="funread", cate2="source", cate3="merge", cate4="api_key"
        )
        self.model = model or read_secret(
            cate1="funread", cate2="source", cate3="merge", cate4="model"
        )
        self.timeout = timeout
        self.max_retries = max(1, max_retries)
        self.retry_sleep_seconds = max(0, retry_sleep_seconds)

    @staticmethod
    def _extract_json_object(content: str) -> Dict[str, Any]:
        text = content.strip()
        if text.startswith("```"):
            match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
            if match:
                text = match.group(1)
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end < start:
            raise ValueError("LLM response does not contain a JSON object")
        return json.loads(text[start : end + 1])

    @staticmethod
    def _build_prompt(source_type: str, hostname: str, versions: List[Dict[str, Any]]) -> str:
        versions_json = json.dumps(versions, ensure_ascii=False, separators=(",", ":"))
        return (
            "你是一个阅读源合并器。"
            "请把同一个站点的多个版本阅读源合并成一个最优版本。"
            "输出必须是合并后的单个源对象本身，必须是紧凑 JSON。"
            "不要输出解释，不要输出 markdown，不要输出数组，不要输出外层包装对象。"
            "请直接给出最终答案，不要解释、不要分步骤、不要用‘思考’标签。"
            "要求：\n"
            "1. 保留同一含义下信息更完整、更具体的字段。\n"
            "2. 不要引入原始数据中不存在的新站点 URL。\n"
            "3. 保留能工作的规则，删除明显为空、重复或冲突的内容。\n"
            "4. 返回结果必须仍然属于同一 hostname。\n"
            "5. 只返回最终合并后的源对象字段，不要返回 bookSources、rssSources、source、data 等包装层。\n"
            f"6. source_type={source_type}, hostname={hostname}\n"
            f"原始版本如下：\n{versions_json}"
        )

    def _post_and_collect_content(self, payload: Dict[str, Any]) -> str:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self.base_url}/chat/completions"
        request_started_at = time.time()
        logger.info(
            "Start LLM merge request: "
            f"model={self.model}, versions_payload_chars={len(json.dumps(payload, ensure_ascii=False))}"
        )
        with requests.post(url, headers=headers, json=payload, timeout=self.timeout) as response:
            logger.info(
                "LLM merge response headers received: "
                f"status={response.status_code}, elapsed={time.time() - request_started_at:.2f}s"
            )
            response.raise_for_status()
            try:
                result = response.json()
            except Exception:
                text = response.text
                logger.info(
                    "LLM merge request finished with raw text: "
                    f"elapsed={time.time() - request_started_at:.2f}s, content_chars={len(text)}"
                )
                return text
            choices = result.get("choices", [])
            if choices and isinstance(choices[0], dict):
                message = choices[0].get("message", {})
                if isinstance(message, dict):
                    content = message.get("content")
                    if isinstance(content, str):
                        logger.info(
                            "LLM merge request finished: "
                            f"elapsed={time.time() - request_started_at:.2f}s, content_chars={len(content)}"
                        )
                        return content
            text = response.text
            logger.info(
                "LLM merge request finished with raw text: "
                f"elapsed={time.time() - request_started_at:.2f}s, content_chars={len(text)}"
            )
            return text

    def merge_sources(
        self,
        source_type: str,
        hostname: str,
        versions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not self.api_key:
            raise ValueError("LLM merge api key is not configured")
        payload = {
            "model": self.model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": "你只返回一个合法、紧凑、无包装层的 JSON 对象。"},
                {
                    "role": "user",
                    "content": self._build_prompt(source_type, hostname, versions),
                },
            ],
            "response_format": {"type": "json_object"},
        }
        last_error: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                try:
                    content = self._post_and_collect_content(payload=payload)
                except requests.HTTPError as error:
                    if error.response is None or error.response.status_code != 400:
                        raise
                    payload_without_response_format = dict(payload)
                    payload_without_response_format.pop("response_format", None)
                    logger.warning(
                        "Retry LLM merge request without response_format: "
                        f"attempt={attempt}/{self.max_retries}, hostname={hostname}"
                    )
                    content = self._post_and_collect_content(
                        payload=payload_without_response_format
                    )
                if not isinstance(content, str):
                    raise ValueError("LLM response content is not text")
                merged_source = self._extract_json_object(content)
                logger.info(
                    "LLM merge request parsed successfully: "
                    f"attempt={attempt}/{self.max_retries}, hostname={hostname}"
                )
                return merged_source
            except (requests.RequestException, ValueError, json.JSONDecodeError) as error:
                last_error = error
                logger.warning(
                    "LLM merge request failed: "
                    f"attempt={attempt}/{self.max_retries}, hostname={hostname}, error={error}"
                )
                if attempt >= self.max_retries:
                    break
                if self.retry_sleep_seconds > 0:
                    time.sleep(self.retry_sleep_seconds)
        raise ValueError(
            f"LLM merge failed after {self.max_retries} attempts for hostname={hostname}: {last_error}"
        )


class SourceMergeRunner:
    """Walk local source files and merge multiple versions back into each file."""

    def __init__(
        self,
        store: SourceProcessor,
        merger: Optional[SourceMerger] = None,
        min_versions: int = 2,
        max_versions_per_merge: int = DEFAULT_MAX_VERSIONS_PER_MERGE,
        max_prompt_chars: int = DEFAULT_MAX_PROMPT_CHARS,
    ):
        self.store = store
        self.merger = merger or OpenAICompatibleSourceMerger()
        self.min_versions = min_versions
        self.max_versions_per_merge = max_versions_per_merge
        self.max_prompt_chars = max_prompt_chars

    def run(self, limit: Optional[int] = None) -> Dict[str, int]:
        stats = {"processed": 0, "merged": 0, "skipped": 0, "failed": 0}
        for file_path in self.iter_source_files():
            if limit is not None and stats["processed"] >= limit:
                break
            stats["processed"] += 1
            logger.info(f"Start merge source file: {file_path}")
            status = self.merge_file(file_path)
            stats[status] += 1
        return stats

    def iter_source_files(self) -> List[str]:
        file_list: List[tuple[int, str]] = []
        if not os.path.exists(self.store.path_bok):
            return []
        for root, _, files in os.walk(self.store.path_bok):
            for name in files:
                if name.endswith(".json"):
                    file_path = os.path.join(root, name)
                    file_list.append((self._read_version_count(file_path), file_path))
        file_list.sort(key=lambda item: (item[0], item[1]))
        return [file_path for _, file_path in file_list]

    def merge_file(self, file_path: str) -> str:
        try:
            data = self.store._load_json_safely(file_path)
            version_items = self._collect_version_items(data)
            if len(version_items) < self.min_versions:
                logger.info(
                    f"Skip merge source file: file={file_path}, versions={len(version_items)}, "
                    f"min_versions={self.min_versions}"
                )
                return "skipped"
            hostname = str(data.get("hostname") or "")
            logger.info(
                "Prepare merge source file: "
                f"file={file_path}, hostname={hostname}, versions={len(version_items)}"
            )
            merged_item = self._merge_version_items_progressively(
                file_path=file_path,
                data=data,
                hostname=hostname,
                version_items=version_items,
            )
            data["merged"] = [merged_item]
            data["candidate"] = []
            self.store._save_json_safely(file_path, data)
            logger.info(
                "Merged source successfully: "
                f"file={file_path}, hostname={hostname}, versions={len(version_items)}"
            )
            return "merged"
        except Exception as e:
            logger.warning(f"Failed to merge source file {file_path}: {e}")
            return "failed"

    def _collect_version_items(self, data: Dict[str, Any]) -> List[VersionItem]:
        version_items: List[VersionItem] = []
        for key in ("merged", "candidate"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                source = item.get("source")
                if isinstance(source, dict):
                    md5_list = item.get("md5_list", [])
                    version_items.append(
                        {
                            "md5_list": [value for value in md5_list if isinstance(value, str)],
                            "source": copy.deepcopy(source),
                        }
                    )
        return version_items

    def _collect_versions(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [item["source"] for item in self._collect_version_items(data)]

    def _read_version_count(self, file_path: str) -> int:
        try:
            data = self.store._load_json_safely(file_path)
        except Exception:
            return 0
        return len(self._collect_version_items(data))

    def _estimate_version_items_size(self, version_items: List[VersionItem]) -> int:
        return len(json.dumps([item["source"] for item in version_items], ensure_ascii=False))

    def _split_version_items(self, version_items: List[VersionItem]) -> List[List[VersionItem]]:
        chunks: List[List[VersionItem]] = []
        current: List[VersionItem] = []
        current_size = 0
        for version_item in version_items:
            version_size = len(json.dumps(version_item["source"], ensure_ascii=False))
            exceeds_count = len(current) >= self.max_versions_per_merge
            exceeds_size = current and current_size + version_size > self.max_prompt_chars
            if exceeds_count or exceeds_size:
                chunks.append(current)
                current = []
                current_size = 0
            current.append(copy.deepcopy(version_item))
            current_size += version_size
        if current:
            chunks.append(current)
        if len(chunks) == len(version_items) and len(version_items) > 1:
            return self._group_version_items_by_count(version_items)
        return chunks

    def _group_version_items_by_count(
        self, version_items: List[VersionItem]
    ) -> List[List[VersionItem]]:
        group_size = max(2, self.max_versions_per_merge)
        grouped_chunks: List[List[VersionItem]] = []
        for start in range(0, len(version_items), group_size):
            grouped_chunks.append(copy.deepcopy(version_items[start : start + group_size]))
        return grouped_chunks

    def _merge_version_items_progressively(
        self,
        file_path: str,
        data: Dict[str, Any],
        hostname: str,
        version_items: List[VersionItem],
    ) -> VersionItem:
        if len(version_items) < self.min_versions:
            raise ValueError("Not enough versions to merge")

        if (
            len(version_items) <= self.max_versions_per_merge
            and self._estimate_version_items_size(version_items) <= self.max_prompt_chars
        ):
            logger.info(
                "Merge source chunk directly: "
                f"hostname={hostname}, versions={len(version_items)}, "
                f"chars={self._estimate_version_items_size(version_items)}"
            )
            merged_source = self.merger.merge_sources(
                source_type=self.store.cate1,
                hostname=hostname,
                versions=[item["source"] for item in version_items],
            )
            validated_source = self._validate_merged_source(
                source=merged_source,
                expected_hostname=hostname,
            )
            return {
                "md5_list": self._build_merged_md5_list_from_items(version_items, validated_source),
                "source": validated_source,
            }

        chunks = self._split_version_items(version_items)
        logger.info(
            "Split merge source into chunks: "
            f"hostname={hostname}, versions={len(version_items)}, chunks={len(chunks)}"
        )
        merged_chunks: List[VersionItem] = []
        for index, chunk in enumerate(chunks, start=1):
            if len(chunk) == 1:
                logger.info(
                    "Skip LLM merge for single-version chunk: "
                    f"hostname={hostname}, chunk={index}/{len(chunks)}"
                )
                merged_chunks.append(copy.deepcopy(chunk[0]))
                self._save_merge_checkpoint(
                    file_path=file_path,
                    data=data,
                    processed_items=merged_chunks,
                    remaining_chunks=chunks[index:],
                )
                continue
            logger.info(
                "Merge source chunk: "
                f"hostname={hostname}, chunk={index}/{len(chunks)}, "
                f"versions={len(chunk)}, chars={self._estimate_version_items_size(chunk)}"
            )
            merged_source = self.merger.merge_sources(
                source_type=self.store.cate1,
                hostname=hostname,
                versions=[item["source"] for item in chunk],
            )
            validated_source = self._validate_merged_source(
                source=merged_source,
                expected_hostname=hostname,
            )
            merged_chunks.append(
                {
                    "md5_list": self._build_merged_md5_list_from_items(chunk, validated_source),
                    "source": validated_source,
                }
            )
            self._save_merge_checkpoint(
                file_path=file_path,
                data=data,
                processed_items=merged_chunks,
                remaining_chunks=chunks[index:],
            )

        if len(merged_chunks) == 1:
            return merged_chunks[0]
        return self._merge_version_items_progressively(
            file_path=file_path,
            data=data,
            hostname=hostname,
            version_items=merged_chunks,
        )

    def _save_merge_checkpoint(
        self,
        file_path: str,
        data: Dict[str, Any],
        processed_items: List[VersionItem],
        remaining_chunks: List[List[VersionItem]],
    ) -> None:
        remaining_items: List[VersionItem] = []
        for chunk in remaining_chunks:
            remaining_items.extend(copy.deepcopy(chunk))
        checkpoint_data = copy.deepcopy(data)
        checkpoint_data["merged"] = []
        checkpoint_data["candidate"] = copy.deepcopy(processed_items) + remaining_items
        self.store._save_json_safely(file_path, checkpoint_data)

    def _build_merged_md5_list_from_items(
        self, version_items: List[VersionItem], merged_source: Dict[str, Any]
    ) -> List[str]:
        merged_md5 = self._compute_md5(merged_source)
        seen = {merged_md5}
        md5_list = [merged_md5]
        for item in version_items:
            for value in item.get("md5_list", []):
                if isinstance(value, str) and value not in seen:
                    seen.add(value)
                    md5_list.append(value)
        return md5_list

    def _build_merged_md5_list(
        self, data: Dict[str, Any], merged_source: Dict[str, Any]
    ) -> List[str]:
        merged_md5 = self._compute_md5(merged_source)
        seen = {merged_md5}
        md5_list = [merged_md5]
        for key in ("merged", "candidate"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                for value in item.get("md5_list", []):
                    if isinstance(value, str) and value not in seen:
                        seen.add(value)
                        md5_list.append(value)
        return md5_list

    @staticmethod
    def _compute_md5(source: Dict[str, Any]) -> str:
        from funsecret import get_md5_str

        return get_md5_str(json.dumps(source, sort_keys=True, ensure_ascii=False))

    def _validate_merged_source(
        self, source: Dict[str, Any], expected_hostname: str
    ) -> Dict[str, Any]:
        if not isinstance(source, dict):
            raise ValueError("Merged source must be a dict")
        normalized = self.store.source_format(copy.deepcopy(source))
        source_url_key = self.store.get_source_url_key()
        source_url = normalized.get(source_url_key)
        if not isinstance(source_url, str) or not source_url:
            raise ValueError(f"Merged source missing required field: {source_url_key}")
        actual_hostname = url_to_hostname(source_url)
        if actual_hostname is None:
            raise ValueError(f"Invalid merged source url: {source_url}")
        if expected_hostname and actual_hostname != expected_hostname:
            raise ValueError(
                f"Merged source hostname changed from {expected_hostname} to {actual_hostname}"
            )
        json.dumps(normalized, ensure_ascii=False)
        return normalized


class MergeSourceTask(Task):
    """Run source merge for local source files."""

    def __init__(self, path: Optional[str] = None, *args, **kwargs):
        self.path = path or self._read_cache_root()
        super(MergeSourceTask, self).__init__(*args, **kwargs)

    @staticmethod
    def _read_cache_root() -> str:
        return read_secret(cate1="funread", cate2="cache", cate3="path", cate4="root")

    @staticmethod
    def _create_store(path: str, source_type: str) -> SourceProcessor:
        if source_type == "book":
            return BookSourceProcessor(path=path, cate1="book")
        if source_type == "rss":
            return RSSSourceProcessor(path=path, cate1="rss")
        raise ValueError(f"Unsupported source type: {source_type}")

    def run_source(
        self,
        source_type: str,
        merger: Optional[SourceMerger] = None,
        limit: Optional[int] = None,
    ) -> Dict[str, int]:
        with self._create_store(self.path, source_type=source_type) as store:
            runner = SourceMergeRunner(store=store, merger=merger)
            return runner.run(limit=limit)

    def run_book(self, merger: Optional[SourceMerger] = None, limit: Optional[int] = None):
        return self.run_source(source_type="book", merger=merger, limit=limit)

    def run_rss(self, merger: Optional[SourceMerger] = None, limit: Optional[int] = None):
        return self.run_source(source_type="rss", merger=merger, limit=limit)
