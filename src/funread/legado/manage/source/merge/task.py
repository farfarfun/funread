"""LLM-driven source merge tasks."""

import copy
import json
import os
import re
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
DEFAULT_LLM_TIMEOUT = 120


class SourceMerger(Protocol):
    def merge_sources(
        self,
        source_type: str,
        hostname: str,
        versions: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Merge multiple versions into one final source object."""


class OpenAICompatibleSourceMerger:
    """Default LLM-based merger using an OpenAI-compatible API."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        model: Optional[str] = None,
        timeout: int = DEFAULT_LLM_TIMEOUT,
    ):
        self.api_key = api_key or read_secret(
            cate1="funread", cate2="cache", cate3="source", cate4="merge_api_key"
        )
        self.base_url = (
            base_url or self._read_optional_secret("merge_base_url") or DEFAULT_LLM_BASE_URL
        ).rstrip("/")
        self.model = model or self._read_optional_secret("merge_model") or DEFAULT_LLM_MODEL
        self.timeout = timeout

    @staticmethod
    def _read_optional_secret(cate4: str) -> Optional[str]:
        try:
            return read_secret(cate1="funread", cate2="cache", cate3="source", cate4=cate4)
        except Exception:
            return None

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
        return (
            "你是一个阅读源合并器。"
            "请把同一个站点的多个版本阅读源合并成一个最优版本。"
            "输出必须是一个 JSON 对象，不要输出解释，不要输出 markdown。"
            "要求：\n"
            "1. 保留同一含义下信息更完整、更具体的字段。\n"
            "2. 不要引入原始数据中不存在的新站点 URL。\n"
            "3. 保留能工作的规则，删除明显为空、重复或冲突的内容。\n"
            "4. 返回结果必须仍然属于同一 hostname。\n"
            f"5. source_type={source_type}, hostname={hostname}\n"
            f"原始版本如下：\n{json.dumps(versions, ensure_ascii=False, indent=2)}"
        )

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
                {"role": "system", "content": "你只返回一个合法 JSON 对象。"},
                {
                    "role": "user",
                    "content": self._build_prompt(source_type, hostname, versions),
                },
            ],
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        response = requests.post(
            f"{self.base_url}/chat/completions",
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code == 400:
            payload.pop("response_format", None)
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=payload,
                timeout=self.timeout,
            )
        response.raise_for_status()
        result = response.json()
        content = result["choices"][0]["message"]["content"]
        if not isinstance(content, str):
            raise ValueError("LLM response content is not text")
        return self._extract_json_object(content)


class SourceMergeRunner:
    """Walk local source files and merge multiple versions back into each file."""

    def __init__(
        self,
        store: SourceProcessor,
        merger: Optional[SourceMerger] = None,
        min_versions: int = 2,
    ):
        self.store = store
        self.merger = merger or OpenAICompatibleSourceMerger()
        self.min_versions = min_versions

    def run(self, limit: Optional[int] = None) -> Dict[str, int]:
        stats = {"processed": 0, "merged": 0, "skipped": 0, "failed": 0}
        for file_path in self.iter_source_files():
            if limit is not None and stats["processed"] >= limit:
                break
            stats["processed"] += 1
            status = self.merge_file(file_path)
            stats[status] += 1
        return stats

    def iter_source_files(self) -> List[str]:
        file_list: List[str] = []
        if not os.path.exists(self.store.path_bok):
            return file_list
        for root, _, files in os.walk(self.store.path_bok):
            for name in files:
                if name.endswith(".json"):
                    file_list.append(os.path.join(root, name))
        file_list.sort()
        return file_list

    def merge_file(self, file_path: str) -> str:
        try:
            data = self.store._load_json_safely(file_path)
            versions = self._collect_versions(data)
            if len(versions) < self.min_versions:
                return "skipped"
            hostname = str(data.get("hostname") or "")
            merged_source = self.merger.merge_sources(
                source_type=self.store.cate1,
                hostname=hostname,
                versions=versions,
            )
            validated_source = self._validate_merged_source(
                source=merged_source,
                expected_hostname=hostname,
            )
            merged_md5_list = self._build_merged_md5_list(data, validated_source)
            data["merged"] = [{"md5_list": merged_md5_list, "source": validated_source}]
            data["candidate"] = []
            self.store._save_json_safely(file_path, data)
            logger.info(f"Merged source file: {file_path}")
            return "merged"
        except Exception as e:
            logger.warning(f"Failed to merge source file {file_path}: {e}")
            return "failed"

    def _collect_versions(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        versions: List[Dict[str, Any]] = []
        for key in ("merged", "candidate"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                source = item.get("source")
                if isinstance(source, dict):
                    versions.append(copy.deepcopy(source))
        return versions

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
