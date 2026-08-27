"""RSS 更新任务模块"""

from datetime import datetime
from typing import Any, Dict, List, Tuple

import requests
from fundrive.drives.github import GithubDrive
from funfake.headers import Headers
from farlog import getLogger
from funworker import BaseConsumer, BaseProcessor, Pipeline

from ..utils.worker import ListProducer

logger = getLogger("funread")

DEFAULT_ICON_URL = "https://via.placeholder.com/150"
CAT_API_URL = "https://api.thecatapi.com/v1/images/search?size=full"
DEFAULT_TIMEOUT = 10
REQUEST_RETRIES = 2
DEFAULT_REPO = "farfarfun/funread-cache"
DEFAULT_ICON_FETCH_WORKERS = 8

EXTERNAL_SOURCES = [
    {"title": "源仓库(新)", "url": "https://link3.cc/yckceo"},
    {"title": "开源阅读-语雀文档", "url": "https://www.yuque.com/legado"},
    {"title": "喵公子书源", "url": "http://yuedu.miaogongzi.net/gx.html"},
    {"title": "「阅读」APP 源-aoaostar", "url": "https://legado.aoaostar.com/"},
    {"title": "yiove", "url": "https://shuyuan.yiove.com/"},
]


class _BookSourceProcessor(BaseProcessor):
    """Builds one book-source entry (including its icon fetch) for a directory listing."""

    def __init__(self, task: "UpdateRssTask"):
        self.task = task

    def process(self, item: Tuple[int, Dict[str, Any]]) -> Tuple[int, Dict[str, Any]]:
        index, dir_info = item
        source = {
            "title": dir_info["name"],
            "pic": self.task.random_icon(),
            "url": f"https://farfarfun.github.io/{self.task.repo}/{dir_info['path']}/index.html",
            "description": dir_info.get("description", "Legado source"),
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        return index, source


class _OrderedResultsConsumer(BaseConsumer):
    """Collect (index, value) pairs and expose them sorted by original index."""

    def __init__(self, input_queue, **kwargs):
        super().__init__(input_queue=input_queue, **kwargs)
        self._results: Dict[int, Any] = {}

    def consume(self, item: Tuple[int, Any]) -> None:
        index, value = item
        self._results[index] = value

    def ordered_values(self) -> List[Any]:
        return [self._results[i] for i in sorted(self._results)]


class UpdateRssTask:
    """RSS 更新任务，用于更新订阅源列表"""

    def __init__(self, repo: str = DEFAULT_REPO) -> None:
        self.drive = GithubDrive()
        self.drive.login(repo)
        self.repo = repo
        self.faker = Headers()

    def random_icon(self, retries: int = REQUEST_RETRIES) -> str:
        for attempt in range(retries):
            try:
                response = requests.get(
                    CAT_API_URL,
                    headers=self.faker.generate(),
                    timeout=DEFAULT_TIMEOUT,
                )
                response.raise_for_status()
                data = response.json()
                if data and len(data) > 0 and "url" in data[0]:
                    return data[0]["url"]
            except (requests.RequestException, KeyError, IndexError, ValueError) as e:
                if attempt < retries - 1:
                    logger.debug(f"Icon fetch attempt {attempt + 1} failed: {e}, retrying...")
                    continue
                logger.warning(f"Failed to fetch icon after {retries} attempts: {e}")
        return DEFAULT_ICON_URL

    def update_book(self, dir_path: str = "funread/legado/snapshot/lasted") -> None:
        try:
            sources = self._build_book_sources(dir_path)
            sources.extend(self._enrich_external_sources(EXTERNAL_SOURCES))

            data = {
                "name": "funread",
                "next": f"https://gitee.com/{self.repo}/raw/master/{dir_path}/funread.json",
                "list": sources,
            }
            self.drive.upload_file(git_path=f"{dir_path}/source.json", content=data)
            logger.info(f"Successfully updated book sources: {len(sources)} sources")
        except Exception as e:
            logger.error(f"Failed to update book sources: {e}")
            raise

    def _build_book_sources(self, dir_path: str) -> List[Dict[str, Any]]:
        try:
            dir_list = list(self.drive.get_dir_list(dir_path))
        except Exception as e:
            logger.error(f"Failed to build book sources: {e}")
            return []
        if not dir_list:
            return []

        pipeline = Pipeline.build(
            producer_cls=ListProducer,
            processor=_BookSourceProcessor(task=self),
            consumer_cls=_OrderedResultsConsumer,
            num_workers=min(DEFAULT_ICON_FETCH_WORKERS, len(dir_list)),
            producer_kwargs={"items": list(enumerate(dir_list))},
            processor_name="rss-book-sources",
        )
        pipeline.run()
        pipeline.log_progress()
        return pipeline.consumer.ordered_values()

    def _enrich_external_sources(self, sources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        for source in sources:
            if "pic" not in source:
                source["pic"] = self.random_icon()
            if "time" not in source:
                source["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return sources

    def update_main(self) -> None:
        try:
            rss_urls = self._build_rss_urls()
            data = self._build_main_rss_config(rss_urls)
            self.drive.upload_file(
                git_path="funread/legado/snapshot/lasted/funread.json", content=data
            )
            logger.info("Successfully updated main RSS configuration")
        except Exception as e:
            logger.error(f"Failed to update main RSS: {e}")
            raise

    def _build_rss_urls(self) -> Dict[str, str]:
        return {
            "源": f"https://gitee.com/{self.repo}/raw/master/funread/legado/snapshot/lasted/source.json",
            "源(备)": f"https://github.com/{self.repo}/raw/master/funread/legado/snapshot/lasted/source.json",
        }

    def _build_main_rss_config(self, rss_urls: Dict[str, str]) -> List[Dict[str, Any]]:
        return [
            {
                "lastUpdateTime": int(datetime.now().timestamp()),
                "sourceName": "funread",
                "sourceIcon": self.random_icon(),
                "sourceUrl": "https://github.com/farfarfun",
                "loadWithBaseUrl": False,
                "singleUrl": False,
                "sortUrl": "\n".join([f"{k}::{v}" for k, v in rss_urls.items()]),
                "ruleArticles": "$.list[*]",
                "ruleNextArticles": "$.next",
                "ruleTitle": "$.title",
                "rulePubDate": "⏰{{$.time}}",
                "ruleImage": "$.pic",
                "ruleLink": "$.url",
                "sourceGroup": "VIP",
                "customOrder": -9999999,
                "enabled": True,
            }
        ]

    def run(self) -> None:
        logger.info("Starting RSS update task")
        try:
            self.update_book()
            self.update_main()
            logger.info("RSS update task completed successfully")
        except Exception as e:
            logger.error(f"RSS update task failed: {e}")
            raise
