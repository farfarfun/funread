"""RSS 源下载和管理模块"""

import traceback
from typing import Any, Dict

import requests
from funfake.headers import Headers
from funutil import getLogger
from funutil.cache import disk_cache
from tqdm import tqdm

from funread.legado.manage.download.base import DownloadSource
from funread.legado.manage.utils import retain_zh_ch_dig

logger = getLogger("funread")
faker = Headers()


class RSSSourceFormat:
    """RSS 源格式化类，用于统一 RSS 源格式"""

    def __init__(self, source: Dict[str, Any]):
        """
        初始化 RSS 源格式化器

        Args:
            source: 原始 RSS 源字典
        """
        self.source = source
        self.source["sourceComment"] = ""
        self.source["sourceUrl"] = self.source["sourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")

        for key in ["sourceGroup", "sourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self) -> Dict[str, Any]:
        """
        执行完整的格式化流程

        Returns:
            格式化后的 RSS 源字典
        """
        # 移除空值字段
        keys_to_remove = [key for key in self.source.keys() if not self.source[key]]
        for key in keys_to_remove:
            self.source.pop(key)

        # 移除不需要的字段
        for key in ["customOrder", "respondTime", "lastUpdateTime"]:
            self.source.pop(key, None)

        # 处理相对 URL（注意：RSS 源可能使用 sourceUrl 而不是 bookSourceUrl）
        base_url = self.source.get("bookSourceUrl") or self.source.get("sourceUrl", "")
        for key in ["searchUrl", "exploreUrl"]:
            if key in self.source and base_url:
                self.source[key] = self.source[key].replace(base_url, "")
        return self.source

    def __format_base(self, group, map):
        book_info = self.source.get(group, {})

        for key, name in map.items():
            if key not in self.source:
                continue
            value = self.source.pop(key)
            if value:
                book_info[name] = value
        if len(book_info) > 0:
            self.source[group] = book_info


class RSSSourceDownload(DownloadSource):
    """RSS 源下载器，继承自 DownloadSource"""

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化 RSS 源

        Args:
            source: 原始 RSS 源字典

        Returns:
            格式化后的 RSS 源字典
        """
        return RSSSourceFormat(source).run()

    def loader(self):
        urls = [
            "https://agit.ai/butterfly/yd/raw/branch/yd/迷迭订阅源.json",
        ]
        urls.extend(
            [f"https://www.yckceo.com/yuedu/rsss/json/id/{_id}.json" for _id in range(0, 50)]
        )
        urls.extend(
            [f"https://www.yckceo.com/yuedu/rss/json/id/{_id}.json" for _id in range(0, 500)]
        )

        cache_path = f"{self.path_rot}/../cache"
        logger.info(f"cache_path:{cache_path}")

        @disk_cache(cache_key="url", cache_dir=cache_path, expire=3600 * 24)
        def load_data(url: str) -> dict:
            try:
                return requests.get(url, headers=faker.generate()).json()
            except Exception as e:
                # logger.error(f"error: {e},traceback: {traceback.format_exc()}")
                return {}

        for _url in tqdm(urls):
            try:
                self.add_sources(load_data(_url))
            except Exception as e:
                logger.info(f"error:{e}")
