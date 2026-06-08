"""RSS 源下载和管理模块"""

from typing import Any, Dict

from funread.legado.manage.download.base import DownloadSource
from funread.legado.manage.source_download import iter_source_download_data
from funread.legado.manage.utils import retain_zh_ch_dig


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

    def loader(self) -> None:
        """从 URL 记录迭代器中加载 RSS 源数据"""
        for _, data in iter_source_download_data(source_type=self.cate1):
            self.add_sources(data)
