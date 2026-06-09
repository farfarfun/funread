"""RSS source processor."""

from typing import Any, Dict

from funread.legado.manage.source.storage import iter_source_list_data
from funread.legado.manage.utils import retain_zh_ch_dig

from ..core.processor import SourceProcessor


class RSSSourceFormat:
    """RSS 源格式化类，用于统一 RSS 源格式"""

    def __init__(self, source: Dict[str, Any]):
        self.source = source
        self.source["sourceComment"] = ""
        self.source["sourceUrl"] = self.source["sourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")
        for key in ["sourceGroup", "sourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self) -> Dict[str, Any]:
        keys_to_remove = [key for key in self.source.keys() if not self.source[key]]
        for key in keys_to_remove:
            self.source.pop(key)
        for key in ["customOrder", "respondTime", "lastUpdateTime"]:
            self.source.pop(key, None)
        base_url = self.source.get("bookSourceUrl") or self.source.get("sourceUrl", "")
        for key in ["searchUrl", "exploreUrl"]:
            if key in self.source and base_url:
                self.source[key] = self.source[key].replace(base_url, "")
        return self.source


class RSSSourceProcessor(SourceProcessor):
    """RSS 源处理器。"""

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        return RSSSourceFormat(source).run()

    def loader(self) -> None:
        for _, data in iter_source_list_data(source_type=self.cate1):
            self.add_sources(data)
