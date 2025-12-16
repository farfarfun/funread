"""书源下载和管理模块"""

from typing import Any, Dict

from funread.legado.manage.download.base import DownloadSource
from funread.legado.manage.utils import retain_zh_ch_dig


class BookSourceFormat:
    """书源格式化类，用于统一书源规则格式"""

    def __init__(self, source: Dict[str, Any]):
        """
        初始化书源格式化器

        Args:
            source: 原始书源字典
        """
        self.source = source
        self.source["bookSourceComment"] = ""
        self.source["bookSourceUrl"] = self.source["bookSourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")

        for key in ["bookSourceGroup", "bookSourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self) -> Dict[str, Any]:
        """
        执行完整的格式化流程

        Returns:
            格式化后的书源字典
        """
        self.format_book_info()
        self.format_content()
        self.format_search()
        self.format_explore()
        self.format_toc()

        # 移除空值字段
        keys_to_remove = [key for key in self.source.keys() if not self.source[key]]
        for key in keys_to_remove:
            self.source.pop(key)

        # 移除不需要的字段
        for key in ["customOrder", "respondTime", "lastUpdateTime"]:
            self.source.pop(key, None)

        # 处理相对 URL
        base_url = self.source.get("bookSourceUrl", "")
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

    def format_book_info(self):
        map = {
            "ruleBookAuthor": "author",
            "ruleBookContent": "content",
            "ruleBookContentReplace": "contentReplace",
            "ruleBookInfoInit": "init",
            "ruleBookKind": "kind",
            "ruleBookLastChapter": "lastChapter",
            "ruleBookName": "name",
            "ruleBookUrlPattern": "urlPattern",
            "ruleBookWordCount": "wordCount",
        }
        self.__format_base("ruleBookInfo", map)

    def format_content(self):
        map = {
            "ruleContentUrl": "url",
            "ruleContentUrlNext": "urlNext",
            "ruleBookContentReplaceRegex": "replaceRegex",
            "ruleBookContentSourceRegex": "sourceRegex",
            "ruleBookContentWebJs": "webJs",
        }
        self.__format_base("ruleContent", map)

    def format_search(self):
        map = {
            "ruleSearchUrl": "url",
            "ruleSearchName": "name",
            "ruleSearchAuthor": "author",
            "ruleSearchList": "bookList",
            "ruleSearchCoverUrl": "coverUrl",
            "ruleSearchIntroduce": "intro",
            "ruleSearchKind": "kind",
            "ruleSearchLastChapter": "lastChapter",
            "ruleSearchNoteUrl": "noteUrl",
            "ruleSearchWordCount": "wordCount",
        }
        self.__format_base("ruleSearch", map)

    def format_explore(self):
        map = {
            "ruleFindUrl": "url",
            "ruleFindName": "name",
            "ruleFindAuthor": "author",
            "ruleFindList": "bookList",
            "ruleFindCoverUrl": "coverUrl",
            "ruleFindIntroduce": "intro",
            "ruleFindKind": "kind",
            "ruleFindLastChapter": "lastChapter",
            "ruleFindNoteUrl": "noteUrl",
        }
        self.__format_base("ruleExplore", map)

    def format_toc(self):
        map = {
            "ruleChapterList": "chapterList",
            "ruleChapterName": "chapterName",
            "ruleChapterUpdateTime": "updateTime",
            "ruleChapterUrl": "chapterUrl",
            "ruleChapterUrlNext": "nextTocUrl",
        }
        self.__format_base("ruleToc", map)


class BookSourceDownload(DownloadSource):
    """书源下载器，继承自 DownloadSource"""

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化书源

        Args:
            source: 原始书源字典

        Returns:
            格式化后的书源字典
        """
        return BookSourceFormat(source).run()
