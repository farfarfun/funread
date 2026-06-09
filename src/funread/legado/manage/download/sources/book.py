"""Book source processor."""

from typing import Any, Dict

from funread.legado.manage.source.storage import iter_source_list_data
from funread.legado.manage.utils import retain_zh_ch_dig

from ..core.processor import SourceProcessor


class BookSourceFormat:
    """书源格式化类，用于统一书源规则格式"""

    def __init__(self, source: Dict[str, Any]):
        self.source = source
        self.source["bookSourceComment"] = ""
        self.source["bookSourceUrl"] = self.source["bookSourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")

        for key in ["bookSourceGroup", "bookSourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self) -> Dict[str, Any]:
        self.format_book_info()
        self.format_content()
        self.format_search()
        self.format_explore()
        self.format_toc()
        keys_to_remove = [key for key in self.source.keys() if not self.source[key]]
        for key in keys_to_remove:
            self.source.pop(key)
        for key in ["customOrder", "respondTime", "lastUpdateTime"]:
            self.source.pop(key, None)
        base_url = self.source.get("bookSourceUrl", "")
        for key in ["searchUrl", "exploreUrl"]:
            if key in self.source and base_url:
                self.source[key] = self.source[key].replace(base_url, "")
        return self.source

    def __format_base(self, group, mapping):
        book_info = self.source.get(group, {})
        for key, name in mapping.items():
            if key not in self.source:
                continue
            value = self.source.pop(key)
            if value:
                book_info[name] = value
        if len(book_info) > 0:
            self.source[group] = book_info

    def format_book_info(self):
        self.__format_base(
            "ruleBookInfo",
            {
                "ruleBookAuthor": "author",
                "ruleBookContent": "content",
                "ruleBookContentReplace": "contentReplace",
                "ruleBookInfoInit": "init",
                "ruleBookKind": "kind",
                "ruleBookLastChapter": "lastChapter",
                "ruleBookName": "name",
                "ruleBookUrlPattern": "urlPattern",
                "ruleBookWordCount": "wordCount",
            },
        )

    def format_content(self):
        self.__format_base(
            "ruleContent",
            {
                "ruleContentUrl": "url",
                "ruleContentUrlNext": "urlNext",
                "ruleBookContentReplaceRegex": "replaceRegex",
                "ruleBookContentSourceRegex": "sourceRegex",
                "ruleBookContentWebJs": "webJs",
            },
        )

    def format_search(self):
        self.__format_base(
            "ruleSearch",
            {
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
            },
        )

    def format_explore(self):
        self.__format_base(
            "ruleExplore",
            {
                "ruleFindUrl": "url",
                "ruleFindName": "name",
                "ruleFindAuthor": "author",
                "ruleFindList": "bookList",
                "ruleFindCoverUrl": "coverUrl",
                "ruleFindIntroduce": "intro",
                "ruleFindKind": "kind",
                "ruleFindLastChapter": "lastChapter",
                "ruleFindNoteUrl": "noteUrl",
            },
        )

    def format_toc(self):
        self.__format_base(
            "ruleToc",
            {
                "ruleChapterList": "chapterList",
                "ruleChapterName": "chapterName",
                "ruleChapterUpdateTime": "updateTime",
                "ruleChapterUrl": "chapterUrl",
                "ruleChapterUrlNext": "nextTocUrl",
            },
        )


class BookSourceProcessor(SourceProcessor):
    """书源处理器。"""

    def get_source_url_key(self) -> str:
        return "bookSourceUrl"

    def loader(self) -> None:
        for _, data in iter_source_list_data(source_type=self.cate1):
            self.add_sources(data)

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        return BookSourceFormat(source).run()
