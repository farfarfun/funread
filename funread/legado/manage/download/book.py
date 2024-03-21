import re

from funread.legado.manage.download.base import DownloadSource


def retain_zh_ch_dig(text):
    return re.sub("[^\u4e00-\u9fa5a-zA-Z0-9\[\]]+", "", text)


class BookSourceFormat:
    def __init__(self, source):

        self.source = source
        self.source["bookSourceComment"] = ""
        self.source["bookSourceUrl"] = self.source["bookSourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")

        for key in ["bookSourceGroup", "bookSourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self):
        self.format_book_info()
        self.format_content()
        self.format_search()
        self.format_explore()
        self.format_toc()
        keys = [key for key in self.source.keys() if not self.source[key]]
        for key in keys:
                self.source.pop(key)

        for key in ['customOrder','respondTime','lastUpdateTime']:
            self.source.pop(key)

        for key in ['searchUrl','exploreUrl']:
            if key in self.source.keys():
                self.source[key]=self.source[key].replace(self.source['bookSourceUrl'],'')
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def source_format(self, source):
        return BookSourceFormat(source).run()
