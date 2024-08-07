import re

from funread.legado.manage.download.base import DownloadSource


def retain_zh_ch_dig(text):
    return re.sub("[^\u4e00-\u9fa5a-zA-Z0-9\[\]]+", "", text)


class RSSSourceFormat:
    def __init__(self, source):
        self.source = source
        self.source["sourceComment"] = ""
        self.source["sourceUrl"] = self.source["sourceUrl"].rstrip("/|#")
        if "httpUserAgent" in self.source.keys():
            self.source["header"] = self.source.pop("httpUserAgent")

        for key in ["sourceGroup", "sourceName"]:
            self.source[key] = retain_zh_ch_dig(self.source.get(key, ""))

    def run(self):
        keys = [key for key in self.source.keys() if not self.source[key]]
        for key in keys:
            
            self.source.pop(key)

        for key in ['customOrder', 'respondTime', 'lastUpdateTime']:
            if key in self.source.keys():
                self.source.pop(key)

        for key in ['searchUrl', 'exploreUrl']:
            if key in self.source.keys():
                self.source[key] = self.source[key].replace(self.source['bookSourceUrl'], '')
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def source_format(self, source):
        return RSSSourceFormat(source).run()

    
