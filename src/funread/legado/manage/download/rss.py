import re
import traceback

import requests
from funfake.headers import Headers
from funutil import getLogger
from funutil.cache import disk_cache
from tqdm import tqdm

from funread.legado.manage.download.base import DownloadSource

logger = getLogger("funread")

faker = Headers()


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

        for key in ["customOrder", "respondTime", "lastUpdateTime"]:
            if key in self.source.keys():
                self.source.pop(key)

        for key in ["searchUrl", "exploreUrl"]:
            if key in self.source.keys():
                self.source[key] = self.source[key].replace(self.source["bookSourceUrl"], "")
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

    def loader(self):
        urls = [
            "https://agit.ai/butterfly/yd/raw/branch/yd/迷迭订阅源.json",
        ]
        urls.extend([f"https://www.yckceo.com/yuedu/rsss/json/id/{_id}.json" for _id in range(0, 50)])
        urls.extend([f"https://www.yckceo.com/yuedu/rss/json/id/{_id}.json" for _id in range(0, 500)])

        cache_path = f"{self.path_rot}/../cache"
        logger.info(f"cache_path:{cache_path}")

        @disk_cache(cache_key=cache_path, expire=3600 * 24)
        def load_data(url: str) -> dict:
            try:
                return requests.get(url, headers=faker.generate()).json()
            except Exception as e:
                logger.error(f"error: {e},traceback: {traceback.format_exc()}")
                return {}

        for _url in tqdm(urls, total=len(urls)):
            self.add_sources(load_data(_url))
