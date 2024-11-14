import json
import os
import re

import requests
from funfake.headers import Headers
from funfile import funos
from funsecret import get_md5_str
from funutil import getLogger
from tqdm import tqdm

from funread.legado.manage.download.base import DownloadSource

logger = getLogger("funread")


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

    def rss_download(self):
        faker = Headers()
        urls = [
            "https://jt12.de/SYV2_4/2024/03/04/20/48/54/170955653465e5c33680046.json",
            "https://jt12.de/SYV2/2023/03/17/0/02/42/167898256264133da2869d4.json",
            "https://agit.ai/butterfly/yd/raw/branch/yd/迷迭订阅源.json",
        ]
        size = len(urls)
        for uri in ("https://www.yckceo.com",):
            for _id in tqdm(range(0, 50), desc=uri):
                urls.append(f"{uri}/yuedu/rsss/json/id/{_id}.json")
            for _id in tqdm(range(0, 500), desc=uri):
                urls.append(f"{uri}/yuedu/rss/json/id/{_id}.json")

        cache_path = f"{self.path_rot}/../cache"
        funos.makedirs(cache_path)
        for index, url in tqdm(enumerate(urls), total=len(urls)):
            file = f"{cache_path}/{get_md5_str(url)}.json"
            if index > size and os.path.exists(file):
                continue
            try:
                data = json.dumps(requests.get(url, headers=faker.generate()).json())
                with open(file, "w") as fw:
                    fw.write(data)
            except Exception as e:
                logger.error(f"Failed to download {url}: {e}")
