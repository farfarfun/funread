import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from funread.legado.manage.download import GenerateSourceTask
from funread.legado.manage.publish import UpdateEntrance

from tqdm import tqdm
from funread.legado.manage.source import add_source_list_url


def add_url_data():
    urls = [
        "https://bitbucket.org/xiu2/yuedu/raw/master/shuyuan",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/8274870a1493d7c4e51c41682a8d1e9500457826.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/3fc2c64c5489c491de6284dca2c2dfce7f551bc9.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/71e56d4f1d8f1bff61fdd3582ef7513600a9e108.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/1b8256c78b385543b5e8aa6a0d7693c76f8e60d4.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/4dc410d1d0a674de21c5d869496efd60a7fcba7c.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/edeb9b5490b7028906ad3cd2c2b7404b2e4052b9.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/290e0bb1f148e963941fade280a938df81b374b7.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/346da4b785d3dd5aed990a553e10d03d1ececec4.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/6a2c6bb280c2508b7946a6fbe908e3208254f529.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/c495b2f09c55df7acec91eb34588e78b1add7908.json",
        "https://jihulab.com/aoaostar/legado/-/raw/release/cache/0a189226b495a6b15c57acc06177ee15db8cd33c.json",
    ]

    for url in tqdm(urls):
        add_source_list_url(url=url, source_type="book")

    for uri in ("https://www.yckceo.com",):
        for _id in tqdm(range(7000, 7500), desc=uri):
            add_source_list_url(url=f"{uri}/yuedu/shuyuan/json/id/{_id}.json", source_type="book")

        for _id in tqdm(range(0, 1200), desc=uri):
            add_source_list_url(url=f"{uri}/yuedu/shuyuans/json/id/{_id}.json", source_type="book")

        for _id in tqdm(range(0, 300), desc=uri):
            add_source_list_url(url=f"{uri}/yuedu/rss/json/id/{_id}.json", source_type="rss")
        for _id in tqdm(range(0, 300), desc=uri):
            add_source_list_url(url=f"{uri}/yuedu/rsss/json/id/{_id}.json", source_type="rss")


# add_url_data()

# GenerateSourceTask().run_book(sync=True)
# GenerateSourceTask().run_book(merge=True)
# GenerateSourceTask().run_book(dump=True)

# GenerateSourceTask().run_rss()
# GenerateSourceTask().run_rss(load=True)
GenerateSourceTask().run_rss(merge=True)
GenerateSourceTask().run_rss(dump=True)
# UpdateEntrance().run()
