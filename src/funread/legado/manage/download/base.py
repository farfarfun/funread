import json
import os
from datetime import datetime
import traceback
import pandas as pd
import requests
from funfile import pickle, funos
from funfile.compress import tarfile
from funread.legado.manage.utils import url_to_hostname
from funsecret import get_md5_str
from tqdm import tqdm


class DownloadSource(object):
    def __init__(self, path="./funread-hub", cate1="rss", *args, **kwargs):
        self.cate1 = cate1
        self.path_rot = f"{path}/{cate1}"
        self.path_bak = f"{path}/{cate1}/bak"
        self.path_pkl = f"{path}/{cate1}/pkl"
        self.path_bok = f"{path}/{cate1}/source"
        self.pkl_url = f"{self.path_pkl}/url_info.pkl.bz2"
        self.pkl_md5 = f"{self.path_pkl}/source_info.pkl.bz2"

        self.url_map = {}
        self.md5_set = {}
        self.current_id = 1

        funos.makedirs(self.path_bak)
        funos.makedirs(self.path_bok)
        funos.makedirs(self.path_pkl)

    def loads(self):
        print("loads")
        if os.path.exists(self.pkl_url):
            df = pd.read_pickle(self.pkl_url, compression="infer")
            self.url_map = {k: v for k, v in df.values}
        else:
            self.url_map = {"https://farfarfun.github.com": 100000}
        self.current_id = max(self.url_map.values())
        if os.path.exists(self.pkl_md5):
            df = pd.read_pickle(self.pkl_md5, compression="infer")
            self.md5_set = {info["md5"]: info for info in df.to_dict(orient="records")}

    def dumps(self):
        print("dumps")

        df = pd.DataFrame([{"url": k, "url_id": v} for k, v in self.url_map.items()])
        df.to_pickle(self.pkl_url, compression="infer")

        df = pd.DataFrame(self.md5_set.values())
        df.to_pickle(self.pkl_md5, compression="infer")

    def url_index(self, url):
        if url in self.url_map:
            return self.url_map[url]
        else:
            self.current_id += 1
            self.url_map[url] = self.current_id
            return self.current_id

    def source_format(self, source):
        pass

    def add_source(self, source, *args, **kwargs):
        md5 = get_md5_str(json.dumps(source))
        source = self.source_format(source)
        hostname = url_to_hostname(source["sourceUrl"])
        if hostname is None:
            return
        url_id = self.url_index(hostname)

        cate1 = (url_id // 100) * 100
        fdir = f"{self.path_bok}/{cate1}-{cate1 + 100}/"
        if not os.path.exists(fdir):
            os.makedirs(fdir)
        fpath = f"{fdir}/{url_id}.json"

        url_info = {"url_id": url_id, "hostname": hostname, "cate1": cate1}
        self.add_source_to_candidate(md5, fpath, source, url_info=url_info)

        self.md5_set[md5] = {
            "md5": md5,
            "url_id": url_id,
            "hostname": hostname,
            "cate1": cate1,
        }

    def add_sources(self, data, *args, **kwargs):
        """"""
        if isinstance(data, str):
            if data.startswith("http"):
                data = requests.get(data).json()
            elif os.path.exists(data):
                data = pickle.load(data)
            elif data[0] == "[" or data[0] == "{":
                data = json.loads(data)

        for source in tqdm(data):
            try:
                self.add_source(source, *args, **kwargs)
            except Exception as e:
                # traceback.print_exc()
                # print(e)
                pass

    def add_source_to_candidate(self, md5, fpath, source, url_info=None):
        url_info = url_info or {}
        if os.path.exists(fpath):
            data = json.loads(open(fpath, "r").read())
        else:
            data = {
                "available": True,
                "merged": [],
                "candidate": [],
                "url_id": url_info["url_id"],
                "hostname": url_info["hostname"],
            }
        md5_list = []
        [
            [md5_list.extend(md5["md5_list"]) for md5 in data[key]]
            for key in ("merged", "candidate")
            if key in data
        ]
        if md5 not in md5_list:
            data["candidate"].append({"md5_list": [md5], "source": source})

        with open(fpath, "w") as fw:
            fw.write(json.dumps(data, sort_keys=True, indent=4))

    def export_sources(self, size=1000):
        file_list = []
        for root, dirs, files in os.walk(self.path_bok):
            for file in files:
                if file.endswith(".json"):
                    file_list.append(os.path.join(root, file))
        dd = []
        for file in tqdm(file_list):
            data = open(file, "r").read()
            data = json.loads(data)
            if not data.get("available", True):
                continue
            for key in ("candidate", "merged"):
                for item in data[key][:3]:
                    source = item["source"]
                    source["sourceUrl"] = (
                        f"{source['sourceUrl']}#{item['md5_list'][0][:10]}"
                    )
                    dd.append(source)
                    if len(dd) >= size:
                        yield dd
                        dd = []
        yield dd

    def loads_zip(self, zip_file=None):
        funos.delete(self.path_pkl)
        funos.delete(self.path_bok)
        if zip_file is None:
            files = os.listdir(self.path_bak)
            files.sort(key=lambda x: x)
            zip_file = os.path.join(self.path_bak, files[-1])
        print(f"load zip file from {zip_file}")
        with tarfile.open(zip_file, "r") as tar:
            tar.extractall(self.path_rot)
        self.loads()

    def dumps_zip(self):
        self.dumps()
        zip_file = f"{self.path_bak}/{self.cate1}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.tar.xz"
        with tarfile.open(zip_file, "w|xz") as tar:
            tar.add(self.path_pkl, arcname=os.path.basename(self.path_pkl))
            tar.add(self.path_bok, arcname=os.path.basename(self.path_bok))

    def __enter__(self):
        self.loads()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dumps()
