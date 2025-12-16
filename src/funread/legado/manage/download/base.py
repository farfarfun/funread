"""基础下载类模块"""

import json
import os
import traceback
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Union

import pandas as pd
import requests
from funfile import funos, pickle
from funfile.compress import tarfile
from funsecret import get_md5_str
from funutil import getLogger
from tqdm import tqdm

from funread.legado.manage.utils import url_to_hostname

logger = getLogger("funread")


class DownloadSource:
    """
    基础下载类，提供源的下载、管理和存储功能

    主要功能：
    - 源的添加和管理
    - MD5 去重
    - URL 索引管理
    - 数据持久化（pickle）
    - 数据备份和恢复（tar.xz）
    """

    def __init__(self, path: str = "./funread-hub", cate1: str = "rss", *args, **kwargs):
        """
        初始化下载器

        Args:
            path: 数据存储根路径
            cate1: 分类名称（如 'book' 或 'rss'）
        """
        self.cate1 = cate1
        self.path_rot = f"{path}/{cate1}"
        self.path_bak = f"{path}/{cate1}/bak"
        self.path_pkl = f"{path}/{cate1}/pkl"
        self.path_bok = f"{path}/{cate1}/source"
        self.pkl_url = f"{self.path_pkl}/url_info.pkl.bz2"
        self.pkl_md5 = f"{self.path_pkl}/source_info.pkl.bz2"

        self.url_map: Dict[str, int] = {}
        self.md5_set: Dict[str, Dict[str, Any]] = {}
        self.current_id = 1

        funos.makedirs(self.path_bak)
        funos.makedirs(self.path_bok)
        funos.makedirs(self.path_pkl)

    def loader(self) -> None:
        """
        加载源数据，需要在子类中实现

        Raises:
            NotImplementedError: 如果子类未实现此方法
        """
        raise NotImplementedError("Subclass must implement loader() method")

    def source_format(self, source: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化源数据，需要在子类中实现

        Args:
            source: 原始源数据字典

        Returns:
            格式化后的源数据字典

        Raises:
            NotImplementedError: 如果子类未实现此方法
        """
        raise NotImplementedError("Subclass must implement source_format() method")

    def url_index(self, url: str) -> int:
        """
        获取或创建 URL 的索引 ID

        Args:
            url: URL 字符串

        Returns:
            URL 对应的索引 ID
        """
        if url in self.url_map:
            return self.url_map[url]
        else:
            self.current_id += 1
            self.url_map[url] = self.current_id
            return self.current_id

    @staticmethod
    def add_source_to_candidate(
        md5: str,
        fpath: str,
        source: Dict[str, Any],
        url_info: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        将源添加到候选列表

        Args:
            md5: 源的 MD5 值
            fpath: 文件路径
            source: 源数据字典
            url_info: URL 信息字典
        """
        url_info = url_info or {}

        # 读取或创建数据文件
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Failed to read {fpath}: {e}")
                data = {
                    "available": True,
                    "merged": [],
                    "candidate": [],
                    "url_id": url_info.get("url_id", 0),
                    "hostname": url_info.get("hostname", ""),
                }
        else:
            data = {
                "available": True,
                "merged": [],
                "candidate": [],
                "url_id": url_info.get("url_id", 0),
                "hostname": url_info.get("hostname", ""),
            }

        # 收集所有已存在的 MD5 值
        md5_list: List[str] = []
        for key in ("merged", "candidate"):
            if key in data:
                for item in data[key]:
                    if "md5_list" in item:
                        md5_list.extend(item["md5_list"])

        # 如果 MD5 不存在，添加到候选列表
        if md5 not in md5_list:
            data["candidate"].append({"md5_list": [md5], "source": source})

            # 确保目录存在
            os.makedirs(os.path.dirname(fpath), exist_ok=True)

            # 写入文件
            try:
                with open(fpath, "w", encoding="utf-8") as fw:
                    json.dump(data, fw, sort_keys=True, indent=4, ensure_ascii=False)
            except IOError as e:
                logger.error(f"Failed to write {fpath}: {e}")
                raise

    def add_source(self, source: Dict[str, Any], *args, **kwargs) -> bool:
        """
        添加单个源

        Args:
            source: 源数据字典

        Returns:
            是否成功添加
        """
        if source is None or len(source) == 0 or "sourceUrl" not in source:
            return False
        try:
            # 计算 MD5
            md5 = get_md5_str(json.dumps(source, sort_keys=True))

            # 格式化源
            source = self.source_format(source)

            # 验证必需的字段
            if "sourceUrl" not in source:
                logger.warning("Source missing 'sourceUrl' field, skipping")
                return False

            # 获取主机名
            hostname = url_to_hostname(source["sourceUrl"])
            if hostname is None:
                logger.warning(f"Failed to parse hostname from URL: {source['sourceUrl']}")
                return False

            # 获取或创建 URL ID
            url_id = self.url_index(hostname)

            # 计算文件路径
            cate1 = (url_id // 100) * 100
            fdir = f"{self.path_bok}/{cate1}-{cate1 + 100}/"
            funos.makedirs(fdir)
            fpath = f"{fdir}/{url_id}.json"

            # 添加到候选列表
            url_info = {"url_id": url_id, "hostname": hostname, "cate1": cate1}
            self.add_source_to_candidate(md5, fpath, source, url_info=url_info)

            # 更新 MD5 索引
            self.md5_set[md5] = {
                "md5": md5,
                "url_id": url_id,
                "hostname": hostname,
                "cate1": cate1,
            }
            return True
        except Exception as e:
            logger.error(f"Error adding source: {e}, traceback: {traceback.format_exc()}")
            return False

    def add_sources(
        self, data: Union[str, List[Dict[str, Any]], Dict[str, Any]], *args, **kwargs
    ) -> int:
        """
        批量添加源

        Args:
            data: 源数据，可以是：
                - URL 字符串（以 http:// 或 https:// 开头）
                - 文件路径（pickle 或 JSON 文件）
                - JSON 字符串
                - 源字典列表
                - 单个源字典

        Returns:
            成功添加的源数量
        """
        # 解析输入数据
        if isinstance(data, str):
            if data.startswith(("http://", "https://")):
                try:
                    response = requests.get(data, timeout=30)
                    response.raise_for_status()
                    data = response.json()
                except (requests.RequestException, json.JSONDecodeError) as e:
                    logger.error(f"Failed to fetch or parse URL {data}: {e}")
                    return 0
            elif os.path.exists(data):
                try:
                    if data.endswith(".pkl") or data.endswith(".pkl.bz2"):
                        data = pickle.load(data)
                    else:
                        with open(data, "r", encoding="utf-8") as f:
                            data = json.load(f)
                except (IOError, json.JSONDecodeError, Exception) as e:
                    logger.error(f"Failed to load file {data}: {e}")
                    return 0
            elif data.strip().startswith(("[", "{")):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON string: {e}")
                    return 0
            else:
                logger.error(f"Invalid data format: {data[:100]}")
                return 0

        # 确保 data 是列表
        if isinstance(data, dict):
            data = [data]
        elif not isinstance(data, list):
            logger.error(f"Unsupported data type: {type(data)}")
            return 0

        # 批量添加
        success_count = 0
        for source in data:
            if self.add_source(source, *args, **kwargs):
                success_count += 1

        logger.info(f"Successfully added {success_count}/{len(data)} sources")
        return success_count

    def export_sources(self, size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
        """
        导出源数据，按批次返回

        Args:
            size: 每批次的源数量

        Yields:
            源字典列表，每批最多包含 size 个源
        """
        # 收集所有 JSON 文件
        file_list: List[str] = []
        if os.path.exists(self.path_bok):
            for root, dirs, files in os.walk(self.path_bok):
                for file in files:
                    if file.endswith(".json"):
                        file_list.append(os.path.join(root, file))

        dd: List[Dict[str, Any]] = []
        for file_path in tqdm(file_list, desc="Exporting sources"):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if not data.get("available", True):
                    continue

                # 从候选和已合并列表中提取源
                for key in ("merged", "candidate"):
                    if key not in data:
                        continue
                    _data = data[key]
                    if len(_data) == 0:
                        continue
                    if key == "candidate":
                        _data = _data[:3]
                    for item in data[key]:  # 每个文件最多取前3个
                        if "source" not in item or "md5_list" not in item:
                            continue

                        source = item["source"].copy()
                        # 添加 MD5 片段到 URL
                        if "sourceUrl" in source and item["md5_list"]:
                            source["sourceUrl"] = (
                                f"{source['sourceUrl']}#{item['md5_list'][0][:10]}"
                            )

                        dd.append(source)
                        if len(dd) >= size:
                            yield dd
                            dd = []
                    break
            except (IOError, json.JSONDecodeError, KeyError) as e:
                logger.warning(f"Failed to process {file_path}: {e}")
                continue

        # 返回最后一批
        if dd:
            yield dd

    def loads(self) -> None:
        """
        加载持久化的数据（URL 映射和 MD5 索引）
        """
        logger.info("Loading persisted data")

        # 加载 URL 映射
        if os.path.exists(self.pkl_url):
            try:
                df = pd.read_pickle(self.pkl_url, compression="infer")
                if "url" in df.columns and "url_id" in df.columns:
                    self.url_map = {row["url"]: row["url_id"] for _, row in df.iterrows()}
                else:
                    # 兼容旧格式
                    self.url_map = {k: v for k, v in df.values}
            except Exception as e:
                logger.warning(f"Failed to load URL map: {e}, using default")
                self.url_map = {"https://farfarfun.github.com": 100000}
        else:
            self.url_map = {"https://farfarfun.github.com": 100000}

        # 更新当前 ID
        if self.url_map:
            self.current_id = max(self.url_map.values())
        else:
            self.current_id = 1

        # 加载 MD5 索引
        if os.path.exists(self.pkl_md5):
            try:
                df = pd.read_pickle(self.pkl_md5, compression="infer")
                self.md5_set = {info["md5"]: info for info in df.to_dict(orient="records")}
            except Exception as e:
                logger.warning(f"Failed to load MD5 index: {e}, starting fresh")
                self.md5_set = {}
        else:
            self.md5_set = {}

    def dumps(self) -> None:
        """
        保存数据到持久化存储（pickle 格式）
        """
        logger.info("Saving data to persistent storage")
        funos.makedirs(self.path_pkl)
        funos.makedirs(self.path_bok)

        try:
            # 保存 URL 映射
            if self.url_map:
                df = pd.DataFrame([{"url": k, "url_id": v} for k, v in self.url_map.items()])
                df.to_pickle(self.pkl_url, compression="infer")

            # 保存 MD5 索引
            if self.md5_set:
                df = pd.DataFrame(list(self.md5_set.values()))
                df.to_pickle(self.pkl_md5, compression="infer")
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            raise

    def loads_zip(self, zip_file: Optional[str] = None) -> None:
        """
        从压缩备份文件恢复数据

        Args:
            zip_file: 备份文件路径，如果为 None 则使用最新的备份文件
        """
        # 删除现有数据
        if os.path.exists(self.path_pkl):
            funos.delete(self.path_pkl)
        if os.path.exists(self.path_bok):
            funos.delete(self.path_bok)

        # 确定备份文件
        if zip_file is None:
            if not os.path.exists(self.path_bak):
                logger.warning("Backup directory does not exist")
                return
            files = [
                f for f in os.listdir(self.path_bak) if f.endswith((".tar", ".tar.xz", ".tar.gz"))
            ]
            if len(files) == 0:
                logger.warning("No backup files found")
                return
            files.sort()
            zip_file = os.path.join(self.path_bak, files[-1])

        if not os.path.exists(zip_file):
            logger.error(f"Backup file not found: {zip_file}")
            return

        logger.info(f"Loading backup from {zip_file}")
        try:
            with tarfile.open(zip_file, "r:*") as tar:
                tar.extractall(self.path_rot)
            self.loads()
        except (tarfile.TarError, IOError) as e:
            logger.error(f"Failed to extract backup: {e}")
            raise

    def dumps_zip(self) -> str:
        """
        创建数据备份压缩文件

        Returns:
            备份文件路径
        """
        # 先保存数据
        self.dumps()

        # 确保目录存在
        funos.makedirs(self.path_pkl)
        funos.makedirs(self.path_bok)
        funos.makedirs(self.path_bak)

        # 生成备份文件名
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        zip_file = f"{self.path_bak}/{self.cate1}-{timestamp}.tar.xz"

        logger.info(f"Creating backup: {zip_file}")
        try:
            with tarfile.open(zip_file, "w|xz") as tar:
                if os.path.exists(self.path_pkl):
                    tar.add(self.path_pkl, arcname=os.path.basename(self.path_pkl))
                if os.path.exists(self.path_bok):
                    tar.add(self.path_bok, arcname=os.path.basename(self.path_bok))
            logger.info(f"Backup created successfully: {zip_file}")
            return zip_file
        except (tarfile.TarError, IOError) as e:
            logger.error(f"Failed to create backup: {e}")
            raise

    def __enter__(self):
        self.loads()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dumps()
