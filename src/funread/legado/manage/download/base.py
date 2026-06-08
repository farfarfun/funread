"""基础下载类模块"""

import json
import os
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Union

import requests
from nltfile import funos, pickle
from nltfile.compress import tarfile
from nltlog import getLogger
from funsecret import get_md5_str
from tqdm import tqdm

from funread.legado.manage.utils import url_to_hostname

logger = getLogger("funread")

# 常量定义
DEFAULT_BACKUP_HOST = "https://farfarfun.github.com"
DEFAULT_BACKUP_ID = 10000000
REQUEST_TIMEOUT = 30
MAX_PICKLE_SIZE = 1024 * 1024 * 100  # 100MB max pickle file size


class DownloadSource:
    """
    基础下载类，提供源的下载、管理和存储功能

    主要功能：
    - 源的添加和管理
    - MD5 去重
    - URL 索引管理
    - 数据持久化（JSON）
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
        # 使用Path进行安全的路径操作
        base_path = Path(path) / cate1
        self.path_rot = str(base_path)
        self.path_bak = str(base_path / "bak")
        self.path_pkl = str(base_path / "pkl")
        self.path_bok = str(base_path / "source")
        self.pkl_url = str(Path(self.path_pkl) / "url_info.json")
        self.database_url = kwargs.get("database_url")
        self.pkl_md5 = str(Path(self.path_pkl) / "source_info.json")

        self.url_map: Dict[str, int] = {}
        self.md5_set: Dict[str, Dict[str, Any]] = {}
        self.current_id = 1

        # 创建必要目录
        self._ensure_directories()

    def _ensure_directories(self) -> None:
        """确保所有必要的目录存在"""
        for path in [self.path_bak, self.path_bok, self.path_pkl]:
            funos.makedirs(path)

    @staticmethod
    def _validate_path(path: str, base_path: str) -> bool:
        """验证路径安全性，防止路径遍历攻击"""
        try:
            real_path = Path(path).resolve()
            real_base = Path(base_path).resolve()
            return str(real_path).startswith(str(real_base))
        except (OSError, ValueError):
            return False

    @staticmethod
    def _load_json_safely(file_path: str) -> Dict[str, Any]:
        """安全加载JSON文件"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in {file_path}: {e}")
            raise
        except IOError as e:
            logger.error(f"Failed to read {file_path}: {e}")
            raise

    @staticmethod
    def _save_json_safely(file_path: str, data: Dict[str, Any]) -> None:
        """安全保存JSON文件"""
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, sort_keys=True, indent=2, ensure_ascii=False)
        except IOError as e:
            logger.error(f"Failed to write {file_path}: {e}")
            raise

    @staticmethod
    def _extract_persisted_items(data: Any) -> Any:
        """兼容历史与当前持久化格式，提取真实数据载荷"""
        if isinstance(data, dict) and "data" in data:
            return data["data"]
        return data

    @staticmethod
    def _coerce_int(value: Any) -> Optional[int]:
        """将持久化值尽量转换为 int，不可转换时返回 None"""
        if isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return int(value)
            except ValueError:
                return None
        if isinstance(value, (list, tuple)) and len(value) == 1:
            return DownloadSource._coerce_int(value[0])
        return None

    @classmethod
    def _normalize_url_map(cls, data: Any) -> Dict[str, int]:
        """规范化 URL 映射，兼容历史格式并过滤脏数据"""
        normalized: Dict[str, int] = {}

        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and isinstance(data.get("url"), list):
            url_ids = data.get("url_id", [])
            if not isinstance(url_ids, list):
                raise ValueError("Invalid URL map data: url_id must be a list")
            items = [{"url": url, "url_id": url_id} for url, url_id in zip(data["url"], url_ids)]
        elif isinstance(data, dict):
            items = [{"url": url, "url_id": url_id} for url, url_id in data.items()]
        else:
            raise ValueError(f"Unsupported URL map data type: {type(data).__name__}")

        for item in items:
            if not isinstance(item, dict):
                logger.warning(f"Skipping invalid URL map item: {item!r}")
                continue
            url = item.get("url")
            url_id = cls._coerce_int(item.get("url_id"))
            if not isinstance(url, str) or not url:
                logger.warning(f"Skipping URL map item with invalid url: {item!r}")
                continue
            if url_id is None:
                logger.warning(f"Skipping URL map item with invalid url_id: {item!r}")
                continue
            normalized[url] = url_id

        return normalized

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

    def get_source_url_key(self) -> str:
        """返回当前源类型使用的 URL 字段名"""
        return "sourceUrl"

    def persist_download_record(self, url: str, source_data: Any) -> None:
        """将下载 URL 与源数量写入数据库，数据库未配置时静默跳过"""
        try:
            from funread.legado.manage import upsert_source_download_record

            if isinstance(source_data, list):
                source_count = len(source_data)
            elif isinstance(source_data, dict):
                if isinstance(source_data.get("list"), list):
                    source_count = len(source_data["list"])
                elif isinstance(source_data.get("data"), list):
                    source_count = len(source_data["data"])
                elif "error" in source_data:
                    source_count = 0
                else:
                    source_count = 1
            else:
                source_count = 0

            upsert_source_download_record(
                download_url=url,
                source_type=self.cate1,
                source_count=source_count,
            )
        except ValueError:
            return
        except Exception as e:
            logger.warning(f"Failed to persist download record for {url}: {e}")

    def url_index(self, url: str) -> int:
        """获取或创建 URL 的索引 ID。"""
        if url in self.url_map:
            return self.url_map[url]

        from funread.legado.manage import add_source_url

        record = add_source_url(url=url, database_url=self.database_url)
        self.url_map[url] = record.id
        self.current_id = max(self.current_id, record.id)
        return record.id

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
                data = DownloadSource._load_json_safely(fpath)
            except (json.JSONDecodeError, IOError):
                data = DownloadSource._create_default_data(url_info)
        else:
            data = DownloadSource._create_default_data(url_info)

        # 检查是否已是最终版本或不可用
        if data.get("final", False) or not data.get("available", True):
            return

        # 收集所有已存在的 MD5 值
        existing_md5s = DownloadSource._collect_existing_md5s(data)

        # 如果 MD5 不存在，添加到候选列表
        if md5 not in existing_md5s:
            data["candidate"].append({"md5_list": [md5], "source": source})
            DownloadSource._save_json_safely(fpath, data)

    @staticmethod
    def _create_default_data(url_info: Dict[str, Any]) -> Dict[str, Any]:
        """创建默认的数据结构"""
        return {
            "available": True,
            "merged": [],
            "candidate": [],
            "final": False,
            "url_id": url_info.get("url_id", 0),
            "hostname": url_info.get("hostname", ""),
        }

    @staticmethod
    def _collect_existing_md5s(data: Dict[str, Any]) -> List[str]:
        """从数据中收集所有已存在的 MD5 值"""
        md5_list = []
        for key in ("merged", "candidate"):
            if key in data:
                for item in data[key]:
                    if "md5_list" in item:
                        md5_list.extend(item["md5_list"])
        return md5_list

    def add_source(self, source: Dict[str, Any], *args, **kwargs) -> bool:
        """
        添加单个源

        Args:
            source: 源数据字典

        Returns:
            是否成功添加
        """
        source_url_key = self.get_source_url_key()
        if source is None or len(source) == 0 or source_url_key not in source:
            return False
        try:
            # 计算 MD5
            md5 = get_md5_str(json.dumps(source, sort_keys=True))

            # 格式化源
            source = self.source_format(source)

            # 验证必需的字段
            if source_url_key not in source:
                logger.warning(f"Source missing '{source_url_key}' field, skipping")
                return False

            # 获取主机名
            source_url = source[source_url_key]
            hostname = url_to_hostname(source_url)
            if hostname is None:
                logger.warning(f"Failed to parse hostname from URL: {source_url}")
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
                - 文件路径（JSON 文件）
                - JSON 字符串
                - 源字典列表
                - 单个源字典

        Returns:
            成功添加的源数量
        """
        # 解析输入数据
        parsed_data = self._parse_input_data(data)
        if parsed_data is None:
            return 0

        # 确保 data 是列表
        if isinstance(parsed_data, dict):
            parsed_data = [parsed_data]
        elif not isinstance(parsed_data, list):
            logger.error(f"Unsupported data type: {type(parsed_data)}")
            return 0

        # 批量添加
        success_count = sum(1 for source in parsed_data if self.add_source(source, *args, **kwargs))
        return success_count

    def _parse_input_data(self, data: Union[str, Dict, List]) -> Optional[Union[Dict, List]]:
        """解析输入数据，返回结构化数据或None"""
        if isinstance(data, str):
            return self._parse_string_data(data)
        elif isinstance(data, (dict, list)):
            return data
        else:
            logger.error(f"Invalid data type: {type(data)}")
            return None

    def _parse_string_data(self, data: str) -> Optional[Union[Dict, List]]:
        """解析字符串类型的数据"""
        # 尝试作为URL
        if data.startswith(("http://", "https://")):
            return self._fetch_from_url(data)

        # 尝试作为文件路径
        if os.path.exists(data):
            return self._load_from_file(data)

        # 尝试作为JSON字符串
        if data.strip().startswith(("[", "{")):
            try:
                return json.loads(data)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON string: {e}")
                return None

        logger.error(f"Invalid data format: {data[:100]}")
        return None

    def _fetch_from_url(self, url: str) -> Optional[Union[Dict, List]]:
        """从URL获取数据"""
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            self.persist_download_record(url, data)
            return data
        except requests.RequestException as e:
            self.persist_download_record(url, {"error": str(e)})
            logger.error(f"Failed to fetch URL {url}: {e}")
            return None
        except json.JSONDecodeError as e:
            self.persist_download_record(url, {"error": f"Invalid JSON: {e}"})
            logger.error(f"Failed to parse JSON from URL {url}: {e}")
            return None

    def _load_from_file(self, file_path: str) -> Optional[Union[Dict, List]]:
        """从文件加载数据"""
        try:
            if file_path.endswith(".pkl") or file_path.endswith(".pkl.bz2"):
                logger.warning("Pickle format is deprecated, use JSON instead")
                return pickle.load(file_path)
            else:
                return self._load_json_safely(file_path)
        except (IOError, Exception) as e:
            logger.error(f"Failed to load file {file_path}: {e}")
            return None

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
                        source_url_key = self.get_source_url_key()
                        if source_url_key in source and item["md5_list"]:
                            source[source_url_key] = (
                                f"{source[source_url_key]}#{item['md5_list'][0][:10]}"
                            )
                        if "customOrder" in data:
                            source["customOrder"] = data["customOrder"]
                        else:
                            source["customOrder"] = 999999999
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

        # 从数据库加载 URL 映射
        try:
            from funread.legado.manage import load_source_url_map

            self.url_map = load_source_url_map(database_url=self.database_url)
        except ValueError:
            self.url_map = {}
        except Exception as e:
            logger.warning(f"Failed to load URL map from database: {e}")
            self.url_map = {}

        # 更新当前 ID
        self.current_id = max(self.url_map.values()) if self.url_map else DEFAULT_BACKUP_ID - 1

        # 加载 MD5 索引
        if os.path.exists(self.pkl_md5):
            try:
                data = self._extract_persisted_items(self._load_json_safely(self.pkl_md5))
                if isinstance(data, list):
                    self.md5_set = {item["md5"]: item for item in data}
                elif isinstance(data, dict):
                    self.md5_set = data
                else:
                    raise ValueError(f"Unsupported MD5 index data type: {type(data).__name__}")
            except (IOError, json.JSONDecodeError):
                logger.warning("Failed to load MD5 index, starting fresh")
                self.md5_set = {}
            except (KeyError, TypeError, ValueError) as e:
                logger.warning(f"Invalid MD5 index data, starting fresh: {e}")
                self.md5_set = {}
        else:
            self.md5_set = {}

    def dumps(self) -> None:
        """
        保存数据到持久化存储（JSON 格式）
        """
        logger.info("Saving data to persistent storage")
        self._ensure_directories()

        try:
            # URL 映射已持久化到数据库

            # 保存 MD5 索引
            if self.md5_set:
                md5_data = list(self.md5_set.values())
                self._save_json_safely(self.pkl_md5, {"data": md5_data})
        except IOError as e:
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
        except Exception as e:
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
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            raise

    def __enter__(self):
        self.loads()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dumps()
