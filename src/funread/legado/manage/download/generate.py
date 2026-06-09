"""HTML生成和源文件管理模块"""

from datetime import datetime
from typing import Any, Dict, List, Optional
import json
from dominate.tags import *
from fundrive.drives.github import GithubDrive
from nltfile import funos
from nltlog import getLogger
from funsecret import read_secret
from nlttask import Task
import traceback
from funread.legado.manage.download.book import BookSourceDownload
from funread.legado.manage.download.rss import RSSSourceDownload

logger = getLogger("funread")

# 常量定义
default_repo_owner = "farfarfun"
default_repo_name = "funread-cache"
DEFAULT_REPO = "farfarfun/funread-cache"
DEFAULT_DIR_PATH = "funread/legado/snapshot/lasted"
EXPORT_BATCH_SIZE = 500
INITIAL_COUNTER = 1000
MIN_UPLOAD_BATCH_SIZE = 20


# 组织仓库列表
ORG_REPOS = [
    ("funsecret", "farfarfun/funsecret"),
    ("fundrive", "farfarfun/fundrive"),
    ("funread", "farfarfun/funread"),
]

# 订阅源列表
RSS_SOURCES = [
    {
        "label": "github",
        "href": "yuedu://rssSource/importonline?src=https://github.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
    {
        "label": "gitee",
        "href": "yuedu://rssSource/importonline?src=https://gitee.com/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
    {
        "label": "gitlink",
        "href": "yuedu://rssSource/importonline?src=https://gitlink.org.cn/farfarfun/funread-cache/raw/master/funread/legado/rss/rss-main.json",
    },
]


class GenerateCommon:
    """通用HTML生成类"""

    def generate_build_repo(self, category: str, repo: str) -> None:
        """生成仓库链接"""
        with li(category + ":"):
            a("github", href=f"https://github.com/{repo}")
            a("gitee", href=f"https://gitee.com/{repo}")
            a("gitlink", href=f"https://gitlink.org.cn/{repo}")

    def generate_org(self) -> None:
        """生成组织信息"""
        build_type_div = div()
        build_type_fond = b()
        build_type_fond += font("farfarfun")
        build_type_div += build_type_fond
        with ul():
            for category, repo in ORG_REPOS:
                self.generate_build_repo(category, repo)

            with li("订阅源:"):
                for source in RSS_SOURCES:
                    a(source["label"], href=source["href"])

    def generate(self) -> None:
        """生成完整的构建信息"""
        br()
        div(b(font("Build Information", color="#0B610B")))
        div(hr(size=2, alignment="center", width="100%"))
        div((b(font("Cause: Started by upstream pipeline job "))))
        self.generate_org()
        img(
            src="https://profile-avatar.csdnimg.cn/2e9aaf5666c044d7aa273e086f9878d0_n1007530194.jpg"
        )
        br()
        p("** This is an automatically generated email by jenkins job. **")
        p("Feel free to connect 1007530194@qq.com if you have any question.")
        p(f"update time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


class GenerateSourceType:
    """源文件生成和上传类"""

    # CSS 样式定义
    style_applied = """
        body {
            font-family: verdana,arial,sans-serif;
            font-size: 11px;
        }
        table.gridtable {
            color: #333333;
            border-width: 1px;
            border-color: #666666;
            border-collapse: collapse;
            font-size: 11px;
        }
        table.gridtable th {
            border-width: 1px;
            padding: 8px;
            border-style: solid;
            border-color: #666666;
            background-color: #DDEBF7;
        }
        table.gridtable td {
            border-width: 1px;
            padding: 8px;
            border-style: solid;
            border-color: #666666;
            background-color: #ffffff;
            text-align: center;
        }
        table.gridtable td.failed {
            color: #ED5F5F;
        }
        table.gridtable td.passrate {
            font-weight: bold;
            color: green;
        }
        li {
            margin-top: 5px;
        }
        div {
            margin-top: 10px;
        }
    """

    def __init__(
        self,
        dir_path: str = "funread/legado/book/snapshot/20231011",
        source_type: str = "booksource",
        repo: str = DEFAULT_REPO,
    ):
        """
        初始化源生成器

        Args:
            dir_path: 目录路径
            source_type: 源类型 ('booksource' 或 'rsssource')
            repo: GitHub 仓库地址
        """
        self.repo_str = repo
        self.dir_path = dir_path
        self.source_type = source_type
        self.drive = GithubDrive()
        self.drive.login(
            repo_owner=self.repo_str.split("/")[0],
            repo_name=self.repo_str.split("/")[1],
            branch="master",
        )

    def get_downloader(self, path: str):
        """根据源类型创建对应下载器"""
        if self.source_type == "booksource":
            return BookSourceDownload(path=path, cate1="book")
        return RSSSourceDownload(path=path, cate1="rss")


    @staticmethod
    def format_file_size(size: Any) -> str:
        """将字节大小格式化为易读文本"""
        try:
            value = float(size)
        except (TypeError, ValueError):
            return str(size)

        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1
        if unit_index == 0:
            return f"{int(value)} {units[unit_index]}"
        return f"{value:.1f} {units[unit_index]}"

    @staticmethod
    def extract_source_count(file: Dict[str, Any]) -> str:
        """根据文件内容估算该批次包含的源数量"""
        name = str(file.get("name", ""))
        if name == "index.html":
            return "-"

        size = file.get("size")
        try:
            return str(int(size)) if name.endswith(".json") and int(size) < 1024 else "-"
        except (TypeError, ValueError):
            return "-"

    def set_table_head(self) -> None:
        """设置表格头"""
        with tr():
            th("文件")
            th("源个数")
            th("大小")
            th("github")
            th("gitee")

    def fill_table_data(self, file: Dict[str, Any]) -> None:
        """填充表格数据行"""
        data_tr = tr()
        data_tr += td(file["name"])
        data_tr += td(self.extract_source_count(file))
        data_tr += td(self.format_file_size(file.get("size", 0)))

        # GitHub 链接
        github_link = td()
        github_link += a(
            "github",
            href=f"yuedu://{self.source_type}/importonline?src=https://github.com/{self.repo_str}/raw/master/{file['fid']}",
        )
        data_tr += github_link

        # Gitee 链接
        gitee_link = td()
        gitee_link += a(
            "gitee",
            href=f"yuedu://{self.source_type}/importonline?src=https://gitee.com/{self.repo_str}/raw/master/{file['fid']}",
        )
        data_tr += gitee_link

    def generate_table(self) -> None:
        """生成文件表格"""
        result_div = div(id="test case result")
        with result_div.add(table(cls="gridtable")).add(tbody()):
            self.set_table_head()

            try:
                files = self.drive.get_file_list(self.dir_path)
                for file in files:
                    if file["name"].endswith(".json"):
                        self.fill_table_data(file)
            except Exception as e:
                logger.error(f"Failed to generate table: {e}\n{traceback.format_exc()}")

    def generate_html_report(self) -> str:
        """生成 HTML 报告"""
        html_root = html()

        # HTML 头
        with html_root.add(head()):
            style(self.style_applied, type="text/css")

        # HTML 体
        with html_root.add(body()):
            hello_div = div(id="hello")
            hello_div.add(p("Hi All,"))
            hello_div.add(p("This is today's source report"))
            self.generate_table()
            GenerateCommon().generate()

        return html_root.render()

    def split_and_upload(self) -> None:
        """分割并上传源文件"""
        try:
            path = read_secret(cate1="funread", cate2="cache", cate3="path", cate4="root")
            self._load_and_backup(path)
            self._export_and_upload(path)
        except Exception as e:
            logger.error(f"Failed to split and upload: {e}")
            raise

    def _load_and_backup(self, path: str) -> None:
        """加载并备份数据"""
        try:
            with self.get_downloader(path) as runner:
                runner.loader()
                runner.dumps_zip()
                logger.info("Data loaded and backed up successfully")
        except Exception as e:
            logger.error(f"Failed to load and backup data: {e}")
            raise

    def _export_and_upload(self, path: str) -> None:
        """导出并上传数据"""
        try:
            with self.get_downloader(path) as runner:
                funos.delete(runner.path_pkl)
                funos.delete(runner.path_bok)
                runner.loads_zip()

                counter = INITIAL_COUNTER
                for data in runner.export_sources(size=EXPORT_BATCH_SIZE):
                    if data:
                        counter = self._upload_batch(data, counter)
        except Exception as e:
            logger.error(f"Failed to export and upload: {e}")
            raise

    @staticmethod
    def _is_file_too_large_error(error: Exception) -> bool:
        """判断是否为远端文件过大错误"""
        message = str(error).lower()
        return "too large" in message or "422" in message

    def _upload_single_batch(self, data: List[Dict[str, Any]], counter: int) -> None:
        """上传单个数据批次"""
        git_path = f"{self.dir_path}/progress-{counter}.json"
        self.drive.upload_file(
            content=json.dumps(data),
            fid=self.dir_path,
            filepath=None,
            filename=f"progress-{counter}.json",
        )
        logger.info(f"Uploaded {len(data)} sources to {git_path}")

    def _upload_batch(self, data: List[Dict[str, Any]], counter: int) -> int:
        """上传数据批次，必要时自动拆分为更小文件"""
        try:
            self._upload_single_batch(data, counter)
            return counter + 1
        except Exception as e:
            if self._is_file_too_large_error(e) and len(data) > MIN_UPLOAD_BATCH_SIZE:
                split_size = max(len(data) // 2, MIN_UPLOAD_BATCH_SIZE)
                logger.warning(
                    f"Batch {counter} too large with {len(data)} sources, split into chunks of {split_size}"
                )
                next_counter = counter
                for start in range(0, len(data), split_size):
                    next_counter = self._upload_batch(
                        data[start : start + split_size], next_counter
                    )
                return next_counter

            logger.error(f"Failed to upload batch {counter}: {e}")
            raise

    def update_rss(self) -> None:
        """更新 RSS 配置"""
        try:
            html_content = self.generate_html_report()
            self.drive.upload_file(
                filepath=None, fid=f"{self.dir_path}", filename="index.html", content=html_content
            )
            logger.info("RSS configuration updated successfully")
        except Exception as e:
            logger.error(f"Failed to update RSS: {e}")
            raise


class GenerateSourceTask(Task):
    """源文件生成任务"""

    def __init__(
        self,
        dir_path: str = DEFAULT_DIR_PATH,
        source_type: str = "booksource",
        repo: str = DEFAULT_REPO,
        *args,
        **kwargs,
    ):
        """
        初始化源生成任务

        Args:
            dir_path: 目录路径
            source_type: 源类型
            repo: GitHub 仓库地址
        """
        self.repo_str = repo
        self.dir_path = dir_path
        self.source_type = source_type
        super(GenerateSourceTask, self).__init__(*args, **kwargs)

    def run_book(self, *args, **kwargs) -> None:
        """运行书源生成任务"""
        try:
            logger.info("Starting book source generation task")
            generate_source = GenerateSourceType(
                source_type="booksource", dir_path=f"{self.dir_path}/book", repo=self.repo_str
            )
            generate_source.split_and_upload()
            generate_source.update_rss()
            logger.info("Book source generation task completed successfully")
        except Exception as e:
            logger.error(f"Book source generation failed: {e}")
            raise

    def run_rss(self, *args, **kwargs) -> None:
        """运行 RSS 源生成任务"""
        try:
            logger.info("Starting RSS source generation task")
            generate_source = GenerateSourceType(
                source_type="rsssource", dir_path=f"{self.dir_path}/rss", repo=self.repo_str
            )
            generate_source.split_and_upload()
            generate_source.update_rss()
            logger.info("RSS source generation task completed successfully")
        except Exception as e:
            logger.error(f"RSS source generation failed: {e}")
            raise
