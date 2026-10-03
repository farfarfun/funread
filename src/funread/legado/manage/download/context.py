"""Runtime context for source download tasks."""

from typing import Any

from .core.constants import DEFAULT_REPO, INITIAL_COUNTER, MIN_UPLOAD_BATCH_SIZE
from .reporting import SourceRemoteManager, SourceReportBuilder
from .sources import SourceStoreFactory

try:
    from fundrive.drives.github import GithubDrive
except ImportError:  # pragma: no cover - fundrive 仅要求 Python >= 3.12
    GithubDrive = None


class SourceBuildContext:
    """共享运行时上下文，供源码生成/上传/发布任务共用。

    依赖可选包 ``fundrive``（要求 Python >= 3.12），用于把生成的源文件上传
    到 GitHub。未安装时构造函数会抛出明确的 ``ImportError``，而不是让
    导入本模块本身失败，以免拖垮不需要上传能力的调用方（如本地校验、
    测试）。
    """

    def __init__(
        self,
        dir_path: str = "funread/legado/book/snapshot/20231011",
        source_type: str = "booksource",
        repo: str = DEFAULT_REPO,
    ):
        if GithubDrive is None:
            raise ImportError(
                "SourceBuildContext 需要可选依赖 fundrive（Python >= 3.12）："
                "请执行 `pip install funread[publish]`"
            )
        self.repo_str = repo
        self.dir_path = dir_path
        self.source_type = source_type
        self.drive = GithubDrive()
        self._source_count_cache: dict[str, str] = {}
        self.report_builder = SourceReportBuilder(self)
        self.remote_manager = SourceRemoteManager(
            context=self,
            initial_counter=INITIAL_COUNTER,
            min_upload_batch_size=MIN_UPLOAD_BATCH_SIZE,
        )
        self.drive.login(
            repo_owner=self.repo_str.split("/")[0],
            repo_name=self.repo_str.split("/")[1],
            branch="master",
        )

    def _remember_source_count(self, *keys: Any, count: Any) -> None:
        count_text = str(count)
        for key in keys:
            if key:
                self._source_count_cache[str(key)] = count_text

    def create_store(self, path: str):
        return SourceStoreFactory.create(path=path, source_type=self.source_type)

    def _get_report_builder(self) -> SourceReportBuilder:
        report_builder = getattr(self, "report_builder", None)
        if report_builder is None:
            report_builder = SourceReportBuilder(self)
            self.report_builder = report_builder
        return report_builder

    def format_file_size(self, size: Any) -> str:
        return self._get_report_builder().format_file_size(size)

    def extract_source_count(self, file: dict[str, Any]) -> str:
        return self._get_report_builder().extract_source_count(file)

    def generate_table(self) -> None:
        self._get_report_builder().generate_table()

    def generate_html_report(self) -> str:
        return self._get_report_builder().generate_html_report()

    def upload_single_batch(self, data: list[dict[str, Any]], counter: int) -> None:
        self.remote_manager.upload_single_batch(data, counter)

    def upload_batch(self, data: list[dict[str, Any]], counter: int) -> int:
        return self.remote_manager.upload_batch(data, counter)

    def cleanup_stale_remote_batches(self, next_counter: int) -> None:
        self.remote_manager.cleanup_stale_remote_batches(next_counter)
